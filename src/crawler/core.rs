//! The core Crawler implementation for the `spider-lib` framework.
//!
//! This module defines the `Crawler` struct, which acts as the central orchestrator
//! for the web scraping process. It ties together the scheduler, downloader,
//! middlewares, spiders, and item pipelines to execute a crawl. The crawler
//! manages the lifecycle of requests and items, handles concurrency, supports
//! checkpointing for fault tolerance, and collects statistics for monitoring.
//!
//! It utilizes a task-based asynchronous model, spawning distinct tasks for
//! handling initial requests, downloading web pages, parsing responses, and
//! processing scraped items.

use spider_util::error::SpiderError;
use spider_util::item::ScrapedItem;
use spider_middleware::middleware::Middleware;
use spider_pipeline::pipeline::Pipeline;
use spider_util::request::Request;
use crate::scheduler::Scheduler;
use crate::spider::Spider;
use crate::Downloader;
use crate::state::CrawlerState;
use crate::stats::StatCollector;
use anyhow::Result;
use futures_util::future::join_all;
use kanal::{AsyncReceiver, bounded_async};
use tracing::{debug, error, info, trace, warn};

#[cfg(feature = "checkpoint")]
use crate::checkpoint::save_checkpoint;
#[cfg(feature = "checkpoint")]
use std::path::PathBuf;

use std::sync::{
    Arc,
    atomic::Ordering,
};
use std::time::Duration;
use tokio::sync::Mutex;

#[cfg(feature = "cookie-store")]
use tokio::sync::RwLock;

#[cfg(feature = "cookie-store")]
use cookie_store::CookieStore;

/// The central orchestrator for the web scraping process, handling requests, responses, items, concurrency, checkpointing, and statistics collection.
pub struct Crawler<S: Spider, C> {
    scheduler: Arc<Scheduler>,
    req_rx: AsyncReceiver<Request>,
    stats: Arc<StatCollector>,
    downloader: Arc<dyn Downloader<Client = C> + Send + Sync>,
    middlewares: Vec<Box<dyn Middleware<C> + Send + Sync>>,
    spider: Arc<Mutex<S>>,
    item_pipelines: Vec<Box<dyn Pipeline<S::Item>>>,
    max_concurrent_downloads: usize,
    parser_workers: usize,
    max_concurrent_pipelines: usize,
    channel_capacity: usize,
    #[cfg(feature = "checkpoint")]
    checkpoint_path: Option<PathBuf>,
    #[cfg(feature = "checkpoint")]
    checkpoint_interval: Option<Duration>,
    #[cfg(feature = "cookie-store")]
    pub cookie_store: Arc<RwLock<CookieStore>>,
}

impl<S, C> Crawler<S, C>
where
    S: Spider + 'static,
    S::Item: ScrapedItem,
    C: Send + Sync + Clone + 'static,
{
    /// Creates a new `Crawler` instance with the given components and configuration.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        scheduler: Arc<Scheduler>,
        req_rx: AsyncReceiver<Request>,
        downloader: Arc<dyn Downloader<Client = C> + Send + Sync>,
        middlewares: Vec<Box<dyn Middleware<C> + Send + Sync>>,
        spider: S,
        item_pipelines: Vec<Box<dyn Pipeline<S::Item>>>,
        max_concurrent_downloads: usize,
        parser_workers: usize,
        max_concurrent_pipelines: usize,
        channel_capacity: usize,
        #[cfg(feature = "checkpoint")] checkpoint_path: Option<PathBuf>,
        #[cfg(feature = "checkpoint")] checkpoint_interval: Option<Duration>,
        stats: Arc<StatCollector>,
        #[cfg(feature = "cookie-store")] cookie_store: Arc<tokio::sync::RwLock<CookieStore>>,
    ) -> Self {
        Crawler {
            scheduler,
            req_rx,
            stats,
            downloader,
            middlewares,
            spider: Arc::new(Mutex::new(spider)),
            item_pipelines,
            max_concurrent_downloads,
            parser_workers,
            max_concurrent_pipelines,
            channel_capacity,
            #[cfg(feature = "checkpoint")]
            checkpoint_path,
            #[cfg(feature = "checkpoint")]
            checkpoint_interval,
            #[cfg(feature = "cookie-store")]
            cookie_store,
        }
    }

    /// Starts the crawl, orchestrating the scraping process, managing tasks, handling shutdown, checkpointing, and logging statistics.
    pub async fn start_crawl(self) -> Result<(), SpiderError> {
        info!(
            "Crawler starting crawl with configuration: max_concurrent_downloads={}, parser_workers={}, max_concurrent_pipelines={}",
            self.max_concurrent_downloads, self.parser_workers, self.max_concurrent_pipelines
        );

        // Handle conditional fields based on features
        #[cfg(feature = "checkpoint")]
        let Crawler {
            scheduler,
            req_rx,
            stats,
            downloader,
            middlewares,
            spider,
            item_pipelines,
            max_concurrent_downloads,
            parser_workers,
            max_concurrent_pipelines,
            channel_capacity: _, // We don't need to use this value here
            checkpoint_path,
            checkpoint_interval,
            #[cfg(feature = "cookie-store")]
            cookie_store: _,
        } = self;
        
        #[cfg(not(feature = "checkpoint"))]
        let Crawler {
            scheduler,
            req_rx,
            stats,
            downloader,
            middlewares,
            spider,
            item_pipelines,
            max_concurrent_downloads,
            parser_workers,
            max_concurrent_pipelines,
            channel_capacity: _, // We don't need to use this value here
            #[cfg(feature = "cookie-store")]
            cookie_store: _,
        } = self;

        let state = CrawlerState::new();
        let pipelines = Arc::new(item_pipelines);

        let adaptive_channel_capacity = std::cmp::max(
            self.max_concurrent_downloads * 3,
            self.parser_workers * self.max_concurrent_pipelines * 2,
        )
        .max(self.channel_capacity);

        trace!(
            "Creating communication channels with capacity: {}",
            adaptive_channel_capacity
        );
        let (res_tx, res_rx) = bounded_async(adaptive_channel_capacity);
        let (item_tx, item_rx) = bounded_async(adaptive_channel_capacity);

        trace!("Spawning initial requests task");
        let initial_requests_task =
            spawn_initial_requests_task::<S>(scheduler.clone(), spider.clone(), stats.clone());

        trace!("Initializing middleware manager");
        let middlewares_manager = super::SharedMiddlewareManager::new(middlewares);

        trace!("Spawning downloader task");
        let downloader_task = super::spawn_downloader_task::<S, C>(
            scheduler.clone(),
            req_rx,
            downloader,
            middlewares_manager,
            state.clone(),
            res_tx.clone(),
            max_concurrent_downloads,
            stats.clone(),
        );

        trace!("Spawning parser task");
        let parser_task = super::spawn_parser_task::<S>(
            scheduler.clone(),
            spider.clone(),
            state.clone(),
            res_rx,
            item_tx.clone(),
            parser_workers,
            stats.clone(),
        );

        trace!("Spawning item processor task");
        let item_processor_task = super::spawn_item_processor_task::<S>(
            state.clone(),
            item_rx,
            pipelines.clone(),
            max_concurrent_pipelines,
            stats.clone(),
        );

        #[cfg(feature = "checkpoint")]
        {
            if let (Some(path), Some(interval)) = (&checkpoint_path, checkpoint_interval) {
                let scheduler_clone = scheduler.clone();
                let pipelines_clone = pipelines.clone();
                let path_clone = path.clone();
                
                #[cfg(feature = "cookie-store")]
                let cookie_store_clone = cookie_store.clone();
                
                #[cfg(not(feature = "cookie-store"))]
                let _cookie_store_clone = ();

                trace!(
                    "Starting periodic checkpoint task with interval: {:?}",
                    interval
                );
                tokio::spawn(async move {
                    let mut interval_timer = tokio::time::interval(interval);
                    interval_timer.tick().await;
                    loop {
                        tokio::select! {
                            _ = interval_timer.tick() => {
                                trace!("Checkpoint timer ticked, creating snapshot");
                                if let Ok(scheduler_checkpoint) = scheduler_clone.snapshot().await {
                                    debug!("Scheduler snapshot created, saving checkpoint to {:?}", path_clone);
                                    
                                    #[cfg(feature = "cookie-store")]
                                    let save_result = save_checkpoint::<S>(&path_clone, scheduler_checkpoint, &pipelines_clone, &cookie_store_clone).await;
                                    
                                    #[cfg(not(feature = "cookie-store"))]
                                    let save_result = save_checkpoint::<S>(&path_clone, scheduler_checkpoint, &pipelines_clone, &()).await;

                                    if let Err(e) = save_result {
                                        error!("Periodic checkpoint save failed: {}", e);
                                    } else {
                                        debug!("Periodic checkpoint saved successfully to {:?}", path_clone);
                                    }
                                } else {
                                    warn!("Failed to create scheduler snapshot for checkpoint");
                                }
                            }
                        }
                    }
                });
            }
        }

        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("Ctrl-C received, initiating graceful shutdown.");
            }
            _ = async {
                loop {
                    if scheduler.is_idle() && state.is_idle() {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        if scheduler.is_idle() && state.is_idle() {
                            break;
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            } => {
                info!("Crawl has become idle, initiating shutdown.");
            }
        };

        trace!("Closing communication channels");
        drop(res_tx);
        drop(item_tx);

        if let Err(e) = scheduler.shutdown().await {
            error!("Error during scheduler shutdown: {}", e);
        } else {
            debug!("Scheduler shutdown initiated successfully");
        }

        let timeout_duration = Duration::from_secs(30); // Default timeout of 30 seconds

        let mut task_set = tokio::task::JoinSet::new();
        task_set.spawn(item_processor_task);
        task_set.spawn(parser_task);
        task_set.spawn(downloader_task);
        task_set.spawn(initial_requests_task);

        let remaining_results = tokio::time::timeout(timeout_duration, async {
            let mut results = Vec::new();
            while let Some(result) = task_set.join_next().await {
                results.push(result);
            }
            results
        })
        .await;

        let task_results = match remaining_results {
            Ok(results) => {
                trace!("All tasks completed during shutdown");
                results
            }
            Err(_) => {
                warn!(
                    "Tasks did not complete within timeout ({}s), aborting remaining tasks and continuing with shutdown...",
                    timeout_duration.as_secs()
                );
                task_set.abort_all();

                tokio::time::sleep(Duration::from_millis(100)).await;

                Vec::new()
            }
        };

        for result in task_results {
            if let Err(e) = result {
                error!("Task failed during shutdown: {}", e);
            } else {
                trace!("Task completed successfully during shutdown");
            }
        }

        #[cfg(feature = "checkpoint")]
        {
            if let Some(path) = &checkpoint_path {
                debug!("Creating final checkpoint at {:?}", path);
                let scheduler_checkpoint = scheduler.snapshot().await?;
                
                #[cfg(feature = "cookie-store")]
                let result = save_checkpoint::<S>(path, scheduler_checkpoint, &pipelines, &cookie_store).await;
                
                #[cfg(not(feature = "cookie-store"))]
                let result = save_checkpoint::<S>(path, scheduler_checkpoint, &pipelines, &()).await;

                if let Err(e) = result {
                    error!("Final checkpoint save failed: {}", e);
                } else {
                    info!("Final checkpoint saved successfully to {:?}", path);
                }
            }
        }

        info!("Closing item pipelines...");
        let closing_futures: Vec<_> = pipelines.iter().map(|p| p.close()).collect();
        join_all(closing_futures).await;
        debug!("All item pipelines closed");

        info!(
            "Crawl finished successfully. Stats: requests_enqueued={}, requests_succeeded={}, items_scraped={}",
            stats.requests_enqueued.load(Ordering::SeqCst),
            stats.requests_succeeded.load(Ordering::SeqCst),
            stats.items_scraped.load(Ordering::SeqCst)
        );
        Ok(())
    }

    /// Returns a cloned Arc to the `StatCollector` instance used by this crawler.
    ///
    /// This allows programmatic access to the collected statistics at any time during or after the crawl.
    pub fn get_stats(&self) -> Arc<StatCollector> {
        Arc::clone(&self.stats)
    }
}

fn spawn_initial_requests_task<S>(
    scheduler: Arc<Scheduler>,
    spider: Arc<Mutex<S>>,
    stats: Arc<StatCollector>,
) -> tokio::task::JoinHandle<()>
where
    S: Spider + 'static,
    S::Item: ScrapedItem,
{
    tokio::spawn(async move {
        match spider.lock().await.start_requests() {
            Ok(requests) => {
                for mut req in requests {
                    req.url.set_fragment(None);
                    match scheduler.enqueue_request(req).await {
                        Ok(_) => {
                            stats.increment_requests_enqueued();
                        }
                        Err(e) => {
                            error!("Failed to enqueue initial request: {}", e);
                        }
                    }
                }
            }
            Err(e) => error!("Failed to create start requests: {}", e),
        }
    })
}
