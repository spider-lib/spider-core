//! # Builder Module
//!
//! Provides the `CrawlerBuilder`, a fluent API for constructing and configuring
//! `Crawler` instances with customizable settings and components.
//!
//! ## Overview
//!
//! The `CrawlerBuilder` simplifies the process of assembling various `spider-core`
//! components into a fully configured web crawler. It provides a flexible,
//! ergonomic interface for setting up all aspects of the crawling process.
//!
//! ## Key Features
//!
//! - **Concurrency Configuration**: Control the number of concurrent downloads,
//!   parsing workers, and pipeline processors
//! - **Component Registration**: Attach custom downloaders, middlewares, and pipelines
//! - **Checkpoint Management**: Configure automatic saving and loading of crawl state (feature: `core-checkpoint`)
//! - **Statistics Integration**: Initialize and connect the `StatCollector`
//! - **Default Handling**: Automatic addition of essential middlewares when needed
//!
//! ## Example
//!
//! ```rust,ignore
//! use spider_core::CrawlerBuilder;
//! use spider_middleware::rate_limit::RateLimitMiddleware;
//! use spider_pipeline::console_writer::ConsoleWriterPipeline;
//!
//! async fn setup_crawler() -> Result<(), SpiderError> {
//!     let crawler = CrawlerBuilder::new(MySpider)
//!         .max_concurrent_downloads(10)
//!         .max_parser_workers(4)
//!         .add_middleware(RateLimitMiddleware::default())
//!         .add_pipeline(ConsoleWriterPipeline::new())
//!         .with_checkpoint_path("./crawl.checkpoint")
//!         .build()
//!         .await?;
//!
//!     crawler.start_crawl().await
//! }
//! ```

use crate::Downloader;
use crate::ReqwestClientDownloader;
use crate::scheduler::Scheduler;
use crate::spider::Spider;
use num_cpus;
use spider_middleware::middleware::Middleware;
use spider_pipeline::pipeline::Pipeline;
use spider_util::error::SpiderError;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use super::Crawler;
use crate::stats::StatCollector;
#[cfg(feature = "checkpoint")]
use tracing::{debug, warn};

#[cfg(feature = "checkpoint")]
use crate::SchedulerCheckpoint;
#[cfg(feature = "checkpoint")]
use rmp_serde;
#[cfg(feature = "checkpoint")]
use std::fs;

/// Configuration for the crawler's concurrency settings.
pub struct CrawlerConfig {
    /// The maximum number of concurrent downloads.
    pub max_concurrent_downloads: usize,
    /// The number of workers dedicated to parsing responses.
    pub parser_workers: usize,
    /// The maximum number of concurrent item processing pipelines.
    pub max_concurrent_pipelines: usize,
    /// The capacity of communication channels between components.
    pub channel_capacity: usize,
}

impl Default for CrawlerConfig {
    fn default() -> Self {
        CrawlerConfig {
            max_concurrent_downloads: num_cpus::get().max(16),
            parser_workers: num_cpus::get().clamp(4, 16),
            max_concurrent_pipelines: num_cpus::get().min(8),
            channel_capacity: 1000,
        }
    }
}

pub struct CrawlerBuilder<S: Spider, D>
where
    D: Downloader,
{
    crawler_config: CrawlerConfig,
    downloader: D,
    spider: Option<S>,
    middlewares: Vec<Box<dyn Middleware<D::Client> + Send + Sync>>,
    item_pipelines: Vec<Box<dyn Pipeline<S::Item>>>,
    checkpoint_path: Option<PathBuf>,
    checkpoint_interval: Option<Duration>,
    _phantom: PhantomData<S>,
}

impl<S: Spider> Default for CrawlerBuilder<S, ReqwestClientDownloader> {
    fn default() -> Self {
        Self {
            crawler_config: CrawlerConfig::default(),
            downloader: ReqwestClientDownloader::default(),
            spider: None,
            middlewares: Vec::new(),
            item_pipelines: Vec::new(),
            checkpoint_path: None,
            checkpoint_interval: None,
            _phantom: PhantomData,
        }
    }
}

impl<S: Spider> CrawlerBuilder<S, ReqwestClientDownloader> {
    /// Creates a new `CrawlerBuilder` for a given spider with the default ReqwestClientDownloader.
    pub fn new(spider: S) -> Self {
        Self {
            spider: Some(spider),
            ..Default::default()
        }
    }
}

impl<S: Spider, D: Downloader> CrawlerBuilder<S, D> {
    /// Sets the maximum number of concurrent downloads.
    pub fn max_concurrent_downloads(mut self, limit: usize) -> Self {
        self.crawler_config.max_concurrent_downloads = limit;
        self
    }

    /// Sets the maximum number of concurrent parser workers.
    pub fn max_parser_workers(mut self, limit: usize) -> Self {
        self.crawler_config.parser_workers = limit;
        self
    }

    /// Sets the maximum number of concurrent pipelines.
    pub fn max_concurrent_pipelines(mut self, limit: usize) -> Self {
        self.crawler_config.max_concurrent_pipelines = limit;
        self
    }

    /// Sets the capacity of communication channels between components.
    pub fn channel_capacity(mut self, capacity: usize) -> Self {
        self.crawler_config.channel_capacity = capacity;
        self
    }

    /// Sets a custom downloader for the crawler.
    pub fn downloader(mut self, downloader: D) -> Self {
        self.downloader = downloader;
        self
    }

    /// Adds a middleware to the crawler.
    pub fn add_middleware<M>(mut self, middleware: M) -> Self
    where
        M: Middleware<D::Client> + Send + Sync + 'static,
    {
        self.middlewares.push(Box::new(middleware));
        self
    }

    /// Adds an item pipeline to the crawler.
    pub fn add_pipeline<P>(mut self, pipeline: P) -> Self
    where
        P: Pipeline<S::Item> + 'static,
    {
        self.item_pipelines.push(Box::new(pipeline));
        self
    }

    /// Enables checkpointing and sets the path for the checkpoint file.
    pub fn with_checkpoint_path<P: AsRef<Path>>(mut self, path: P) -> Self {
        self.checkpoint_path = Some(path.as_ref().to_path_buf());
        self
    }

    /// Sets the interval for periodic checkpointing.
    pub fn with_checkpoint_interval(mut self, interval: Duration) -> Self {
        self.checkpoint_interval = Some(interval);
        self
    }

    /// Builds the `Crawler` instance, initializing and passing the `StatCollector` along with other configured components.
    #[allow(unused_variables)]
    pub async fn build(mut self) -> Result<Crawler<S, D::Client>, SpiderError>
    where
        D: Downloader + Send + Sync + 'static,
        D::Client: Send + Sync + Clone,
        S::Item: Send + Sync + 'static,
    {
        let spider = self.validate_and_get_spider()?;

        // Add a default ConsoleWriter pipeline if none are provided
        if self.item_pipelines.is_empty() {
            use spider_pipeline::console_writer::ConsoleWriterPipeline;
            self = self.add_pipeline(ConsoleWriterPipeline::new());
        }

        #[cfg(all(feature = "checkpoint", feature = "cookie-store"))]
        {
            let (initial_scheduler_state, loaded_cookie_store) =
                self.load_and_restore_checkpoint_state().await?;
            let (scheduler_arc, req_rx) = Scheduler::new(initial_scheduler_state);
            let downloader_arc = Arc::new(self.downloader);
            let stats = Arc::new(StatCollector::new());
            let crawler = Crawler::new(
                scheduler_arc,
                req_rx,
                downloader_arc,
                self.middlewares,
                spider,
                self.item_pipelines,
                self.crawler_config.max_concurrent_downloads,
                self.crawler_config.parser_workers,
                self.crawler_config.max_concurrent_pipelines,
                self.crawler_config.channel_capacity,
                self.checkpoint_path.take(),
                self.checkpoint_interval,
                stats,
                Arc::new(tokio::sync::RwLock::new(
                    loaded_cookie_store.unwrap_or_default(),
                )),
            );
            Ok(crawler)
        }

        #[cfg(all(feature = "checkpoint", not(feature = "cookie-store")))]
        {
            let (initial_scheduler_state, _loaded_cookie_store) =
                self.load_and_restore_checkpoint_state().await?;
            let (scheduler_arc, req_rx) = Scheduler::new(initial_scheduler_state);
            let downloader_arc = Arc::new(self.downloader);
            let stats = Arc::new(StatCollector::new());
            let crawler = Crawler::new(
                scheduler_arc,
                req_rx,
                downloader_arc,
                self.middlewares,
                spider,
                self.item_pipelines,
                self.crawler_config.max_concurrent_downloads,
                self.crawler_config.parser_workers,
                self.crawler_config.max_concurrent_pipelines,
                self.crawler_config.channel_capacity,
                self.checkpoint_path.take(),
                self.checkpoint_interval,
                stats,
            );
            return Ok(crawler);
        }

        #[cfg(all(not(feature = "checkpoint"), feature = "cookie-store"))]
        {
            let (_initial_scheduler_state, loaded_cookie_store) =
                self.load_and_restore_checkpoint_state().await?;
            let (scheduler_arc, req_rx) = Scheduler::new(None::<()>);
            let downloader_arc = Arc::new(self.downloader);
            let stats = Arc::new(StatCollector::new());
            let crawler = Crawler::new(
                scheduler_arc,
                req_rx,
                downloader_arc,
                self.middlewares,
                spider,
                self.item_pipelines,
                self.crawler_config.max_concurrent_downloads,
                self.crawler_config.parser_workers,
                self.crawler_config.max_concurrent_pipelines,
                self.crawler_config.channel_capacity,
                stats,
                Arc::new(tokio::sync::RwLock::new(
                    loaded_cookie_store.unwrap_or_default(),
                )),
            );
            return Ok(crawler);
        }

        #[cfg(all(not(feature = "checkpoint"), not(feature = "cookie-store")))]
        {
            let (_initial_scheduler_state, _loaded_cookie_store) =
                self.load_and_restore_checkpoint_state().await?;
            let (scheduler_arc, req_rx) = Scheduler::new(None::<()>);
            let downloader_arc = Arc::new(self.downloader);
            let stats = Arc::new(StatCollector::new());
            let crawler = Crawler::new(
                scheduler_arc,
                req_rx,
                downloader_arc,
                self.middlewares,
                spider,
                self.item_pipelines,
                self.crawler_config.max_concurrent_downloads,
                self.crawler_config.parser_workers,
                self.crawler_config.max_concurrent_pipelines,
                self.crawler_config.channel_capacity,
                stats,
            );
            return Ok(crawler);
        }
    }

    #[cfg(all(feature = "checkpoint", feature = "cookie-store"))]
    async fn load_and_restore_checkpoint_state(
        &mut self,
    ) -> Result<(Option<SchedulerCheckpoint>, Option<crate::CookieStore>), SpiderError> {
        let mut initial_scheduler_state = None;
        let mut loaded_pipelines_state = None;
        let mut loaded_cookie_store = None;

        if let Some(path) = &self.checkpoint_path {
            debug!("Attempting to load checkpoint from {:?}", path);
            match fs::read(path) {
                Ok(bytes) => match rmp_serde::from_slice::<crate::Checkpoint>(&bytes) {
                    Ok(checkpoint) => {
                        initial_scheduler_state = Some(checkpoint.scheduler);
                        loaded_pipelines_state = Some(checkpoint.pipelines);

                        loaded_cookie_store = Some(checkpoint.cookie_store);
                    }
                    Err(e) => warn!("Failed to deserialize checkpoint from {:?}: {}", path, e),
                },
                Err(e) => warn!("Failed to read checkpoint file {:?}: {}", path, e),
            }
        }

        if let Some(pipeline_states) = loaded_pipelines_state {
            for (name, state) in pipeline_states {
                if let Some(pipeline) = self.item_pipelines.iter().find(|p| p.name() == name) {
                    pipeline.restore_state(state).await?;
                } else {
                    warn!("Checkpoint contains state for unknown pipeline: {}", name);
                }
            }
        }

        Ok((initial_scheduler_state, loaded_cookie_store))
    }

    #[cfg(all(feature = "checkpoint", not(feature = "cookie-store")))]
    async fn load_and_restore_checkpoint_state(
        &mut self,
    ) -> Result<(Option<SchedulerCheckpoint>, Option<()>), SpiderError> {
        let mut initial_scheduler_state = None;
        let mut loaded_pipelines_state = None;

        if let Some(path) = &self.checkpoint_path {
            debug!("Attempting to load checkpoint from {:?}", path);
            match fs::read(path) {
                Ok(bytes) => match rmp_serde::from_slice::<crate::Checkpoint>(&bytes) {
                    Ok(checkpoint) => {
                        initial_scheduler_state = Some(checkpoint.scheduler);
                        loaded_pipelines_state = Some(checkpoint.pipelines);
                    }
                    Err(e) => warn!("Failed to deserialize checkpoint from {:?}: {}", path, e),
                },
                Err(e) => warn!("Failed to read checkpoint file {:?}: {}", path, e),
            }
        }

        if let Some(pipeline_states) = loaded_pipelines_state {
            for (name, state) in pipeline_states {
                if let Some(pipeline) = self.item_pipelines.iter().find(|p| p.name() == name) {
                    pipeline.restore_state(state).await?;
                } else {
                    warn!("Checkpoint contains state for unknown pipeline: {}", name);
                }
            }
        }

        Ok((initial_scheduler_state, None))
    }

    #[cfg(all(not(feature = "checkpoint"), not(feature = "cookie-store")))]
    async fn load_and_restore_checkpoint_state(
        &mut self,
    ) -> Result<(Option<()>, Option<()>), SpiderError> {
        // When both checkpoint and cookie-store features are disabled, return None for both values
        Ok((None, None))
    }

    #[cfg(all(not(feature = "checkpoint"), feature = "cookie-store"))]
    async fn load_and_restore_checkpoint_state(
        &mut self,
    ) -> Result<(Option<()>, Option<crate::CookieStore>), SpiderError> {
        // When checkpoint is disabled but cookie-store is enabled, initialize default cookie store
        Ok((None, Some(crate::CookieStore::default())))
    }

    fn validate_and_get_spider(&mut self) -> Result<S, SpiderError> {
        if self.crawler_config.max_concurrent_downloads == 0 {
            return Err(SpiderError::ConfigurationError(
                "max_concurrent_downloads must be greater than 0.".to_string(),
            ));
        }
        if self.crawler_config.parser_workers == 0 {
            return Err(SpiderError::ConfigurationError(
                "parser_workers must be greater than 0.".to_string(),
            ));
        }
        self.spider.take().ok_or_else(|| {
            SpiderError::ConfigurationError("Crawler must have a spider.".to_string())
        })
    }
}

