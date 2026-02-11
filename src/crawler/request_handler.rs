//! Contains the request handling logic for the spider crawler.
//!
//! This module implements the core request processing pipeline that manages the flow of requests
//! and responses through the crawling system. It handles:
//!
//! - Receiving requests from the scheduler
//! - Managing concurrent downloads with configurable limits
//! - Processing requests through middleware chains
//! - Applying backpressure mechanisms to prevent overload
//! - Handling response transmission back to the processing pipeline
//! - Coordinating with the scheduler for shutdown procedures
//!
//! The main entry point is the `spawn_downloader_task` function which creates an async task
//! responsible for continuously processing requests from a receiver channel, downloading them,
//! and sending responses to a transmitter channel.

use crate::Downloader;
use crate::crawler::SharedMiddlewareManager;
use crate::scheduler::Scheduler;
use crate::state::CrawlerState;
use crate::stats::StatCollector;

use crate::concurrency::AdaptiveSemaphore;
use kanal::{AsyncReceiver, AsyncSender};
use log::{debug, error, trace};
use spider_middleware::middleware::MiddlewareAction;
use spider_util::item::ScrapedItem;
use spider_util::request::Request;
use spider_util::response::Response;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::task::JoinSet;
use tokio::time::Instant;

#[allow(clippy::too_many_arguments)]
pub fn spawn_downloader_task<S, C>(
    scheduler: Arc<Scheduler>,
    req_rx: AsyncReceiver<Request>,
    downloader: Arc<dyn Downloader<Client = C> + Send + Sync>,
    middlewares: SharedMiddlewareManager<C>,
    state: Arc<CrawlerState>,
    res_tx: AsyncSender<Response>,
    max_concurrent_downloads: usize,
    stats: Arc<StatCollector>,
) -> tokio::task::JoinHandle<()>
where
    S: crate::spider::Spider + 'static,
    S::Item: ScrapedItem,
    C: Send + Sync + Clone + 'static,
{
    let adaptive_semaphore = Arc::new(AdaptiveSemaphore::new(
        max_concurrent_downloads,
        max_concurrent_downloads * 2,
        1,
    ));
    let mut tasks = JoinSet::new();

    // Spawn a task to periodically adjust the semaphore permits based on performance
    let adaptive_semaphore_clone = Arc::clone(&adaptive_semaphore);
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;
            adaptive_semaphore_clone.adjust_permits().await;
        }
    });

    tokio::spawn(async move {
        trace!(
            "Downloader task started with max_concurrent_downloads: {}",
            max_concurrent_downloads
        );
        loop {
            if scheduler.is_shutting_down.load(Ordering::SeqCst) {
                trace!("Scheduler shutdown flag detected, exiting downloader task");
                break;
            }

            // Check for backpressure by monitoring response channel capacity
            if res_tx.len() > max_concurrent_downloads * 2 {
                trace!("High response channel occupancy detected, applying backpressure");
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }

            let request = tokio::select! {
                result = req_rx.recv() => {
                    match result {
                        Ok(req) => {
                            trace!("Received request for URL: {}", req.url);

                            // Apply backpressure if response channel is filling up
                            if res_tx.len() > max_concurrent_downloads {
                                trace!("Applying backpressure, response channel occupancy: {}", res_tx.len());
                                tokio::time::sleep(Duration::from_millis(10)).await;
                            }

                            req
                        },
                        Err(_) => {
                            trace!("Request channel closed, exiting downloader task");
                            break;
                        }
                    }
                }
                _ = tokio::time::sleep(Duration::from_millis(100)) => {
                    continue;
                }
            };

            // Use adaptive semaphore to acquire permits
            let current_permits = adaptive_semaphore.current_permits().await;
            if current_permits == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            }

            state.in_flight_requests.fetch_add(1, Ordering::SeqCst);
            let downloader_clone = Arc::clone(&downloader);
            let middlewares_clone = middlewares.clone();
            let res_tx_clone = res_tx.clone();
            let state_clone = Arc::clone(&state);
            let scheduler_clone = Arc::clone(&scheduler);
            let stats_clone = Arc::clone(&stats);
            let adaptive_semaphore_clone = Arc::clone(&adaptive_semaphore);

            tasks.spawn(async move {
                let start_time = Instant::now();
                let host = request.url.host_str().unwrap_or("unknown").to_string();

                trace!("Processing request through middlewares: {}", request.url);
                let response = process_request_through_middlewares::<S, C>(
                    request,
                    &downloader_clone,
                    &middlewares_clone,
                    &scheduler_clone,
                    &stats_clone,
                )
                .await;

                // Update performance metrics for adaptive concurrency
                let response_time = start_time.elapsed();
                let success = response.is_ok() && response.as_ref().unwrap().is_some();
                adaptive_semaphore_clone
                    .update_metrics(&host, response_time, success)
                    .await;

                if let Ok(Some(final_response)) = response {
                    trace!("Sending response for URL: {}", final_response.url);
                    if res_tx_clone.send(final_response).await.is_err() {
                        error!("Response channel closed, cannot send parsed response.");
                    }
                }

                state_clone
                    .in_flight_requests
                    .fetch_sub(1, Ordering::SeqCst);
            });
        }

        trace!("Waiting for active download tasks to complete");
        while let Some(res) = tasks.join_next().await {
            if let Err(e) = res {
                error!("A download task failed: {:?}", e);
            } else {
                trace!("Download task completed successfully");
            }
        }
        trace!("Downloader task finished");
    })
}

async fn process_request_through_middlewares<S, C>(
    request: Request,
    downloader: &Arc<dyn Downloader<Client = C> + Send + Sync>,
    middlewares: &SharedMiddlewareManager<C>,
    scheduler: &Arc<Scheduler>,
    stats: &Arc<StatCollector>,
) -> Result<Option<Response>, ()>
where
    S: crate::spider::Spider + 'static,
    S::Item: ScrapedItem,
    C: Send + Sync + Clone + 'static,
{
    trace!("Processing request through middlewares: {}", request.url);
    let original_request_url = request.url.clone();
    let mut early_returned_response: Option<Response> = None;

    let mut processed_request_opt = Some(request.clone());

    match middlewares
        .process_request(downloader.client(), request)
        .await
    {
        Ok(MiddlewareAction::Continue(req)) => {
            trace!("Request middleware continued with URL: {}", req.url);
            processed_request_opt = Some(req);
        }
        Ok(MiddlewareAction::Retry(req, delay)) => {
            let request_url = req.url.clone();
            debug!(
                "Request middleware scheduled retry for URL: {} after {:?}",
                request_url, delay
            );
            stats.increment_requests_retried();
            tokio::time::sleep(delay).await;
            if scheduler.enqueue_request(*req).await.is_err() {
                error!(
                    "Failed to re-enqueue retried request for URL: {}",
                    request_url
                );
            }
            return Ok(None);
        }
        Ok(MiddlewareAction::Drop) => {
            debug!(
                "Request dropped by middleware for URL: {}",
                original_request_url
            );
            stats.increment_requests_dropped();
            return Ok(None);
        }
        Ok(MiddlewareAction::ReturnResponse(resp)) => {
            trace!(
                "Request middleware returned cached response for URL: {}",
                resp.url
            );
            early_returned_response = Some(resp);
        }
        Err(e) => {
            error!(
                "Request middleware error for URL {}: {:?}",
                original_request_url, e
            );
            return Ok(None);
        }
    }

    // Download or use early response
    // If early_returned_response is Some, request was consumed by a middleware
    // If early_returned_response is None, processed_request_opt must contain the request
    let response = match early_returned_response {
        Some(resp) => {
            trace!("Using early returned response for URL: {}", resp.url);
            if resp.cached {
                stats.increment_responses_from_cache();
            }
            stats.increment_requests_succeeded();
            stats.increment_responses_received();
            stats.record_response_status(resp.status.as_u16());
            resp
        }
        None => {
            let request_for_download = processed_request_opt.expect("Request must be available for download if not handled by middleware or early returned response");
            let request_url = request_for_download.url.clone();
            trace!("Downloading request for URL: {}", request_url);
            stats.increment_requests_sent();

            // Measure request time
            let start_time = std::time::Instant::now();

            let download_result = downloader.download(request_for_download).await;

            match download_result {
                Ok(resp) => {
                    let duration = start_time.elapsed();
                    trace!(
                        "Download successful for URL: {}, took {:?}",
                        resp.url, duration
                    );

                    // Record the request time
                    stats.record_request_time(resp.url.as_ref(), duration);

                    stats.increment_requests_succeeded();
                    stats.increment_responses_received();
                    stats.record_response_status(resp.status.as_u16());
                    stats.add_bytes_downloaded(resp.body.len());
                    resp
                }
                Err(e) => {
                    let duration = start_time.elapsed();
                    trace!(
                        "Download failed for URL: {}, took {:?}",
                        request_url, duration
                    );

                    // Still record the time even for failed requests
                    stats.record_request_time(request_url.as_ref(), duration);

                    error!("Download error for URL {}: {:?}", request_url, e);
                    stats.increment_requests_failed();
                    return Ok(None);
                }
            }
        }
    };

    let original_request_url = response.request_from_response().url.clone();
    trace!(
        "Processing response through response middlewares for URL: {}",
        original_request_url
    );
    let processed_response = match middlewares.process_response(response).await {
        Ok(MiddlewareAction::Continue(res)) => {
            trace!("Response middleware continued for URL: {}", res.url);
            Some(res)
        }
        Ok(MiddlewareAction::Retry(request, delay)) => {
            let request_url = request.url.clone();
            debug!(
                "Response middleware scheduled retry for URL: {} after {:?}",
                request_url, delay
            );
            stats.increment_requests_retried();
            tokio::time::sleep(delay).await;
            if scheduler.enqueue_request(*request).await.is_err() {
                error!(
                    "Failed to re-enqueue retried request for URL: {}",
                    request_url
                );
            }
            return Err(());
        }
        Ok(MiddlewareAction::Drop) => {
            debug!(
                "Response dropped by middleware for URL: {}",
                original_request_url
            );
            stats.increment_requests_dropped();
            return Err(());
        }
        Ok(MiddlewareAction::ReturnResponse(_)) => {
            // This indicates the middleware has fully handled or consumed the response.
            // Effectively, the response is dropped from further processing by this chain.
            debug!(
                "ReturnResponse action encountered in process_response; this is unexpected and effectively drops the response for further processing for URL: {}",
                original_request_url
            );
            None
        }
        Err(e) => {
            error!(
                "Response middleware error for URL {}: {:?}",
                original_request_url, e
            );
            return Ok(None);
        }
    };

    // Mark the original request URL as visited after successful processing
    if let Some(ref response) = processed_response {
        let original_request = response.request_from_response();
        let fingerprint = original_request.fingerprint();
        trace!("Marking URL as visited: {}", original_request.url);
        if let Err(e) = scheduler.send_mark_as_visited(fingerprint.clone()).await {
            error!(
                "Failed to mark URL as visited (fingerprint: {}): {:?}",
                fingerprint, e
            );
        }
    }

    Ok(processed_response)
}
