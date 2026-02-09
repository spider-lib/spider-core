//! # Response Parser Module
//!
//! Contains the response parsing functionality for the crawler.
//!
//! ## Overview
//!
//! The response parser module handles the processing of HTTP responses received
//! from the downloader. It orchestrates the parsing of responses through the
//! spider's logic, extracting scraped items and new requests to follow.
//! The module implements a concurrent processing model with multiple parser
//! workers to efficiently handle the parsing workload.
//!
//! ## Key Components
//!
//! - **spawn_parser_task**: Creates the main parser coordinator task and worker tasks
//! - **process_crawl_outputs**: Handles the distribution of spider outputs (items and requests)
//! - **Parser Workers**: Multiple concurrent tasks that process individual responses
//! - **Coordinator**: Manages the distribution of responses to available workers
//!
//! ## Architecture
//!
//! The parser uses a coordinator-worker pattern where a coordinator task receives
//! responses from the downloader and distributes them to multiple worker tasks.
//! Each worker task processes responses through the spider's parsing logic,
//! extracting items and new requests. This design allows for concurrent parsing
//! while maintaining proper coordination and backpressure handling.
//!
//! ## Example
//!
//! ```rust,ignore
//! use spider_core::crawler::spawn_parser_task;
//! use spider_util::response::Response;
//! use spider_util::item::ScrapedItem;
//! use kanal::{AsyncReceiver, AsyncSender};
//! use std::sync::Arc;
//! use tokio::sync::Mutex;
//!
//! // The parser task is typically spawned internally by the crawler
//! // but can be used directly if needed for custom implementations
//! let parser_handle = spawn_parser_task(
//!     scheduler,
//!     spider,
//!     state,
//!     response_receiver,
//!     item_sender,
//!     num_parser_workers,
//!     stats,
//! );
//! ```

use crate::scheduler::Scheduler;
use crate::spider::Spider;
use crate::state::CrawlerState;
use crate::stats::StatCollector;
use kanal::{AsyncReceiver, AsyncSender};
use spider_util::item::{ParseOutput, ScrapedItem};
use spider_util::response::Response;
use std::cmp::max;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{debug, error, info, trace, warn};

pub fn spawn_parser_task<S>(
    scheduler: Arc<Scheduler>,
    spider: Arc<Mutex<S>>,
    state: Arc<CrawlerState>,
    res_rx: AsyncReceiver<Response>,
    item_tx: AsyncSender<S::Item>,
    parser_workers: usize,
    stats: Arc<StatCollector>,
) -> tokio::task::JoinHandle<()>
where
    S: Spider + 'static,
    S::Item: ScrapedItem,
{
    let mut tasks = tokio::task::JoinSet::new();
    let (internal_parse_tx, internal_parse_rx) =
        kanal::bounded_async::<Response>(parser_workers * 2);

    // Spawn N parsing worker tasks
    for _ in 0..parser_workers {
        let internal_parse_rx_clone = internal_parse_rx.clone();
        let spider_clone = Arc::clone(&spider);
        let scheduler_clone = Arc::clone(&scheduler);
        let item_tx_clone = item_tx.clone();
        let state_clone = Arc::clone(&state);
        let stats_clone = Arc::clone(&stats);

        tasks.spawn(async move {
            while let Ok(response) = internal_parse_rx_clone.recv().await {
                debug!("Parsing response from {}", response.url);
                match spider_clone.lock().await.parse(response).await {
                    Ok(outputs) => {
                        process_crawl_outputs::<S>(
                            outputs,
                            scheduler_clone.clone(),
                            item_tx_clone.clone(),
                            state_clone.clone(),
                            stats_clone.clone(),
                        )
                        .await;
                    }
                    Err(e) => error!("Spider parsing error: {:?}", e),
                }
                state_clone.parsing_responses.fetch_sub(1, Ordering::SeqCst);
            }
        });
    }

    tokio::spawn(async move {
        trace!(
            "Response parser coordinator started with {} workers",
            parser_workers
        );
        while let Ok(response) = res_rx.recv().await {
            trace!("Received response for parsing from URL: {}", response.url);

            // Apply backpressure if item channel is filling up
            if item_tx.len() > parser_workers * max(2, parser_workers / 2) {
                trace!(
                    "Applying backpressure to parser, item channel occupancy: {}",
                    item_tx.len()
                );
                tokio::time::sleep(Duration::from_millis(5)).await;
            }

            state.parsing_responses.fetch_add(1, Ordering::SeqCst);
            if internal_parse_tx.send(response).await.is_err() {
                error!("Internal parse channel closed, cannot send response to parser worker.");
                state.parsing_responses.fetch_sub(1, Ordering::SeqCst);
            }
        }

        trace!("Closing internal parse channel");
        drop(internal_parse_tx);

        trace!("Waiting for parsing worker tasks to complete");
        while let Some(res) = tasks.join_next().await {
            if let Err(e) = res {
                error!("A parsing worker task failed: {:?}", e);
            } else {
                trace!("Parsing worker task completed successfully");
            }
        }
        trace!("Response parser coordinator finished");
    })
}

pub async fn process_crawl_outputs<S>(
    outputs: ParseOutput<S::Item>,
    scheduler: Arc<Scheduler>,
    item_tx: AsyncSender<S::Item>,
    state: Arc<CrawlerState>,
    stats: Arc<StatCollector>,
) where
    S: Spider + 'static,
    S::Item: ScrapedItem,
{
    let (items, requests) = outputs.into_parts();
    let items_len = items.len();
    let requests_len = requests.len();

    if requests_len > 0 || items_len > 0 {
        info!(
            "Processing {} requests and {} items from spider output.",
            requests_len, items_len
        );
    } else {
        trace!("Spider output contained no requests or items");
    }

    stats.increment_items_scraped();

    let mut request_error_total = 0;

    // Process all requests first without delays
    for (idx, request) in requests.into_iter().enumerate() {
        trace!(
            "Processing request {} of {} from spider output: {}",
            idx + 1,
            requests_len,
            request.url
        );

        if scheduler.is_shutting_down.load(Ordering::SeqCst) {
            debug!(
                "Scheduler is shutting down, skipping request: {}",
                request.url
            );
            request_error_total += 1;
            continue;
        }

        match scheduler.enqueue_request(request).await {
            Ok(_) => {
                trace!("Successfully enqueued request");
                stats.increment_requests_enqueued();
            }
            Err(e) => {
                if scheduler.is_shutting_down.load(Ordering::SeqCst) {
                    debug!("Scheduler is shutting down, skipping remaining requests");
                    request_error_total += 1;
                    continue;
                }

                error!("Failed to enqueue request: {:?}", e);
                request_error_total += 1;
            }
        }
    }

    if request_error_total > 0 {
        warn!(
            "Failed to enqueue {} of {} requests.",
            request_error_total, requests_len
        );
    } else if requests_len > 0 {
        debug!("Successfully enqueued all {} requests", requests_len);
    }

    let mut item_error_total = 0;
    for (idx, item) in items.into_iter().enumerate() {
        trace!(
            "Processing item {} of {} from spider output",
            idx + 1,
            items_len
        );

        if item_tx.is_closed() {
            warn!("Item channel is closed, stopping item processing");
            item_error_total += items_len - idx;
            break;
        }

        state.processing_items.fetch_add(1, Ordering::SeqCst);
        if item_tx.send(item).await.is_err() {
            error!("Failed to send item to processing channel");
            item_error_total += 1;
            state.processing_items.fetch_sub(1, Ordering::SeqCst);
        }
    }

    if item_error_total > 0 {
        warn!(
            "Failed to send {} of {} scraped items.",
            item_error_total, items_len
        );
    } else if items_len > 0 {
        debug!("Successfully sent all {} items for processing", items_len);
    }
}
