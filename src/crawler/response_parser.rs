use spider_util::item::{ParseOutput, ScrapedItem};
use spider_util::response::Response;
use crate::scheduler::Scheduler;
use crate::spider::Spider;
use crate::state::CrawlerState;
use crate::stats::StatCollector;
use kanal::{AsyncReceiver, AsyncSender};
use std::sync::Arc;
use std::sync::atomic::Ordering;
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
