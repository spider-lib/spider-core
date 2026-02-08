use spider_util::item::ScrapedItem;
use spider_pipeline::pipeline::Pipeline;
use crate::state::CrawlerState;
use crate::stats::StatCollector;
use kanal::AsyncReceiver;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tracing::{debug, error, trace, warn};

pub fn spawn_item_processor_task<S>(
    state: Arc<CrawlerState>,
    item_rx: AsyncReceiver<S::Item>,
    pipelines: Arc<Vec<Box<dyn Pipeline<S::Item>>>>,
    max_concurrent_pipelines: usize,
    stats: Arc<StatCollector>,
) -> tokio::task::JoinHandle<()>
where
    S: crate::spider::Spider + 'static,
    S::Item: ScrapedItem,
{
    let mut tasks = JoinSet::new();
    let semaphore = Arc::new(Semaphore::new(max_concurrent_pipelines));
    trace!(
        "Starting item processor with max_concurrent_pipelines: {}",
        max_concurrent_pipelines
    );
    tokio::spawn(async move {
        while let Ok(item) = item_rx.recv().await {
            trace!("Received item for processing");
            let permit = match semaphore.clone().acquire_owned().await {
                Ok(p) => {
                    trace!("Acquired processing permit");
                    p
                }
                Err(_) => {
                    warn!("Semaphore closed, shutting down item processor actor.");
                    break;
                }
            };

            let state_clone = Arc::clone(&state);
            let pipelines_clone = Arc::clone(&pipelines);
            let stats_clone = Arc::clone(&stats);
            tasks.spawn(async move {
                trace!(
                    "Processing item through {} pipelines",
                    pipelines_clone.len()
                );
                let mut item_to_process = Some(item);

                for (idx, pipeline) in pipelines_clone.iter().enumerate() {
                    if let Some(item) = item_to_process.take() {
                        trace!(
                            "Processing item through pipeline '{}' ({} of {})",
                            pipeline.name(),
                            idx + 1,
                            pipelines_clone.len()
                        );
                        match pipeline.process_item(item).await {
                            Ok(Some(next_item)) => {
                                trace!("Pipeline '{}' returned processed item", pipeline.name());
                                item_to_process = Some(next_item);
                            }
                            Ok(None) => {
                                debug!("Pipeline '{}' dropped item", pipeline.name());
                                stats_clone.increment_items_dropped_by_pipeline();
                                break;
                            }
                            Err(e) => {
                                error!("Pipeline '{}' error: {:?}", pipeline.name(), e);
                                stats_clone.increment_items_dropped_by_pipeline();
                                break;
                            }
                        }
                    } else {
                        trace!("Item was dropped by pipeline, stopping processing");
                        break;
                    }
                }

                if item_to_process.is_some() {
                    trace!("Item successfully processed by all pipelines");
                    stats_clone.increment_items_processed();
                } else {
                    trace!("Item was dropped during pipeline processing");
                }
                state_clone.processing_items.fetch_sub(1, Ordering::SeqCst);
                trace!("Released processing permit");
                drop(permit);
            });
        }
        trace!("Waiting for active item processing tasks to complete");
        while let Some(res) = tasks.join_next().await {
            if let Err(e) = res {
                error!("An item processing task failed: {:?}", e);
            } else {
                trace!("Item processing task completed successfully");
            }
        }
        trace!("Item processor finished");
    })
}
