/// Module for managing crawler checkpoints.
///
/// This module defines the data structures (`SchedulerCheckpoint`, `Checkpoint`)
/// and functions for saving and loading the state of a crawler. Checkpoints enable
/// the crawler to gracefully recover from interruptions or to resume a crawl
/// at a later time. They capture the state of the scheduler (pending requests,
/// visited URLs, salvaged requests) and the item pipelines.
use spider_util::error::SpiderError;
use spider_util::item::ScrapedItem;
use spider_pipeline::pipeline::Pipeline;
use spider_util::request::Request;
use crate::spider::Spider;
use dashmap::DashSet;
use rmp_serde;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, VecDeque};
use std::fs;
use std::path::Path;
use std::sync::Arc;
use tracing::{info, warn};

use tokio::sync::RwLock;

/// A snapshot of the scheduler's state.
#[derive(Serialize, Deserialize, Default, Clone, Debug)]
pub struct SchedulerCheckpoint {
    /// The queue of pending requests.
    pub request_queue: VecDeque<Request>,
    /// Requests that could not be enqueued and were salvaged.
    pub salvaged_requests: VecDeque<Request>,
    /// The set of visited URL fingerprints.
    pub visited_urls: DashSet<String>,
}

/// A complete checkpoint of the crawler's state.
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct Checkpoint {
    /// The state of the scheduler.
    pub scheduler: SchedulerCheckpoint,
    /// A map of pipeline states, keyed by pipeline name.
    pub pipelines: HashMap<String, Value>,
    /// The state of the cookie store.
    #[serde(default)]
    pub cookie_store: cookie_store::CookieStore,
}

pub async fn save_checkpoint<S: Spider>(
    path: &Path,
    scheduler_checkpoint: SchedulerCheckpoint,
    pipelines: &Arc<Vec<Box<dyn Pipeline<S::Item>>>>,
    cookie_store: &Arc<RwLock<cookie_store::CookieStore>>,
) -> Result<(), SpiderError>
where
    S::Item: ScrapedItem,
{
    info!("Saving checkpoint to {:?}", path);

    let mut pipelines_checkpoint_map = HashMap::new();
    for pipeline in pipelines.iter() {
        if let Some(state) = pipeline.get_state().await? {
            pipelines_checkpoint_map.insert(pipeline.name().to_string(), state);
        }
    }

    if !scheduler_checkpoint.salvaged_requests.is_empty() {
        warn!(
            "Found {} salvaged requests during checkpoint. These have been added to the request queue.",
            scheduler_checkpoint.salvaged_requests.len()
        );
    }

    let checkpoint = {
        let cookie_store_read = cookie_store.read().await;
        let cookie_store_clone = (*cookie_store_read).clone();
        drop(cookie_store_read);
        Checkpoint {
            scheduler: scheduler_checkpoint,
            pipelines: pipelines_checkpoint_map,
            cookie_store: cookie_store_clone,
        }
    };

    let tmp_path = path.with_extension("tmp");
    let encoded = rmp_serde::to_vec(&checkpoint)
        .map_err(|e| SpiderError::GeneralError(format!("Failed to serialize checkpoint: {}", e)))?;
    fs::write(&tmp_path, encoded).map_err(|e| {
        SpiderError::GeneralError(format!(
            "Failed to write checkpoint to temporary file: {}",
            e
        ))
    })?;
    fs::rename(&tmp_path, path).map_err(|e| {
        SpiderError::GeneralError(format!("Failed to rename temporary checkpoint file: {}", e))
    })?;

    info!("Checkpoint saved successfully.");
    Ok(())
}