//! # Checkpoint Module
//!
//! Manages crawler checkpoints for saving and restoring crawl state.
//!
//! ## Overview
//!
//! The checkpoint module provides functionality for saving and restoring the
//! complete state of a crawler. This enables crawlers to gracefully recover
//! from interruptions, resume interrupted crawls, and persist progress across
//! sessions. Checkpoints capture the state of various crawler components
//! including the scheduler, item pipelines, and cookie stores.
//!
//! ## Key Components
//!
//! - **SchedulerCheckpoint**: Captures the state of the request scheduler
//! - **Checkpoint**: Complete crawler state snapshot including scheduler, pipelines, and cookies
//! - **save_checkpoint**: Function to serialize and save crawler state to disk
//! - **Feature Integration**: Conditional compilation support for cookie stores
//!
//! ## Implementation Details
//!
//! The checkpoint system uses MessagePack (msgpack) serialization for efficient
//! binary storage of crawler state. It handles the serialization of complex
//! data structures like request queues, visited URL sets, and pipeline states.
//! The system also manages temporary file creation to ensure atomic saves.
//!
//! ## Example
//!
//! ```rust,ignore
//! use spider_core::checkpoint::save_checkpoint;
//! use spider_core::scheduler::Scheduler;
//! use spider_pipeline::pipeline::Pipeline;
//! use std::sync::Arc;
//! use std::path::Path;
//!
//! // Checkpoints are typically managed internally by the crawler
//! // but can be used programmatically if needed
//! let scheduler_checkpoint = scheduler.snapshot().await?;
//! let pipelines_ref = Arc::new(pipelines);
//!
//! save_checkpoint(
//!     &Path::new("./crawl.checkpoint"),
//!     scheduler_checkpoint,
//!     &pipelines_ref,
//!     &cookie_store,
//! ).await?;
//! ```

use crate::spider::Spider;
use dashmap::DashSet;
use rmp_serde;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use spider_pipeline::pipeline::Pipeline;
use spider_util::error::SpiderError;
use spider_util::item::ScrapedItem;
use spider_util::request::Request;
use std::collections::{HashMap, VecDeque};
use std::fs;
use std::path::Path;
use std::sync::Arc;
use tracing::{info, warn};

#[cfg(feature = "cookie-store")]
use tokio::sync::RwLock;

#[cfg(feature = "cookie-store")]
use cookie_store::CookieStore;

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
    #[cfg(feature = "cookie-store")]
    #[serde(default)]
    pub cookie_store: CookieStore,

    /// Placeholder when cookie store is disabled
    #[cfg(not(feature = "cookie-store"))]
    #[serde(skip)]
    pub _cookie_store_placeholder: (),
}

pub async fn save_checkpoint<S: Spider>(
    path: &Path,
    scheduler_checkpoint: SchedulerCheckpoint,
    pipelines: &Arc<Vec<Box<dyn Pipeline<S::Item>>>>,
    #[cfg(feature = "cookie-store")] cookie_store: &Arc<RwLock<CookieStore>>,
    #[cfg(not(feature = "cookie-store"))] _cookie_store: &(),
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

    let checkpoint = Checkpoint {
        scheduler: scheduler_checkpoint,
        pipelines: pipelines_checkpoint_map,
        #[cfg(feature = "cookie-store")]
        cookie_store: {
            let cookie_store_read = cookie_store.read().await;
            (*cookie_store_read).clone()
        },
        #[cfg(not(feature = "cookie-store"))]
        _cookie_store_placeholder: (),
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

