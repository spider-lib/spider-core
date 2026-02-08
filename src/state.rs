//! Module for tracking the operational state of the crawler.
//!
//! This module defines the `CrawlerState` struct, which provides a centralized
//! mechanism for monitoring the real-time activity of the web crawler. It
//! utilizes atomic counters to keep track of:
//! - The number of HTTP requests currently in flight (being downloaded).
//! - The number of responses actively being parsed by spiders.
//! - The number of scraped items currently being processed by pipelines.
//!
//! This state information is crucial for determining when the crawler is idle
//! and can be gracefully shut down, or when to trigger checkpointing.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Represents the shared state of the crawler's various actors.
#[derive(Debug, Default)]
pub struct CrawlerState {
    /// The number of requests currently being downloaded.
    pub in_flight_requests: AtomicUsize,
    /// The number of responses currently being parsed.
    pub parsing_responses: AtomicUsize,
    /// The number of items currently being processed by pipelines.
    pub processing_items: AtomicUsize,
}

impl CrawlerState {
    /// Creates a new, atomically reference-counted `CrawlerState`.
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Checks if all crawler activities are idle.
    pub fn is_idle(&self) -> bool {
        self.in_flight_requests.load(Ordering::SeqCst) == 0
            && self.parsing_responses.load(Ordering::SeqCst) == 0
            && self.processing_items.load(Ordering::SeqCst) == 0
    }
}