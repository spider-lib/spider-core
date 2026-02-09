//! # Scheduler Module
//!
//! Implements the request scheduler for managing the crawling frontier and duplicate detection.
//!
//! ## Overview
//!
//! The `Scheduler` is a central component that coordinates the web crawling process
//! by managing the queue of pending requests and tracking visited URLs to prevent
//! duplicate processing. It uses an actor-like design pattern with internal message
//! processing for thread-safe operations.
//!
//! ## Key Responsibilities
//!
//! - **Request Queue Management**: Maintains a queue of pending requests to be processed
//! - **Duplicate Detection**: Tracks visited URLs using Bloom Filter and LRU cache for efficiency
//! - **Request Salvaging**: Handles failed enqueuing attempts to prevent request loss
//! - **State Snapshots**: Provides checkpointing capabilities for crawl resumption
//! - **Concurrent Access**: Thread-safe operations for multi-threaded crawling
//!
//! ## Architecture
//!
//! The scheduler operates asynchronously using an internal message queue to handle
//! operations like request enqueuing, URL marking, and state snapshots. It combines
//! a Bloom Filter for fast preliminary duplicate checks with an LRU cache for
//! definitive tracking, optimizing performance when handling millions of URLs.
//!
//! ## Example
//!
//! ```rust,ignore
//! use spider_core::Scheduler;
//! use spider_util::request::Request;
//! use url::Url;
//!
//! let (scheduler, request_receiver) = Scheduler::new(None);
//!
//! // Enqueue a request
//! let request = Request::new(Url::parse("https://example.com").unwrap());
//! scheduler.enqueue_request(request).await?;
//!
//! // Mark a URL as visited
//! scheduler.send_mark_as_visited("unique_fingerprint".to_string()).await?;
//! ```

#[cfg(feature = "checkpoint")]
use crate::SchedulerCheckpoint;

use spider_util::error::SpiderError;
use spider_util::request::Request;
use crossbeam::queue::SegQueue;
use kanal::{AsyncReceiver, AsyncSender, bounded_async, unbounded_async};
use moka::sync::Cache;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tracing::{debug, error, info, trace, warn};

enum SchedulerMessage {
    Enqueue(Box<Request>),
    MarkAsVisited(String),
    MarkAsVisitedBatch(Vec<String>),
    Shutdown,
}

use spider_util::bloom_filter::BloomFilter;

use tokio::sync::Notify;

pub struct Scheduler {
    request_queue: SegQueue<Request>,
    visited_urls: Cache<String, bool>,
    bloom_filter: std::sync::Arc<parking_lot::RwLock<BloomFilter>>,
    bloom_filter_buffer: Arc<std::sync::Mutex<Vec<String>>>,
    bloom_filter_notify: Arc<Notify>,
    tx_internal: AsyncSender<SchedulerMessage>,
    pending_requests: AtomicUsize,
    salvaged_requests: SegQueue<Request>,
    pub(crate) is_shutting_down: AtomicBool,
    max_pending_requests: usize,
}

impl Scheduler {
    /// Creates a new `Scheduler` and returns a tuple containing the scheduler and a request receiver.
    #[cfg(feature = "checkpoint")]
    pub fn new(
        initial_state: Option<SchedulerCheckpoint>,
    ) -> (Arc<Self>, AsyncReceiver<Request>) {
        let (tx_internal, rx_internal) = unbounded_async();

        let (tx_req_out, rx_req_out) = bounded_async(100);

        let request_queue: SegQueue<Request>;
        let visited_urls: Cache<String, bool>;
        let pending_requests: AtomicUsize;
        let salvaged_requests: SegQueue<Request>;

        if let Some(state) = initial_state {
            info!(
                "Initializing scheduler from checkpoint with {} requests, {} visited URLs, and {} salvaged requests.",
                state.request_queue.len(),
                state.visited_urls.len(),
                state.salvaged_requests.len(),
            );
            let pending = state.request_queue.len() + state.salvaged_requests.len();
            request_queue = SegQueue::new();
            for request in state.request_queue {
                request_queue.push(request);
            }

            visited_urls = Cache::builder()
                .max_capacity(500000) // Increased cache size for better performance
                .time_to_idle(std::time::Duration::from_secs(3600)) // Keep entries for 1 hour of inactivity
                .eviction_listener(|_key, _value, _cause| {
                    // Optional: Add metrics here if needed
                })
                .build();
            for url in state.visited_urls {
                visited_urls.insert(url, true);
            }

            pending_requests = AtomicUsize::new(pending);
            salvaged_requests = SegQueue::new();
            for request in state.salvaged_requests {
                salvaged_requests.push(request);
            }
        } else {
            request_queue = SegQueue::new();
            visited_urls = Cache::builder().max_capacity(200000).build(); // Increased cache size for better performance
            pending_requests = AtomicUsize::new(0);
            salvaged_requests = SegQueue::new();
        }

        let bloom_filter_buffer = Arc::new(std::sync::Mutex::new(Vec::new()));
        let bloom_filter_notify = Arc::new(Notify::new());

        let scheduler = Arc::new(Scheduler {
            request_queue,
            visited_urls,
            bloom_filter: std::sync::Arc::new(parking_lot::RwLock::new(BloomFilter::new(5000000, 5))), // Increased size and hash functions for better accuracy
            bloom_filter_buffer: bloom_filter_buffer.clone(),
            bloom_filter_notify: bloom_filter_notify.clone(),
            tx_internal,
            pending_requests,
            salvaged_requests,
            is_shutting_down: AtomicBool::new(false),
            max_pending_requests: 30000, // Increased for better buffering
        });

        // Spawn background task to periodically flush the bloom filter buffer
        let scheduler_clone_for_bloom = Arc::clone(&scheduler);
        let bloom_filter_buffer_clone = bloom_filter_buffer.clone();
        let bloom_filter_notify_clone = bloom_filter_notify.clone();
        tokio::spawn(async move {
            scheduler_clone_for_bloom.flush_bloom_filter_buffer(bloom_filter_buffer_clone, bloom_filter_notify_clone).await;
        });

        let scheduler_clone = Arc::clone(&scheduler);
        tokio::spawn(async move {
            scheduler_clone.run_loop(rx_internal, tx_req_out).await;
        });

        (scheduler, rx_req_out)
    }

    /// Creates a new `Scheduler` without checkpoint support and returns a tuple containing the scheduler and a request receiver.
    #[cfg(not(feature = "checkpoint"))]
    pub fn new(
        _initial_state: Option<()>, // Placeholder parameter to maintain same signature
    ) -> (Arc<Self>, AsyncReceiver<Request>) {
        let (tx_internal, rx_internal) = unbounded_async();

        let (tx_req_out, rx_req_out) = bounded_async(100);

        let request_queue = SegQueue::new();
        let visited_urls = Cache::builder()
            .max_capacity(500000) // Increased cache size for better performance
            .time_to_idle(std::time::Duration::from_secs(3600)) // Keep entries for 1 hour of inactivity
            .eviction_listener(|_key, _value, _cause| {
                // Optional: Add metrics here if needed
            })
            .build();
        let pending_requests = AtomicUsize::new(0);
        let salvaged_requests = SegQueue::new();

        let bloom_filter_buffer = Arc::new(std::sync::Mutex::new(Vec::new()));
        let bloom_filter_notify = Arc::new(Notify::new());

        let scheduler = Arc::new(Scheduler {
            request_queue,
            visited_urls,
            bloom_filter: std::sync::Arc::new(parking_lot::RwLock::new(BloomFilter::new(5000000, 5))), // Increased size and hash functions for better accuracy
            bloom_filter_buffer: bloom_filter_buffer.clone(),
            bloom_filter_notify: bloom_filter_notify.clone(),
            tx_internal,
            pending_requests,
            salvaged_requests,
            is_shutting_down: AtomicBool::new(false),
            max_pending_requests: 30000, // Increased for better buffering
        });

        // Spawn background task to periodically flush the bloom filter buffer
        let scheduler_clone_for_bloom = Arc::clone(&scheduler);
        let bloom_filter_buffer_clone = bloom_filter_buffer.clone();
        let bloom_filter_notify_clone = bloom_filter_notify.clone();
        tokio::spawn(async move {
            scheduler_clone_for_bloom.flush_bloom_filter_buffer(bloom_filter_buffer_clone, bloom_filter_notify_clone).await;
        });

        let scheduler_clone = Arc::clone(&scheduler);
        tokio::spawn(async move {
            scheduler_clone.run_loop(rx_internal, tx_req_out).await;
        });

        (scheduler, rx_req_out)
    }

    async fn run_loop(
        &self,
        rx_internal: AsyncReceiver<SchedulerMessage>,
        tx_req_out: AsyncSender<Request>,
    ) {
        info!(
            "Scheduler run_loop started with max pending requests: {}",
            self.max_pending_requests
        );
        loop {
            if let Ok(Some(msg)) = rx_internal.try_recv() {
                trace!("Processing pending internal message");
                if !self.handle_message(Ok(msg)).await {
                    break;
                }
                continue;
            }

            let maybe_request = if !tx_req_out.is_closed() && !self.is_idle() {
                self.request_queue.pop()
            } else {
                None
            };

            if let Some(request) = maybe_request {
                trace!("Sending request to crawler: {}", request.url);
                tokio::select! {
                    send_res = tx_req_out.send(request) => {
                        if send_res.is_err() {
                            error!("Crawler receiver dropped. Scheduler can no longer send requests.");
                        } else {
                            trace!("Successfully sent request to crawler");
                        }
                        self.pending_requests.fetch_sub(1, Ordering::SeqCst);
                    },
                    recv_res = rx_internal.recv() => {
                        trace!("Received internal message while sending request");
                        if !self.handle_message(recv_res).await {
                            break;
                        }
                        continue;
                    }
                }
            } else {
                trace!("No pending requests, waiting for internal message");
                if !self.handle_message(rx_internal.recv().await).await {
                    break;
                }
            }
        }
        info!(
            "Scheduler run_loop finished with {} pending requests remaining.",
            self.pending_requests.load(Ordering::SeqCst)
        );
    }

    async fn handle_message(&self, msg: Result<SchedulerMessage, kanal::ReceiveError>) -> bool {
        match msg {
            Ok(SchedulerMessage::Enqueue(boxed_request)) => {
                let request = *boxed_request;
                trace!("Enqueuing request: {}", request.url);
                self.request_queue.push(request);
                self.pending_requests.fetch_add(1, Ordering::SeqCst);
                true
            }
            Ok(SchedulerMessage::MarkAsVisited(fingerprint)) => {
                trace!("Marking URL fingerprint as visited: {}", fingerprint);
                self.visited_urls.insert(fingerprint.clone(), true);
                
                // Add to buffer instead of directly to bloom filter to reduce lock contention
                {
                    let mut buffer = self.bloom_filter_buffer.lock().unwrap();
                    buffer.push(fingerprint.clone());
                    if buffer.len() >= 100 { // Flush buffer when it reaches 100 items
                        self.bloom_filter_notify.notify_one();
                    }
                }
                
                debug!("Marked URL as visited: {}", fingerprint);
                true
            }
            Ok(SchedulerMessage::MarkAsVisitedBatch(fingerprints)) => {
                let count = fingerprints.len();
                trace!("Marking {} URL fingerprints as visited in batch", count);
                for fingerprint in &fingerprints {
                    self.visited_urls.insert(fingerprint.clone(), true);
                }
                
                // Add all to buffer instead of directly to bloom filter
                {
                    let mut buffer = self.bloom_filter_buffer.lock().unwrap();
                    buffer.extend(fingerprints);
                    if buffer.len() >= 100 { // Flush buffer when it reaches 100 items
                        self.bloom_filter_notify.notify_one();
                    }
                }
                
                debug!("Marked {} URLs as visited in batch", count);
                true
            }
            Ok(SchedulerMessage::Shutdown) => {
                info!("Scheduler received shutdown signal. Exiting run_loop.");
                self.is_shutting_down.store(true, Ordering::SeqCst);
                
                // Flush remaining items in buffer before shutdown
                self.flush_bloom_filter_buffer_now();
                
                false
            }
            Err(_) => {
                warn!("Scheduler internal message channel closed. Exiting run_loop.");
                self.is_shutting_down.store(true, Ordering::SeqCst);
                false
            }
        }
    }

    /// Takes a snapshot of the current state of the scheduler.
    /// Uses a non-blocking approach to collect queue elements without disrupting concurrent operations.
    #[cfg(feature = "checkpoint")]
    pub async fn snapshot(&self) -> Result<SchedulerCheckpoint, SpiderError> {
        let visited_urls = dashmap::DashSet::new();
        for entry in self.visited_urls.iter() {
            let (key, _) = entry;
            visited_urls.insert(key.as_ref().clone());
        }

        // Collect request queue elements without blocking
        // We'll gather elements as they are without fully draining the queue
        let mut request_queue = std::collections::VecDeque::new();
        let mut temp_requests = Vec::new();

        // Collect elements without fully draining the queue
        while let Some(request) = self.request_queue.pop() {
            temp_requests.push(request);
        }

        // Add them to the snapshot and back to the queue
        for request in temp_requests.into_iter() {
            request_queue.push_back(request.clone());
            // Only add back if not shutting down
            if !self.is_shutting_down.load(Ordering::SeqCst) {
                self.request_queue.push(request);
            }
        }

        // Collect salvaged requests similarly
        let mut salvaged_requests = std::collections::VecDeque::new();
        let mut temp_salvaged = Vec::new();

        while let Some(request) = self.salvaged_requests.pop() {
            temp_salvaged.push(request);
        }

        for request in temp_salvaged.into_iter() {
            salvaged_requests.push_back(request.clone());
            // Only add back if not shutting down
            if !self.is_shutting_down.load(Ordering::SeqCst) {
                self.salvaged_requests.push(request);
            }
        }

        Ok(SchedulerCheckpoint {
            request_queue,
            visited_urls,
            salvaged_requests,
        })
    }

    /// Takes a snapshot of the current state of the scheduler (stub when checkpoint feature is disabled).
    #[cfg(not(feature = "checkpoint"))]
    pub async fn snapshot(&self) -> Result<(), SpiderError> {
        // Return an empty result when checkpoint feature is disabled
        Ok(())
    }

    /// Enqueues a new request to be processed.
    pub async fn enqueue_request(&self, request: Request) -> Result<(), SpiderError> {
        if !self.should_enqueue_request(&request) {
            trace!("Request already visited, skipping: {}", request.url);
            return Ok(());
        }

        // Check if we've reached the maximum pending requests limit
        let current_pending = self.pending_requests.load(Ordering::SeqCst);
        if current_pending >= self.max_pending_requests {
            warn!(
                "Maximum pending requests reached ({}), request dropped due to backpressure: {}",
                self.max_pending_requests, request.url
            );
            return Err(SpiderError::GeneralError(
                "Scheduler at maximum capacity, request dropped due to backpressure.".into(),
            ));
        }

        trace!("Enqueuing request: {}", request.url);
        if self
            .tx_internal
            .send(SchedulerMessage::Enqueue(Box::new(request.clone())))
            .await
            .is_err()
        {
            if !self.is_shutting_down.load(Ordering::SeqCst) {
                error!(
                    "Scheduler internal message channel is closed. Salvaging request: {}",
                    request.url
                );
            }
            self.salvaged_requests.push(request);
            return Err(SpiderError::GeneralError(
                "Scheduler internal channel closed, request salvaged.".into(),
            ));
        }

        trace!("Successfully enqueued request: {}", request.url);
        Ok(())
    }

    /// Sends a shutdown signal to the scheduler.
    pub async fn shutdown(&self) -> Result<(), SpiderError> {
        self.is_shutting_down.store(true, Ordering::SeqCst);

        if !self.tx_internal.is_closed() {
            self.tx_internal
                .send(SchedulerMessage::Shutdown)
                .await
                .map_err(|e| {
                    SpiderError::GeneralError(format!(
                        "Scheduler: Failed to send shutdown signal: {}",
                        e
                    ))
                })
        } else {
            debug!("Scheduler internal channel already closed, skipping shutdown signal");
            Ok(())
        }
    }

    /// Sends a message to the scheduler to mark a URL as visited.
    pub async fn send_mark_as_visited(&self, fingerprint: String) -> Result<(), SpiderError> {
        trace!(
            "Sending MarkAsVisited message for fingerprint: {}",
            fingerprint
        );
        self.tx_internal
            .send(SchedulerMessage::MarkAsVisited(fingerprint.clone()))
            .await
            .map_err(|e| {
                if !self.is_shutting_down.load(Ordering::SeqCst) {
                    error!("Scheduler internal message channel is closed. Failed to mark URL as visited (fingerprint: {}): {}", fingerprint, e);
                }
                SpiderError::GeneralError(format!(
                    "Scheduler: Failed to send MarkAsVisited message: {}",
                    e
                ))
            })
    }

    /// Sends a message to the scheduler to mark multiple URLs as visited in batch.
    pub async fn send_mark_as_visited_batch(&self, fingerprints: Vec<String>) -> Result<(), SpiderError> {
        if fingerprints.is_empty() {
            return Ok(());
        }
        
        trace!(
            "Sending MarkAsVisitedBatch message for {} fingerprints",
            fingerprints.len()
        );
        self.tx_internal
            .send(SchedulerMessage::MarkAsVisitedBatch(fingerprints))
            .await
            .map_err(|e| {
                if !self.is_shutting_down.load(Ordering::SeqCst) {
                    error!("Scheduler internal message channel is closed. Failed to mark URLs as visited in batch: {}", e);
                }
                SpiderError::GeneralError(format!(
                    "Scheduler: Failed to send MarkAsVisitedBatch message: {}",
                    e
                ))
            })
    }

    /// Checks if a URL has been visited using Bloom Filter for fast preliminary check
    pub fn has_been_visited(&self, fingerprint: &str) -> bool {
        // Check the bloom filter first
        if !self.bloom_filter.read().might_contain(fingerprint) {
            return false;
        }

        // Also check if it's in the buffer (for recently added items)
        {
            let buffer = self.bloom_filter_buffer.lock().unwrap();
            if buffer.iter().any(|item| item == fingerprint) {
                return true;
            }
        }

        self.visited_urls.contains_key(fingerprint)
    }

    /// Flushes all items in the bloom filter buffer to the actual bloom filter
    fn flush_bloom_filter_buffer_now(&self) {
        let mut buffer = self.bloom_filter_buffer.lock().unwrap();
        if !buffer.is_empty() {
            let items: Vec<String> = buffer.drain(..).collect();
            drop(buffer); // Release the lock before taking the bloom filter lock
            
            let mut bloom_filter = self.bloom_filter.write();
            for item in items {
                bloom_filter.add(&item);
            }
        }
    }

    /// Background task to periodically flush the bloom filter buffer
    async fn flush_bloom_filter_buffer(
        &self,
        _buffer: Arc<std::sync::Mutex<Vec<String>>>,
        notify: Arc<Notify>,
    ) {
        loop {
            // Wait for notification or timeout (every 100ms)
            tokio::select! {
                _ = notify.notified() => {
                    // Notification received, flush the buffer
                    self.flush_bloom_filter_buffer_now();
                }
                _ = tokio::time::sleep(tokio::time::Duration::from_millis(100)) => {
                    // Timeout reached, flush the buffer anyway
                    self.flush_bloom_filter_buffer_now();
                }
            }
            
            // Small delay to avoid busy waiting
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }
    }

    /// Checks if a request should be enqueued by checking if its fingerprint has already been visited
    pub fn should_enqueue_request(&self, request: &Request) -> bool {
        let fingerprint = request.fingerprint();
        !self.has_been_visited(&fingerprint)
    }

    /// Returns the number of pending requests in the scheduler.
    #[inline]
    pub fn len(&self) -> usize {
        self.pending_requests.load(Ordering::SeqCst)
    }

    /// Checks if the scheduler has no pending requests.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Checks if the scheduler is idle (has no pending requests).
    #[inline]
    pub fn is_idle(&self) -> bool {
        self.is_empty()
    }
}
