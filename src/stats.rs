//! # Statistics Module
//!
//! Collects and stores various metrics and statistics about the crawler's operation.
//!
//! ## Overview
//!
//! The `StatCollector` tracks important metrics throughout the crawling process,
//! including request counts, response statistics, item processing metrics, and
//! performance indicators. This data is essential for monitoring crawl progress,
//! diagnosing issues, and optimizing performance.
//!
//! ## Key Metrics Tracked
//!
//! - **Request Metrics**: Enqueued, sent, succeeded, failed, retried, and dropped requests
//! - **Response Metrics**: Received, cached, and status code distributions
//! - **Item Metrics**: Scraped, processed, and dropped items
//! - **Performance Metrics**: Throughput, response times, and bandwidth usage
//! - **Timing Metrics**: Elapsed time and processing rates
//!
//! ## Features
//!
//! - **Thread-Safe**: Uses atomic operations for concurrent metric updates
//! - **Real-Time Monitoring**: Provides live statistics during crawling
//! - **Export Formats**: Supports JSON and Markdown export formats
//! - **Snapshot Capability**: Captures consistent state for reporting
//!
//! ## Example
//!
//! ```rust,ignore
//! use spider_core::StatCollector;
//!
//! let stats = StatCollector::new();
//!
//! // During crawling, metrics are automatically updated
//! stats.increment_requests_sent();
//! stats.increment_items_scraped();
//!
//! // Export statistics in various formats
//! println!("{}", stats.to_json_string_pretty().unwrap());
//! println!("{}", stats.to_markdown_string());
//! ```

use spider_util::error::SpiderError;
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
// Thread-safe exponential moving average for tracking recent rates
#[derive(Debug)]
pub(crate) struct ExpMovingAverage {
    alpha: f64,  // Smoothing factor (typically 0.1-0.3)
    rate: Arc<parking_lot::RwLock<f64>>,
    last_update: Arc<parking_lot::RwLock<Instant>>,
}

impl ExpMovingAverage {
    fn new(alpha: f64) -> Self {
        ExpMovingAverage {
            alpha,
            rate: Arc::new(parking_lot::RwLock::new(0.0)),
            last_update: Arc::new(parking_lot::RwLock::new(Instant::now())),
        }
    }

    fn update(&self, count: usize) {
        let now = Instant::now();
        let mut last_update = self.last_update.write();
        let time_delta = now.duration_since(*last_update).as_secs_f64();
        *last_update = now;
        
        if time_delta > 0.0 {
            let current_rate = count as f64 / time_delta;
            let mut rate = self.rate.write();
            // Apply exponential moving average formula
            *rate = self.alpha * current_rate + (1.0 - self.alpha) * (*rate);
        }
    }

    fn get_rate(&self) -> f64 {
        *self.rate.read()
    }
}

// A snapshot of the current statistics, used for reporting.
// This avoids code duplication in the various export/display methods.
struct StatsSnapshot {
    requests_enqueued: usize,
    requests_sent: usize,
    requests_succeeded: usize,
    requests_failed: usize,
    requests_retried: usize,
    requests_dropped: usize,
    responses_received: usize,
    responses_from_cache: usize,
    total_bytes_downloaded: usize,
    items_scraped: usize,
    items_processed: usize,
    items_dropped_by_pipeline: usize,
    response_status_counts: HashMap<u16, usize>,
    elapsed_duration: Duration,
    average_request_time: Option<Duration>,
    fastest_request_time: Option<Duration>,
    slowest_request_time: Option<Duration>,
    request_time_count: usize,
    
    // Recent rates from sliding windows
    recent_requests_per_second: f64,
    recent_responses_per_second: f64,
    recent_items_per_second: f64,
}

impl StatsSnapshot {
    fn formatted_duration(&self) -> String {
        format!("{:?}", self.elapsed_duration)
    }

    fn formatted_request_time(&self, duration: Option<Duration>) -> String {
        match duration {
            Some(d) => {
                if d.as_millis() < 1000 {
                    format!("{} ms", d.as_millis())
                } else {
                    format!("{:.2} s", d.as_secs_f64())
                }
            }
            None => "N/A".to_string(),
        }
    }

    fn requests_per_second(&self) -> f64 {
        self.recent_requests_per_second
    }

    fn responses_per_second(&self) -> f64 {
        self.recent_responses_per_second
    }

    fn items_per_second(&self) -> f64 {
        self.recent_items_per_second
    }

    fn formatted_bytes(&self) -> String {
        const KB: usize = 1024;
        const MB: usize = 1024 * KB;
        const GB: usize = 1024 * MB;

        if self.total_bytes_downloaded >= GB {
            format!("{:.2} GB", self.total_bytes_downloaded as f64 / GB as f64)
        } else if self.total_bytes_downloaded >= MB {
            format!("{:.2} MB", self.total_bytes_downloaded as f64 / MB as f64)
        } else if self.total_bytes_downloaded >= KB {
            format!("{:.2} KB", self.total_bytes_downloaded as f64 / KB as f64)
        } else {
            format!("{} B", self.total_bytes_downloaded)
        }
    }
}

/// Collects and stores various statistics about the crawler's operation.
#[derive(Debug, serde::Serialize)]
pub struct StatCollector {
    // Crawl-related metrics
    #[serde(skip)]
    pub start_time: Instant,

    // Request-related metrics
    pub requests_enqueued: AtomicUsize,
    pub requests_sent: AtomicUsize,
    pub requests_succeeded: AtomicUsize,
    pub requests_failed: AtomicUsize,
    pub requests_retried: AtomicUsize,
    pub requests_dropped: AtomicUsize,

    // Response-related metrics
    pub responses_received: AtomicUsize,
    pub responses_from_cache: AtomicUsize,
    pub response_status_counts: Arc<dashmap::DashMap<u16, usize>>, // e.g., 200, 404, 500
    pub total_bytes_downloaded: AtomicUsize,

    // Add more advanced response time metrics if needed (e.g., histograms)

    // Item-related metrics
    pub items_scraped: AtomicUsize,
    pub items_processed: AtomicUsize,
    pub items_dropped_by_pipeline: AtomicUsize,

    // Timing metrics
    pub request_times: Arc<dashmap::DashMap<String, Duration>>,
    
    // Exponential moving average metrics for accurate speed calculations
    #[serde(skip)]
    requests_sent_ema: ExpMovingAverage,
    #[serde(skip)]
    responses_received_ema: ExpMovingAverage,
    #[serde(skip)]
    items_scraped_ema: ExpMovingAverage,
    
    // Track previous counts for rate calculation
    #[serde(skip)]
    prev_requests_sent: AtomicUsize,
    #[serde(skip)]
    prev_responses_received: AtomicUsize,
    #[serde(skip)]
    prev_items_scraped: AtomicUsize,
}

impl StatCollector {
    /// Creates a new `StatCollector` with all counters initialized to zero.
    pub(crate) fn new() -> Self {
        StatCollector {
            start_time: Instant::now(),
            requests_enqueued: AtomicUsize::new(0),
            requests_sent: AtomicUsize::new(0),
            requests_succeeded: AtomicUsize::new(0),
            requests_failed: AtomicUsize::new(0),
            requests_retried: AtomicUsize::new(0),
            requests_dropped: AtomicUsize::new(0),
            responses_received: AtomicUsize::new(0),
            responses_from_cache: AtomicUsize::new(0),
            response_status_counts: Arc::new(dashmap::DashMap::new()),
            total_bytes_downloaded: AtomicUsize::new(0),
            items_scraped: AtomicUsize::new(0),
            items_processed: AtomicUsize::new(0),
            items_dropped_by_pipeline: AtomicUsize::new(0),
            request_times: Arc::new(dashmap::DashMap::new()),
            // Initialize exponential moving averages for recent speed calculations (alpha = 0.2 for good balance)
            requests_sent_ema: ExpMovingAverage::new(0.2),
            responses_received_ema: ExpMovingAverage::new(0.2),
            items_scraped_ema: ExpMovingAverage::new(0.2),
            
            // Initialize previous counts to 0
            prev_requests_sent: AtomicUsize::new(0),
            prev_responses_received: AtomicUsize::new(0),
            prev_items_scraped: AtomicUsize::new(0),
        }
    }

    /// Creates a snapshot of the current statistics.
    /// This is the single source of truth for all presentation logic.
    fn snapshot(&self) -> StatsSnapshot {
        let mut status_counts: HashMap<u16, usize> = HashMap::new();
        for entry in self.response_status_counts.iter() {
            let (key, value) = entry.pair();
            status_counts.insert(*key, *value);
        }

        // Get recent rates from exponential moving averages
        let recent_requests_per_second = self.requests_sent_ema.get_rate();
        let recent_responses_per_second = self.responses_received_ema.get_rate();
        let recent_items_per_second = self.items_scraped_ema.get_rate();

        StatsSnapshot {
            requests_enqueued: self.requests_enqueued.load(Ordering::SeqCst),
            requests_sent: self.requests_sent.load(Ordering::SeqCst),
            requests_succeeded: self.requests_succeeded.load(Ordering::SeqCst),
            requests_failed: self.requests_failed.load(Ordering::SeqCst),
            requests_retried: self.requests_retried.load(Ordering::SeqCst),
            requests_dropped: self.requests_dropped.load(Ordering::SeqCst),
            responses_received: self.responses_received.load(Ordering::SeqCst),
            responses_from_cache: self.responses_from_cache.load(Ordering::SeqCst),
            total_bytes_downloaded: self.total_bytes_downloaded.load(Ordering::SeqCst),
            items_scraped: self.items_scraped.load(Ordering::SeqCst),
            items_processed: self.items_processed.load(Ordering::SeqCst),
            items_dropped_by_pipeline: self.items_dropped_by_pipeline.load(Ordering::SeqCst),
            response_status_counts: status_counts,
            elapsed_duration: self.start_time.elapsed(),
            average_request_time: self.average_request_time(),
            fastest_request_time: self.fastest_request_time(),
            slowest_request_time: self.slowest_request_time(),
            request_time_count: self.request_time_count(),
            
            // Recent rates from sliding windows
            recent_requests_per_second,
            recent_responses_per_second,
            recent_items_per_second,
        }
    }

    /// Increments the count of enqueued requests.
    pub(crate) fn increment_requests_enqueued(&self) {
        self.requests_enqueued.fetch_add(1, Ordering::SeqCst);
    }

    /// Increments the count of sent requests.
    pub(crate) fn increment_requests_sent(&self) {
        let new_count = self.requests_sent.fetch_add(1, Ordering::SeqCst) + 1;
        // Calculate how many requests were sent since the last update
        let prev_count = self.prev_requests_sent.load(Ordering::SeqCst);
        let delta = new_count - prev_count;
        self.requests_sent_ema.update(delta);
        // Update the previous count
        self.prev_requests_sent.store(new_count, Ordering::SeqCst);
    }

    /// Increments the count of successful requests.
    pub(crate) fn increment_requests_succeeded(&self) {
        self.requests_succeeded.fetch_add(1, Ordering::SeqCst);
    }

    /// Increments the count of failed requests.
    pub(crate) fn increment_requests_failed(&self) {
        self.requests_failed.fetch_add(1, Ordering::SeqCst);
    }

    /// Increments the count of retried requests.
    pub(crate) fn increment_requests_retried(&self) {
        self.requests_retried.fetch_add(1, Ordering::SeqCst);
    }

    /// Increments the count of dropped requests.
    pub(crate) fn increment_requests_dropped(&self) {
        self.requests_dropped.fetch_add(1, Ordering::SeqCst);
    }

    /// Increments the count of received responses.
    pub(crate) fn increment_responses_received(&self) {
        let new_count = self.responses_received.fetch_add(1, Ordering::SeqCst) + 1;
        // Calculate how many responses were received since the last update
        let prev_count = self.prev_responses_received.load(Ordering::SeqCst);
        let delta = new_count - prev_count;
        self.responses_received_ema.update(delta);
        // Update the previous count
        self.prev_responses_received.store(new_count, Ordering::SeqCst);
    }

    /// Increments the count of responses served from cache.
    pub(crate) fn increment_responses_from_cache(&self) {
        self.responses_from_cache.fetch_add(1, Ordering::SeqCst);
    }

    /// Records a response status code.
    pub(crate) fn record_response_status(&self, status_code: u16) {
        *self.response_status_counts.entry(status_code).or_insert(0) += 1;
    }

    /// Adds to the total bytes downloaded.
    pub(crate) fn add_bytes_downloaded(&self, bytes: usize) {
        self.total_bytes_downloaded
            .fetch_add(bytes, Ordering::SeqCst);
    }

    /// Increments the count of scraped items.
    pub(crate) fn increment_items_scraped(&self) {
        let new_count = self.items_scraped.fetch_add(1, Ordering::SeqCst) + 1;
        // Calculate how many items were scraped since the last update
        let prev_count = self.prev_items_scraped.load(Ordering::SeqCst);
        let delta = new_count - prev_count;
        self.items_scraped_ema.update(delta);
        // Update the previous count
        self.prev_items_scraped.store(new_count, Ordering::SeqCst);
    }

    /// Increments the count of processed items.
    pub(crate) fn increment_items_processed(&self) {
        self.items_processed.fetch_add(1, Ordering::SeqCst);
    }

    /// Increments the count of items dropped by pipelines.
    pub(crate) fn increment_items_dropped_by_pipeline(&self) {
        self.items_dropped_by_pipeline
            .fetch_add(1, Ordering::SeqCst);
    }

    /// Records the time taken for a request.
    pub fn record_request_time(&self, url: &str, duration: Duration) {
        self.request_times.insert(url.to_string(), duration);
    }

    /// Calculates the average request time across all recorded requests.
    pub fn average_request_time(&self) -> Option<Duration> {
        let times: Vec<Duration> = self
            .request_times
            .iter()
            .map(|entry| *entry.value())
            .collect();
        if times.is_empty() {
            None
        } else {
            let total_nanos: u128 = times.iter().map(|d| d.as_nanos()).sum();
            let avg_nanos = total_nanos / times.len() as u128;
            Some(Duration::from_nanos(avg_nanos as u64))
        }
    }

    /// Gets the fastest request time among all recorded requests.
    pub fn fastest_request_time(&self) -> Option<Duration> {
        self.request_times.iter().map(|entry| *entry.value()).min()
    }

    /// Gets the slowest request time among all recorded requests.
    pub fn slowest_request_time(&self) -> Option<Duration> {
        self.request_times.iter().map(|entry| *entry.value()).max()
    }

    /// Gets the total number of recorded request times.
    pub fn request_time_count(&self) -> usize {
        self.request_times.len()
    }

    /// Gets the request time for a specific URL.
    pub fn get_request_time(&self, url: &str) -> Option<Duration> {
        self.request_times
            .get(url)
            .map(|duration| *duration.value())
    }

    /// Gets all recorded request times as a vector of (URL, Duration) pairs.
    pub fn get_all_request_times(&self) -> Vec<(String, Duration)> {
        self.request_times
            .iter()
            .map(|entry| (entry.key().clone(), *entry.value()))
            .collect()
    }

    /// Clears all recorded request times.
    pub fn clear_request_times(&self) {
        self.request_times.clear();
    }

    /// Converts the snapshot into a JSON string.
    pub fn to_json_string(&self) -> Result<String, SpiderError> {
        Ok(serde_json::to_string(self)?)
    }

    /// Converts the snapshot into a pretty-printed JSON string.
    pub fn to_json_string_pretty(&self) -> Result<String, SpiderError> {
        Ok(serde_json::to_string_pretty(self)?)
    }

    /// Exports the current statistics to a Markdown formatted string.
    pub fn to_markdown_string(&self) -> String {
        let snapshot = self.snapshot();

        let status_codes_list: String = snapshot
            .response_status_counts
            .iter()
            .map(|(code, count)| format!("- **{}**: {}", code, count))
            .collect::<Vec<String>>()
            .join("\n");
        let status_codes_output = if status_codes_list.is_empty() {
            "N/A".to_string()
        } else {
            status_codes_list
        };

        format!(
            r#"# Crawl Statistics Report

- **Duration**: {}
- **Current Rate** (last 10s): {:.2} req/s, {:.2} resp/s, {:.2} item/s
- **Overall Rate** (total): {:.2} req/s, {:.2} resp/s, {:.2} item/s

## Requests
| Metric     | Count |
|------------|-------|
| Enqueued   | {}     |
| Sent       | {}     |
| Succeeded  | {}     |
| Failed     | {}     |
| Retried    | {}     |
| Dropped    | {}     |

## Responses
| Metric     | Count |
|------------|-------|
| Received   | {}     |
 From Cache | {}     |
| Downloaded | {}     |

## Items
| Metric     | Count |
|------------|--------|
| Scraped    | {}     |
| Processed  | {}     |
| Dropped    | {}     |

## Request Times
| Metric           | Value      |
|------------------|------------|
| Average Time     | {}         |
| Fastest Request  | {}         |
| Slowest Request  | {}         |
| Total Recorded   | {}         |

## Status Codes
{}
"#,
            snapshot.formatted_duration(),
            snapshot.requests_per_second(),
            snapshot.responses_per_second(),
            snapshot.items_per_second(),
            // Calculate cumulative speeds for comparison
            {
                let total_seconds = snapshot.elapsed_duration.as_secs() as f64;
                if total_seconds > 0.0 { snapshot.requests_sent as f64 / total_seconds } else { 0.0 }
            },
            {
                let total_seconds = snapshot.elapsed_duration.as_secs() as f64;
                if total_seconds > 0.0 { snapshot.responses_received as f64 / total_seconds } else { 0.0 }
            },
            {
                let total_seconds = snapshot.elapsed_duration.as_secs() as f64;
                if total_seconds > 0.0 { snapshot.items_scraped as f64 / total_seconds } else { 0.0 }
            },
            snapshot.requests_enqueued,
            snapshot.requests_sent,
            snapshot.requests_succeeded,
            snapshot.requests_failed,
            snapshot.requests_retried,
            snapshot.requests_dropped,
            snapshot.responses_received,
            snapshot.responses_from_cache,
            snapshot.formatted_bytes(),
            snapshot.items_scraped,
            snapshot.items_processed,
            snapshot.items_dropped_by_pipeline,
            snapshot.formatted_request_time(snapshot.average_request_time),
            snapshot.formatted_request_time(snapshot.fastest_request_time),
            snapshot.formatted_request_time(snapshot.slowest_request_time),
            snapshot.request_time_count,
            status_codes_output
        )
    }
}

impl Default for StatCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for StatCollector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let snapshot = self.snapshot();

        writeln!(f, "\nCrawl Statistics")?;
        writeln!(f, "----------------")?;
        writeln!(f, "  duration : {}", snapshot.formatted_duration())?;
        writeln!(
            f,
            "  speed    : req/s: {:.2}, resp/s: {:.2}, item/s: {:.2}",
            snapshot.requests_per_second(),
            snapshot.responses_per_second(),
            snapshot.items_per_second()
        )?;
        writeln!(
            f,
            "  requests : enqueued: {}, sent: {}, ok: {}, fail: {}, retry: {}, drop: {}",
            snapshot.requests_enqueued,
            snapshot.requests_sent,
            snapshot.requests_succeeded,
            snapshot.requests_failed,
            snapshot.requests_retried,
            snapshot.requests_dropped
        )?;
        writeln!(
            f,
            "  response : received: {}, from_cache: {}, downloaded: {}",
            snapshot.responses_received,
            snapshot.responses_from_cache,
            snapshot.formatted_bytes()
        )?;
        writeln!(
            f,
            "  items    : scraped: {}, processed: {}, dropped: {}",
            snapshot.items_scraped, snapshot.items_processed, snapshot.items_dropped_by_pipeline
        )?;
        writeln!(
            f,
            "  times    : avg: {}, fastest: {}, slowest: {}, total: {}",
            snapshot.formatted_request_time(snapshot.average_request_time),
            snapshot.formatted_request_time(snapshot.fastest_request_time),
            snapshot.formatted_request_time(snapshot.slowest_request_time),
            snapshot.request_time_count
        )?;

        let status_string = if snapshot.response_status_counts.is_empty() {
            "none".to_string()
        } else {
            snapshot
                .response_status_counts
                .iter()
                .map(|(code, count)| format!("{}: {}", code, count))
                .collect::<Vec<String>>()
                .join(", ")
        };

        writeln!(f, "  status   : {}\n", status_string)
    }
}
