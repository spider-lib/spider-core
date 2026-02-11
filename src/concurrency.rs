//! Advanced concurrency utilities for the spider framework.
//! 
//! This module provides utilities for adaptive concurrency control, 
//! resource management, and performance optimization across the framework.

use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::Instant;
use dashmap::DashMap;

/// Adaptive semaphore that adjusts its permits based on performance metrics
pub struct AdaptiveSemaphore {
    /// Current number of permits
    permits: Arc<RwLock<usize>>,
    /// Maximum number of permits allowed
    max_permits: usize,
    /// Minimum number of permits allowed
    min_permits: usize,
    /// Performance metrics for decision making
    metrics: Arc<DashMap<String, PerformanceMetric>>,
    /// Target response time threshold (in milliseconds)
    target_response_time_ms: u64,
}

/// Performance metric for tracking request/response times
#[derive(Debug, Clone)]
pub struct PerformanceMetric {
    /// Average response time
    pub avg_response_time: Duration,
    /// Number of samples
    pub sample_count: usize,
    /// Error rate
    pub error_rate: f64,
    /// Last update time
    pub last_update: Instant,
}

impl AdaptiveSemaphore {
    /// Creates a new adaptive semaphore
    pub fn new(initial_permits: usize, max_permits: usize, min_permits: usize) -> Self {
        Self {
            permits: Arc::new(RwLock::new(initial_permits)),
            max_permits,
            min_permits,
            metrics: Arc::new(DashMap::new()),
            target_response_time_ms: 1000, // 1 second default
        }
    }

    /// Updates performance metrics for a specific endpoint
    pub async fn update_metrics(&self, endpoint: &str, response_time: Duration, success: bool) {
        let success_flag = if success { 1.0 } else { 0.0 };
        
        self.metrics
            .entry(endpoint.to_string())
            .and_modify(|metric| {
                // Update average response time with exponential moving average
                let new_avg = Duration::from_nanos(
                    ((metric.avg_response_time.as_nanos() as f64 * 0.7) +
                     (response_time.as_nanos() as f64 * 0.3)) as u64
                );
                
                // Update error rate
                let total_samples = metric.sample_count as f64 + 1.0;
                let new_error_rate = (metric.error_rate * metric.sample_count as f64 + (1.0 - success_flag)) / total_samples;
                
                metric.avg_response_time = new_avg;
                metric.error_rate = new_error_rate;
                metric.sample_count += 1;
                metric.last_update = Instant::now();
            })
            .or_insert(PerformanceMetric {
                avg_response_time: response_time,
                sample_count: 1,
                error_rate: 1.0 - success_flag,
                last_update: Instant::now(),
            });
    }

    /// Gets the current number of permits
    pub async fn current_permits(&self) -> usize {
        *self.permits.read().await
    }

    /// Adjusts permits based on performance metrics
    pub async fn adjust_permits(&self) {
        let current_permits = *self.permits.read().await;
        
        // Calculate average metrics across all endpoints
        let mut total_response_time = Duration::from_millis(0);
        let mut total_error_rate = 0.0;
        let mut endpoint_count = 0;
        
        for metric in self.metrics.iter() {
            total_response_time += metric.avg_response_time;
            total_error_rate += metric.error_rate;
            endpoint_count += 1;
        }
        
        if endpoint_count == 0 {
            return; // No metrics to base decision on
        }
        
        let avg_response_time = Duration::from_nanos(
            (total_response_time.as_nanos() / endpoint_count as u128) as u64
        );
        let avg_error_rate = total_error_rate / endpoint_count as f64;
        
        let mut new_permits = current_permits;
        
        // Adjust based on response time
        if avg_response_time.as_millis() > self.target_response_time_ms as u128 {
            // Response time too high, decrease permits
            new_permits = new_permits.saturating_sub(1).max(self.min_permits);
        } else if avg_response_time.as_millis() < (self.target_response_time_ms / 2) as u128 {
            // Response time good, increase permits if not at max
            if new_permits < self.max_permits {
                new_permits += 1;
            }
        }
        
        // Adjust based on error rate
        if avg_error_rate > 0.1 { // More than 10% errors
            new_permits = new_permits.saturating_sub(2).max(self.min_permits);
        } else if avg_error_rate < 0.01 { // Less than 1% errors
            if new_permits < self.max_permits {
                new_permits = std::cmp::min(new_permits + 1, self.max_permits);
            }
        }
        
        // Apply the new permit count
        *self.permits.write().await = new_permits;
    }

    /// Sets the target response time threshold
    pub async fn set_target_response_time(&mut self, target_ms: u64) {
        self.target_response_time_ms = target_ms;
    }
}

/// Resource quota manager for distributed resource allocation
#[derive(Debug)]
pub struct ResourceQuotaManager {
    /// Total available resources
    total_resources: usize,
    /// Currently allocated resources
    allocated_resources: Arc<RwLock<usize>>,
    /// Resource allocations per component
    component_allocations: Arc<DashMap<String, usize>>,
}

impl ResourceQuotaManager {
    /// Creates a new resource quota manager
    pub fn new(total_resources: usize) -> Self {
        Self {
            total_resources,
            allocated_resources: Arc::new(RwLock::new(0)),
            component_allocations: Arc::new(DashMap::new()),
        }
    }

    /// Allocates resources to a component
    pub async fn allocate_resources(&self, component: &str, amount: usize) -> Result<usize, String> {
        let mut allocated = self.allocated_resources.write().await;
        
        if *allocated + amount > self.total_resources {
            return Err(format!(
                "Insufficient resources: requested {}, available {}", 
                amount, 
                self.total_resources - *allocated
            ));
        }
        
        *allocated += amount;
        self.component_allocations.insert(component.to_string(), amount);
        
        Ok(amount)
    }

    /// Releases resources from a component
    pub async fn release_resources(&self, component: &str) {
        if let Some((_, amount)) = self.component_allocations.remove(component) {
            let mut allocated = self.allocated_resources.write().await;
            *allocated = allocated.saturating_sub(amount);
        }
    }

    /// Gets available resources
    pub async fn available_resources(&self) -> usize {
        let allocated = self.allocated_resources.read().await;
        self.total_resources - *allocated
    }

    /// Gets current allocation for a component
    pub fn get_allocation(&self, component: &str) -> Option<usize> {
        self.component_allocations.get(component).map(|v| *v.value())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_adaptive_semaphore() {
        let semaphore = AdaptiveSemaphore::new(5, 10, 1);
        
        assert_eq!(semaphore.current_permits().await, 5);
        
        // Update metrics with good performance
        semaphore.update_metrics("test_endpoint", Duration::from_millis(100), true).await;
        semaphore.adjust_permits().await;
        
        // Should remain the same or increase slightly
        let permits_after = semaphore.current_permits().await;
        assert!(permits_after >= 5);
    }

    #[tokio::test]
    async fn test_resource_quota_manager() {
        let manager = ResourceQuotaManager::new(100);
        
        assert_eq!(manager.available_resources().await, 100);
        
        // Allocate resources
        assert!(manager.allocate_resources("component1", 30).await.is_ok());
        assert_eq!(manager.available_resources().await, 70);
        
        // Try to allocate more than available
        assert!(manager.allocate_resources("component2", 80).await.is_err());
        
        // Release resources
        manager.release_resources("component1").await;
        assert_eq!(manager.available_resources().await, 100);
    }
}