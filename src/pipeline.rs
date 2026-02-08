//! Trait for defining item processing pipelines in `spider-core`.
//!
//! This module provides the `Pipeline` trait, which is a fundamental abstraction
//! for post-processing `ScrapedItem`s after they have been extracted by a spider.
//! Pipelines allow for a modular approach to handling scraped data, enabling
//! operations such as:
//! - Storing items in databases or files.
//! - Validating and cleaning data.
//! - Deduplicating entries.
//! - Performing further transformations.
//!
//! Implementors of this trait define the `process_item` method, which receives
//! a `ScrapedItem` and can modify, drop, or pass it along the pipeline.
//! Pipelines also support state management for checkpointing and cleanup operations.

use spider_util::error::PipelineError;
use crate::item::ScrapedItem;
use async_trait::async_trait;
use serde_json::Value;

/// The `Pipeline` trait defines the contract for item processing pipelines.
///
/// Pipelines are responsible for processing scraped items, such as storing them in a database,
/// writing them to a file, or performing data validation.
#[async_trait]
pub trait Pipeline<I: ScrapedItem>: Send + Sync + 'static {
    /// Returns the name of the pipeline.
    fn name(&self) -> &str;

    /// Processes a single scraped item.
    ///
    /// This method can perform any processing on the item, such as storing it, validating it,
    /// or passing it to another pipeline. It can also choose to drop the item by returning `Ok(None)`.
    async fn process_item(&self, item: I) -> Result<Option<I>, PipelineError>;

    /// Called when the spider is closing.
    ///
    /// This method can be used to perform any cleanup tasks, such as closing file handles or
    /// database connections.
    async fn close(&self) -> Result<(), PipelineError> {
        Ok(())
    }

    /// Returns the current state of the pipeline as a JSON value.
    ///
    /// This method is called during checkpointing to save the pipeline's state.
    /// The returned state should be sufficient to restore the pipeline to its current
    /// state using `restore_state`.
    async fn get_state(&self) -> Result<Option<Value>, PipelineError> {
        Ok(None)
    }

    /// Restores the pipeline's state from a JSON value.
    ///
    /// This method is called when resuming from a checkpoint. The provided state
    /// should be used to restore the pipeline to the state it was in when the
    /// checkpoint was created.
    async fn restore_state(&self, _state: Value) -> Result<(), PipelineError> {
        Ok(())
    }
}