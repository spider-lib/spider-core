//! A "prelude" for users of the `spider-core` crate.
//!
//! This prelude re-exports the most commonly used traits, structs, and macros
//! so that they can be easily imported.
//!
//! # Example
//!
//! ```
//! use spider_core::prelude::*;
//! ```

pub use crate::{
    // Core structs
    Crawler,
    // Core traits
    Downloader,
    Spider,
    // Essential re-exports for trait implementation
    async_trait,
    // Procedural macro
    scraped_item,
};

// Import types from other crates
pub use spider_middleware::middleware::{Middleware, MiddlewareAction};
pub use spider_util::{error::{SpiderError, PipelineError}, request::Request};