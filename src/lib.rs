//! # spider-core
//!
//! The core engine of the `spider-lib` web scraping framework.
//!
//! This crate provides the fundamental components for building web scrapers,
//! including the main `Crawler`, `Scheduler`, `Spider` trait, and other
//! essential infrastructure for managing the crawling process.
//!
//! ## Overview
//!
//! The `spider-core` crate implements the central orchestration layer of the
//! web scraping framework. It manages the flow of requests and responses,
//! coordinates concurrent operations, and provides the foundation for
//! middleware and pipeline systems.
//!
//! ## Key Components
//!
//! - **Crawler**: The main orchestrator that manages the crawling process
//! - **Scheduler**: Handles request queuing and duplicate detection
//! - **Spider**: Trait defining the interface for custom scraping logic
//! - **CrawlerBuilder**: Fluent API for configuring and building crawlers
//! - **Middleware**: Interceptors for processing requests and responses
//! - **Pipeline**: Processors for scraped items
//! - **Stats**: Collection and reporting of crawl statistics
//!
//! ## Usage
//!
//! Most users will interact with the components re-exported from this crate
//! through the main `spider-lib` facade. However, this crate can be used
//! independently for fine-grained control over the crawling process.
//!
//! ```rust,ignore
//! use spider_core::{Crawler, CrawlerBuilder, Spider, Scheduler};
//! use spider_util::{request::Request, response::Response, error::SpiderError};
//!
//! #[derive(Default)]
//! struct MySpider;
//!
//! #[spider_macro::scraped_item]
//! struct MyItem {
//!     title: String,
//!     url: String,
//! }
//!
//! #[async_trait::async_trait]
//! impl Spider for MySpider {
//!     type Item = MyItem;
//!
//!     fn start_urls(&self) -> Vec<&'static str> {
//!         vec!["https://example.com"]
//!     }
//!
//!     async fn parse(&mut self, response: Response) -> Result<ParseOutput<Self::Item>, SpiderError> {
//!         // Custom parsing logic here
//!         todo!()
//!     }
//! }
//!
//! async fn run_crawler() -> Result<(), SpiderError> {
//!     let crawler = CrawlerBuilder::new(MySpider).build().await?;
//!     crawler.start_crawl().await
//! }
//! ```

pub mod builder;
#[cfg(feature = "checkpoint")]
pub mod checkpoint;
pub mod crawler;
pub mod prelude;
pub mod scheduler;
pub mod spider;
pub mod state;
pub mod stats;

// Re-export SchedulerCheckpoint and Checkpoint (when checkpoint feature is enabled)
#[cfg(feature = "checkpoint")]
pub use checkpoint::{Checkpoint, SchedulerCheckpoint};

pub use spider_downloader::{Downloader, ReqwestClientDownloader, SimpleHttpClient};

// Re-export CookieStore (when cookie-store feature is enabled)
#[cfg(feature = "cookie-store")]
pub use cookie_store::CookieStore;

pub use builder::CrawlerBuilder;
pub use crawler::Crawler;
pub use scheduler::Scheduler;
pub use spider_macro::scraped_item;

pub use async_trait::async_trait;
pub use dashmap::DashMap;
pub use spider::Spider;
pub use tokio;
