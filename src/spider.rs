//! # Spider Module
//!
//! Defines the core `Spider` trait and related components for implementing custom web scrapers.
//!
//! ## Overview
//!
//! The `Spider` trait is the primary interface for defining custom scraping logic.
//! It specifies how to start a crawl (via start URLs) and how to process responses
//! to extract data and discover new URLs to follow. This trait follows the Scrapy
//! pattern of spiders that define the crawling behavior.
//!
//! ## Key Components
//!
//! - **Spider Trait**: The main trait for implementing custom scraping logic
//! - **ParseOutput**: Container for returning scraped items and new requests
//! - **Associated Types**: Define the item type and state type that the spider uses
//!
//! ## Implementation
//!
//! Implementors must define:
//! - `start_urls`: The initial URLs to begin the crawl
//! - `parse`: Logic for extracting data and discovering new URLs from responses
//! - `Item`: The type of data structure to store scraped information
//! - `State`: The type of state that the spider uses (must implement Default)
//!
//! ## Example
//!
//! ```rust,ignore
//! use spider_core::Spider;
//! use spider_util::{response::Response, error::SpiderError};
//! use async_trait::async_trait;
//!
//! #[spider_macro::scraped_item]
//! struct Article {
//!     title: String,
//!     content: String,
//! }
//!
//! // State for tracking page count
//! use std::sync::Arc;
//! use std::sync::atomic::{AtomicUsize, Ordering};
//! use dashmap::DashMap;
//!
//! #[derive(Clone, Default)]
//! struct ArticleSpiderState {
//!     page_count: Arc<AtomicUsize>,
//!     visited_urls: Arc<DashMap<String, bool>>,
//! }
//!
//! impl ArticleSpiderState {
//!     fn increment_page_count(&self) {
//!         self.page_count.fetch_add(1, Ordering::SeqCst);
//!     }
//!     
//!     fn mark_url_visited(&self, url: String) {
//!         self.visited_urls.insert(url, true);
//!     }
//! }
//!
//! struct ArticleSpider;
//!
//! #[async_trait]
//! impl Spider for ArticleSpider {
//!     type Item = Article;
//!     type State = ArticleSpiderState;
//!
//!     fn start_urls(&self) -> Vec<&'static str> {
//!         vec!["https://example.com/articles"]
//!     }
//!
//!     async fn parse(&self, response: Response, state: &Self::State) -> Result<ParseOutput<Self::Item>, SpiderError> {
//!         // Update state - can be done concurrently without blocking the spider
//!         state.increment_page_count();
//!         state.mark_url_visited(response.url.to_string());
//!
//!         let mut output = ParseOutput::new();
//!
//!         // Extract articles from the page
//!         // ... parsing logic ...
//!
//!         // Add discovered articles to output
//!         // output.add_item(Article { title, content });
//!
//!         // Add new URLs to follow
//!         // output.add_request(new_request);
//!
//!         Ok(output)
//!     }
//! }
//! ```

use spider_util::error::SpiderError;
use spider_util::item::{ParseOutput, ScrapedItem};
use spider_util::request::Request;
use spider_util::response::Response;

use anyhow::Result;
use async_trait::async_trait;
use url::Url;

/// Defines the contract for a web spider.
#[async_trait]
pub trait Spider: Send + Sync + 'static {
    /// The type of item that the spider scrapes.
    type Item: ScrapedItem;

    /// The type of state that the spider uses.
    /// Must implement Default so it can be instantiated as needed.
    type State: Default + Send + Sync;

    /// Returns the initial URLs to start crawling from.
    fn start_urls(&self) -> Vec<&'static str> {
        Vec::new()
    }

    /// Generates the initial requests to start crawling.
    fn start_requests(&self) -> Result<Vec<Request>, SpiderError> {
        let urls: Result<Vec<Url>, url::ParseError> =
            self.start_urls().into_iter().map(Url::parse).collect();
        Ok(urls?.into_iter().map(Request::new).collect())
    }

    /// Parses a response and extracts scraped items and new requests.
    /// 
    /// Note: This function takes an immutable reference to self (&self) instead of mutable (&mut self),
    /// eliminating the need for mutex locks when accessing the spider in concurrent environments.
    /// State that needs to be modified should be stored in the State type.
    async fn parse(&self, response: Response, state: &Self::State) -> Result<ParseOutput<Self::Item>, SpiderError>;

}
