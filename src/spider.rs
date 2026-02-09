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
//! - **Associated Types**: Define the item type that the spider produces
//!
//! ## Implementation
//!
//! Implementors must define:
//! - `start_urls`: The initial URLs to begin the crawl
//! - `parse`: Logic for extracting data and discovering new URLs from responses
//! - `Item`: The type of data structure to store scraped information
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
//! struct ArticleSpider;
//!
//! #[async_trait]
//! impl Spider for ArticleSpider {
//!     type Item = Article;
//!
//!     fn start_urls(&self) -> Vec<&'static str> {
//!         vec!["https://example.com/articles"]
//!     }
//!
//!     async fn parse(&mut self, response: Response) -> Result<ParseOutput<Self::Item>, SpiderError> {
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
#[cfg(feature = "stream")]
use spider_util::stream_response::StreamResponse;
#[cfg(not(feature = "stream"))]
pub struct StreamResponse;

use anyhow::Result;
use async_trait::async_trait;
use url::Url;

/// Defines the contract for a web spider.
#[async_trait]
pub trait Spider: Send + Sync + 'static {
    /// The type of item that the spider scrapes.
    type Item: ScrapedItem;

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
    #[cfg(feature = "stream")]
    async fn parse(&mut self, _response: Response) -> Result<ParseOutput<Self::Item>, SpiderError> {
        Ok(ParseOutput::new())
    }

    /// Parses a stream response and extracts scraped items and new requests.
    /// This method is optional and only available when the 'stream' feature is enabled.
    #[cfg(feature = "stream")]
    async fn parse_stream(&mut self, response: StreamResponse) -> Result<ParseOutput<Self::Item>, SpiderError>;

    /// Parses a response and extracts scraped items and new requests.
    #[cfg(not(feature = "stream"))]
    async fn parse(&mut self, response: Response) -> Result<ParseOutput<Self::Item>, SpiderError>;

    /// Parses a stream response and extracts scraped items and new requests.
    /// This method is optional and only available when the 'stream' feature is enabled.
    #[cfg(not(feature = "stream"))]
    async fn parse_stream(&mut self, _response: StreamResponse) -> Result<ParseOutput<Self::Item>, SpiderError> {
        Ok(ParseOutput::new())
    }

}
