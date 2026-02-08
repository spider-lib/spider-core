# spider-core

The core engine of the `spider-lib` web scraping framework.

## Overview

The `spider-core` crate provides the fundamental components for building web scrapers, including the main `Crawler`, `Scheduler`, `Spider` trait, and other essential infrastructure for managing the crawling process.

This crate implements the central orchestration layer of the web scraping framework. It manages the flow of requests and responses, coordinates concurrent operations, and provides the foundation for middleware and pipeline systems.

## Key Components

- **Crawler**: The main orchestrator that manages the crawling process
- **Scheduler**: Handles request queuing and duplicate detection
- **Spider**: Trait defining the interface for custom scraping logic
- **CrawlerBuilder**: Fluent API for configuring and building crawlers
- **Middleware**: Interceptors for processing requests and responses
- **Pipeline**: Processors for scraped items
- **Stats**: Collection and reporting of crawl statistics
- **Checkpoint**: Support for resuming crawls from saved state

## Architecture

The spider-core crate serves as the central hub connecting all other components of the spider framework. It handles:

- Request scheduling and execution
- Response processing
- Concurrent crawling operations
- State management
- Statistics collection
- Checkpoint and resume functionality

## Usage

Most users will interact with the components re-exported from this crate through the main `spider-lib` facade. However, this crate can be used independently for fine-grained control over the crawling process.

```rust
use spider_core::{Crawler, CrawlerBuilder, Spider, Scheduler};
use spider_util::{request::Request, response::Response, error::SpiderError};

#[derive(Default)]
struct MySpider;

#[spider_macro::scraped_item]
struct MyItem {
    title: String,
    url: String,
}

#[async_trait::async_trait]
impl Spider for MySpider {
    type Item = MyItem;

    fn start_urls(&self) -> Vec<&'static str> {
        vec!["https://example.com"]
    }

    async fn parse(&mut self, response: Response) -> Result<ParseOutput<Self::Item>, SpiderError> {
        // Custom parsing logic here
        todo!()
    }
}

async fn run_crawler() -> Result<(), SpiderError> {
    let crawler = CrawlerBuilder::new(MySpider).build().await?;
    crawler.start_crawl().await
}
```

## Dependencies

This crate depends on:
- `spider-util`: For basic data structures and utilities
- `spider-middleware`: For request/response processing
- `spider-downloader`: For HTTP request execution
- `spider-pipeline`: For item processing
- Various external crates for async processing, serialization, and data structures

## License

This project is licensed under the MIT License - see the [LICENSE](../LICENSE) file for details.