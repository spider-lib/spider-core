//! # Crawler Module
//!
//! Implements the core crawling engine that orchestrates the web scraping process.
//!
//! ## Overview
//!
//! The crawler module provides the main `Crawler` struct and its associated
//! components that manage the entire scraping workflow. It handles requests,
//! responses, items, and coordinates the various subsystems including
//! downloaders, middlewares, parsers, and pipelines.
//!
//! ## Key Components
//!
//! - **Crawler**: The central orchestrator that manages the crawling lifecycle
//! - **Downloader Task**: Handles HTTP requests and response retrieval
//! - **Parser Task**: Processes responses and extracts data according to spider logic
//! - **Item Processor**: Handles scraped items through registered pipelines
//! - **Middleware Manager**: Coordinates request/response processing through middlewares
//!
//! ## Architecture
//!
//! The crawler uses an asynchronous, task-based model where different operations
//! run concurrently in separate Tokio tasks. Communication between components
//! happens through async channels, allowing for high-throughput processing.
//!
//! ## Internal Components
//!
//! These are implementation details and are not typically used directly:
//! - `spawn_downloader_task`: Creates the task responsible for downloading web pages
//! - `spawn_parser_task`: Creates the task responsible for parsing responses
//! - `spawn_item_processor_task`: Creates the task responsible for processing items
//! - `SharedMiddlewareManager`: Manages concurrent access to middlewares

mod core;
mod item_processor;
mod middleware_manager;
mod request_handler;
mod response_parser;

pub use core::Crawler;
pub(crate) use item_processor::spawn_item_processor_task;
pub(crate) use middleware_manager::SharedMiddlewareManager;
pub(crate) use request_handler::spawn_downloader_task;
pub(crate) use response_parser::spawn_parser_task;

