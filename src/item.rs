//! Data structures for scraped items and spider output in `spider-core`.
//!
//! This module defines the `ScrapedItem` trait, which is the core abstraction
//! for any data extracted by a web spider. Implementors of this trait define
//! the shape of the data they wish to collect.
//!
//! Additionally, the `ParseOutput` struct is provided as the standard return type
//! for a spider's `parse` method. It encapsulates both the `ScrapedItem`s
//! found on a page and any new `Request`s that should be scheduled for crawling.
//! This allows spiders to not only extract data but also to discover and
//! follow new links within the same processing step.

use spider_util::request::Request;
use serde_json::Value;
use std::any::Any;
use std::fmt::Debug;

/// The output of a spider's `parse` method.
#[derive(Debug, Clone)]
pub struct ParseOutput<I> {
    items: Vec<I>,
    requests: Vec<Request>,
}

impl<I> ParseOutput<I> {
    /// Creates a new, empty `ParseOutput`.
    pub fn new() -> Self {
        Self {
            items: Vec::new(),
            requests: Vec::new(),
        }
    }

    /// Consumes the `ParseOutput` and returns its inner items and requests.
    pub fn into_parts(self) -> (Vec<I>, Vec<Request>) {
        (self.items, self.requests)
    }

    /// Adds a scraped item to the output.
    pub fn add_item(&mut self, item: I) {
        self.items.push(item);
    }

    /// Adds a new request to be crawled.
    pub fn add_request(&mut self, request: Request) {
        self.requests.push(request);
    }

    /// Adds multiple scraped items to the output.
    pub fn add_items(&mut self, items: impl IntoIterator<Item = I>) {
        self.items.extend(items);
    }

    /// Adds multiple new requests to be crawled.
    pub fn add_requests(&mut self, requests: impl IntoIterator<Item = Request>) {
        self.requests.extend(requests);
    }
}

impl<I> Default for ParseOutput<I> {
    fn default() -> Self {
        Self::new()
    }
}

/// A trait representing a scraped item.
pub trait ScrapedItem: Debug + Send + Sync + Any + 'static {
    /// Returns the item as a `dyn Any` for downcasting.
    fn as_any(&self) -> &dyn Any;
    /// Clones the item into a `Box<dyn ScrapedItem>`.
    fn box_clone(&self) -> Box<dyn ScrapedItem + Send + Sync>;
    /// Converts the item to a `serde_json::Value`.
    fn to_json_value(&self) -> Value;
}

impl Clone for Box<dyn ScrapedItem + Send + Sync> {
    fn clone(&self) -> Self {
        self.box_clone()
    }
}