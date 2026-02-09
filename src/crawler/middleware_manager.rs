//! Middleware Manager for efficient concurrent access.
//!
//! This module provides a `MiddlewareManager` that stores middlewares in a vector
//! and allows efficient concurrent access without requiring a mutex for every operation.

use spider_middleware::middleware::{Middleware, MiddlewareAction};
use spider_util::error::SpiderError;
use spider_util::request::Request;
use spider_util::response::Response;
use std::sync::Arc;
use tokio::sync::RwLock;

/// A manager for middlewares that provides efficient concurrent access.
pub struct MiddlewareManager<C> {
    middlewares: Vec<Box<dyn Middleware<C> + Send + Sync>>,
}

impl<C: Send + Sync + 'static> MiddlewareManager<C> {
    /// Creates a new `MiddlewareManager` with the given middlewares.
    pub fn new(middlewares: Vec<Box<dyn Middleware<C> + Send + Sync>>) -> Self {
        Self { middlewares }
    }

    /// Processes a request through all registered middlewares.
    pub async fn process_request(
        &mut self,
        client: &C,
        request: Request,
    ) -> Result<MiddlewareAction<Request>, SpiderError> {
        let mut current_request = request;

        for middleware in self.middlewares.iter_mut() {
            match middleware.process_request(client, current_request).await {
                Ok(MiddlewareAction::Continue(req)) => {
                    current_request = req;
                }
                Ok(action) => return Ok(action),
                Err(e) => return Err(e),
            }
        }

        Ok(MiddlewareAction::Continue(current_request))
    }

    /// Processes a response through all registered middlewares in reverse order.
    pub async fn process_response(
        &mut self,
        response: Response,
    ) -> Result<MiddlewareAction<Response>, SpiderError> {
        let mut current_response = response;

        // Process in reverse order to match the request processing chain
        for middleware in self.middlewares.iter_mut().rev() {
            match middleware.process_response(current_response).await {
                Ok(MiddlewareAction::Continue(res)) => {
                    current_response = res;
                }
                Ok(action) => return Ok(action),
                Err(e) => return Err(e),
            }
        }

        Ok(MiddlewareAction::Continue(current_response))
    }
}

/// A shared middleware manager that can be safely accessed concurrently.
pub struct SharedMiddlewareManager<C> {
    manager: Arc<RwLock<MiddlewareManager<C>>>,
}

impl<C: Send + Sync + Clone + 'static> SharedMiddlewareManager<C> {
    /// Creates a new `SharedMiddlewareManager` with the given middlewares.
    pub fn new(middlewares: Vec<Box<dyn Middleware<C> + Send + Sync>>) -> Self {
        Self {
            manager: Arc::new(RwLock::new(MiddlewareManager::new(middlewares))),
        }
    }

    /// Processes a request through all registered middlewares.
    pub async fn process_request(
        &self,
        client: &C,
        request: Request,
    ) -> Result<MiddlewareAction<Request>, SpiderError> {
        self.manager
            .write()
            .await
            .process_request(client, request)
            .await
    }

    /// Processes a response through all registered middlewares in reverse order.
    pub async fn process_response(
        &self,
        response: Response,
    ) -> Result<MiddlewareAction<Response>, SpiderError> {
        self.manager.write().await.process_response(response).await
    }
}

impl<C: Send + Sync + Clone + 'static> Clone for SharedMiddlewareManager<C> {
    fn clone(&self) -> Self {
        Self {
            manager: Arc::clone(&self.manager),
        }
    }
}
