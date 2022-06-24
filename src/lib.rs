pub use async_trait::async_trait;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum RouterError {
    #[error("unsupported topic")]
    UnsupportedTopicName { topic: String },
    #[error("handler error")]
    HandlerError(#[from] Box<dyn std::error::Error>),
}

#[derive(Default)]
pub struct Router {
    table: std::collections::HashMap<String, Box<dyn RouteHandler>>,
}

impl Router {
    pub fn add_handler(
        &mut self,
        topic: &str,
        handler: Box<dyn RouteHandler>,
    ) -> std::result::Result<(), RouterError> {
        if topic.contains('#') || topic.contains('+') {
            Err(RouterError::UnsupportedTopicName {
                topic: topic.to_owned(),
            })
        } else {
            self.table.insert(String::from(topic), handler);
            Ok(())
        }
    }

    pub async fn handle_message(
        &mut self,
        topic: &str,
        content: &[u8],
    ) -> Result<bool, RouterError> {
        if let Some(handler) = self.table.get_mut(topic) {
            handler.call(topic, content).await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn topics_for_subscription(&self) -> impl Iterator<Item = &String> {
        self.table.keys()
    }
}

#[async_trait]
pub trait RouteHandler: Send + Sync {
    async fn call(&mut self, topic: &str, content: &[u8]) -> Result<(), RouterError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    };

    #[derive(Debug)]
    pub struct TestHandler {
        call_counter: Arc<AtomicU32>,
    }

    impl TestHandler {
        pub fn new() -> Box<Self> {
            Box::new(Self {
                call_counter: Default::default(),
            })
        }
        pub fn with_counter(call_counter: Arc<AtomicU32>) -> Box<Self> {
            Box::new(Self { call_counter })
        }
    }

    #[async_trait]
    impl RouteHandler for TestHandler {
        async fn call(&mut self, _topic: &str, _content: &[u8]) -> Result<(), RouterError> {
            self.call_counter.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[test]
    fn subscribe_with_test_handler() {
        let mut router = Router::default();
        router.add_handler("home/test", TestHandler::new()).unwrap();
    }

    #[tokio::test]
    async fn test_handler_gets_called() {
        let mut router = Router::default();
        let counter = Arc::new(AtomicU32::new(0));
        router
            .add_handler("home/test", TestHandler::with_counter(counter.clone()))
            .unwrap();
        router.handle_message("home/test", &[0]).await.unwrap();
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }
}
