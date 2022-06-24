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

fn topic_valid(topic: &str) -> bool {
    let hash_count = topic.matches('#').count();
    match hash_count {
        0 => true,
        1 => topic.ends_with('#'),
        _ => false,
    }
}

fn check_no_wildcards(topic: &str) -> bool {
    !(topic.contains('#') || topic.contains('+'))
}

fn match_topic(key: &str, topic: &str) -> bool {
    // Note!
    // this will technically speaking matching something like:
    // "foo/#/bar" with "foo/#/pub"
    // this isn't correct but also isn't that catastrophic
    // We could also filter out those topics on addition
    let zip = std::iter::zip(key.split('/'), topic.split('/'));
    for pair in zip {
        match pair {
            ("#", _) => return true,
            ("+", _) => (),
            (a, b) if a == b => (),
            _ => return false,
        }
    }
    true
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

    // matcher tests
    #[test]
    fn test_match_simple() {
        let key = "foo/bar/baz";
        let topic = "foo/bar/baz";
        let matched = match_topic(key, topic);
        assert!(matched);
    }

    #[test]
    fn test_match_simple_with_plus() {
        let key = "foo/bar/+";
        let topic = "foo/bar/baz";
        let matched = match_topic(key, topic);
        assert!(matched);
    }

    #[test]
    fn test_match_simple_with_wildcard_pound_sign() {
        let key = "foo/#";
        let topic = "foo/bar/baz";
        let matched = match_topic(key, topic);
        assert!(matched);
    }

    #[test]
    fn test_match_full_wildcard() {
        let key = "#";
        let topic = "foo/bar/baz";
        let matched = match_topic(key, topic);
        assert!(matched);
    }

    #[test]
    fn test_do_not_match_different() {
        let key = "foo/BAZ";
        let topic = "foo/bar/baz";
        let matched = match_topic(key, topic);
        assert!(!matched);
    }

    #[test]
    fn test_do_not_match_different_plus_wildcard() {
        let key = "+/AAAA";
        let topic = "foo/bar/baz";
        let matched = match_topic(key, topic);
        assert!(!matched);
    }

    #[test]
    fn test_topic_valid_simple() {
        let valid = topic_valid("foo/bar/baz");
        assert!(valid);
    }

    #[test]
    fn test_topic_valid_simple_plus_wildcard() {
        let valid = topic_valid("foo/+/baz");
        assert!(valid);
    }

    #[test]
    fn test_topic_valid_simple_hash_end() {
        let valid = topic_valid("foo/bar/#");
        assert!(valid);
    }

    #[test]
    fn test_topic_not_valid_simple_hash_middle() {
        let valid = topic_valid("foo/#/bar");
        assert!(!valid);
    }

    // wildcards filter
    #[test]
    fn test_no_wildcards_simple() {
        let valid = check_no_wildcards("foo/bar");
        assert!(valid);
    }

    #[test]
    fn test_wildcards_plus() {
        let valid = check_no_wildcards("foo/+/bar");
        assert!(!valid);
    }

    #[test]
    fn test_wildcards_hash() {
        let valid = check_no_wildcards("foo/#");
        assert!(!valid);
    }
}
