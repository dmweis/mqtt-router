pub use async_trait::async_trait;

use itertools::{EitherOrBoth, Itertools};
use std::future::Future;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum RouterError {
    /// Topic isn't valid based on MQTT rules
    #[error("invalid topic")]
    InvalidTopicName { topic: String },
    /// Handler encountered and error while routing messages
    #[error(transparent)]
    HandlerError(#[from] anyhow::Error),
    /// Topic containes wildcards which is not legal based on MQTT spec
    #[error("trying to match a topic with wildcards")]
    TryingToHandleTopicWithWildcards,
}

struct Route<'a> {
    topic: String,
    handler: Box<dyn RouteHandler + 'a>,
}

impl<'a> From<(String, Box<dyn RouteHandler + 'a>)> for Route<'a> {
    fn from((topic, handler): (String, Box<dyn RouteHandler + 'a>)) -> Self {
        Self { topic, handler }
    }
}

/// Router with a set of handlers
#[derive(Default)]
pub struct Router<'a> {
    table: Vec<Route<'a>>,
}

impl<'a> Router<'a> {
    /// Add new handler for a topic key
    pub fn add_handler(
        &mut self,
        topic: &str,
        handler: Box<dyn RouteHandler>,
    ) -> std::result::Result<(), RouterError> {
        if !topic_valid(topic) {
            Err(RouterError::InvalidTopicName {
                topic: topic.to_owned(),
            })
        } else {
            self.table.push((String::from(topic), handler).into());
            Ok(())
        }
    }

    pub fn add_closure_handler<F, Fut>(
        &mut self,
        topic: &str,
        closure: F,
    ) -> std::result::Result<(), RouterError>
    where
        F: Fn(&str, &[u8]) -> Fut + Send + Sync + 'a,
        Fut: Future<Output = Result<(), anyhow::Error>> + Send + Sync + 'a,
    {
        // f(1, 2).await;

        let closure = ClosureRouteHanlder { closure };

        if !topic_valid(topic) {
            Err(RouterError::InvalidTopicName {
                topic: topic.to_owned(),
            })
        } else {
            let route = Route {
                topic: String::from(topic),
                handler: Box::new(closure),
            };
            self.table.push(route);
            Ok(())
        }
    }

    /// Execute all matching handlers
    ///
    /// If any handler fails this method stops executing
    pub async fn handle_message(
        &mut self,
        topic: &str,
        content: &[u8],
    ) -> Result<bool, RouterError> {
        if !check_no_wildcards(topic) {
            return Err(RouterError::TryingToHandleTopicWithWildcards);
        }
        let mut found = false;
        for Route {
            topic: topic_key,
            handler,
        } in &mut self.table
        {
            if match_topic(topic_key, topic) {
                handler.call(topic, content).await?;
                found = true;
            }
        }
        Ok(found)
    }

    /// Execute all matching handlers
    ///
    /// Execution continues if an error is encountered
    /// and Result::Err is returned at the end with all collected errors.
    pub async fn handle_message_ignore_errors(
        &mut self,
        topic: &str,
        content: &[u8],
    ) -> Result<bool, Vec<RouterError>> {
        if !check_no_wildcards(topic) {
            return Err(vec![RouterError::TryingToHandleTopicWithWildcards]);
        }
        let mut found = false;
        let mut errors = vec![];
        for Route {
            topic: topic_key,
            handler,
        } in &mut self.table
        {
            if match_topic(topic_key, topic) {
                found = true;
                if let Err(e) = handler.call(topic, content).await {
                    errors.push(e.into());
                }
            }
        }
        if !errors.is_empty() {
            Err(errors)
        } else {
            Ok(found)
        }
    }

    pub fn topics_for_subscription(&self) -> impl Iterator<Item = &String> {
        self.table.iter().map(|Route { topic, handler: _ }| topic)
    }
}

/// Async handler for a route
#[async_trait]
pub trait RouteHandler: Send + Sync {
    async fn call(&mut self, topic: &str, content: &[u8]) -> Result<(), anyhow::Error>;
}

struct ClosureRouteHanlder<F, Fut>
where
    F: Fn(&str, &[u8]) -> Fut + Send + Sync,
    Fut: Future<Output = Result<(), anyhow::Error>> + Send + Sync,
{
    closure: F,
}

#[async_trait]
impl<F, Fut> RouteHandler for ClosureRouteHanlder<F, Fut>
where
    F: Fn(&str, &[u8]) -> Fut + Send + Sync,
    Fut: Future<Output = Result<(), anyhow::Error>> + Send + Sync,
{
    async fn call(&mut self, topic: &str, content: &[u8]) -> Result<(), anyhow::Error> {
        (self.closure)(topic, content).await
    }
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

/// Check if topic matches key
///
/// # Arguments
///
/// * `key` - Topic lookup key
/// * `topic` - Topic to compare against key
///
/// Note that this method doesn't itself filter out incorrect topics or keys
/// such as topics that contain wildcards or keys that do container a # wildcard in the middle
/// It should only be used with other verification methdos
fn match_topic(key: &str, topic: &str) -> bool {
    let zip = key.split('/').zip_longest(topic.split('/'));
    for pair in zip {
        match pair {
            EitherOrBoth::Left(_) | EitherOrBoth::Right(_) => return false,
            EitherOrBoth::Both("#", _) => return true,
            EitherOrBoth::Both("+", _) => (),
            EitherOrBoth::Both(a, b) if a == b => (),
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
        async fn call(&mut self, _topic: &str, _content: &[u8]) -> Result<(), anyhow::Error> {
            self.call_counter.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[test]
    fn subscribe_with_test_handler() {
        let mut router = Router::default();
        router.add_handler("home/test", TestHandler::new()).unwrap();
    }

    #[test]
    fn all_topics_are_listed() {
        let mut router = Router::default();
        router.add_handler("a", TestHandler::new()).unwrap();
        router.add_handler("b", TestHandler::new()).unwrap();
        router.add_handler("c/#", TestHandler::new()).unwrap();
        let topics: std::collections::HashSet<String> =
            router.topics_for_subscription().cloned().collect();
        let expected: std::collections::HashSet<String> = ["a", "b", "c/#"]
            .into_iter()
            .map(|a| a.to_owned())
            .collect();
        assert_eq!(topics, expected);
    }

    #[test]
    fn router_detects_invalid_topic() {
        let mut router = Router::default();
        let error = router.add_handler("home/#/test", TestHandler::new());
        assert!(matches!(
            error,
            Err(RouterError::InvalidTopicName { topic: _ })
        ))
    }

    #[tokio::test]
    async fn router_detects_handling_of_wildcard_topic() {
        let mut router = Router::default();
        router.add_handler("home/#", TestHandler::new()).unwrap();
        let error = router.handle_message("home/#", &[0]).await;

        assert!(matches!(
            error,
            Err(RouterError::TryingToHandleTopicWithWildcards)
        ))
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

    #[tokio::test]
    async fn test_handler_calls_plus_wildcard_matcher() {
        let mut router = Router::default();
        let counter = Arc::new(AtomicU32::new(0));
        router
            .add_handler("home/+", TestHandler::with_counter(counter.clone()))
            .unwrap();
        router.handle_message("home/test", &[0]).await.unwrap();
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_handler_calls_hash_wildcard_matcher() {
        let mut router = Router::default();
        let counter = Arc::new(AtomicU32::new(0));
        router
            .add_handler("home/#", TestHandler::with_counter(counter.clone()))
            .unwrap();
        router.handle_message("home/test/one", &[0]).await.unwrap();
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
    fn do_not_match_longer_topics() {
        let key = "foo/bar";
        let topic = "foo/bar/baz";
        let matched = match_topic(key, topic);
        assert!(!matched);
    }

    #[test]
    fn do_not_match_shorter_topics() {
        let key = "foo/bar";
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

    #[test]
    fn test_topic_not_valid_multiple_hashes() {
        let valid = topic_valid("foo/#/#/bar");
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

    // handler error tests

    #[derive(Debug)]
    pub struct ErroringHandler {}

    impl ErroringHandler {
        pub fn new() -> Box<Self> {
            Box::new(Self {})
        }
    }

    #[async_trait]
    impl RouteHandler for ErroringHandler {
        async fn call(&mut self, _topic: &str, _content: &[u8]) -> Result<(), anyhow::Error> {
            Err(anyhow::anyhow!("testing error"))
        }
    }

    #[tokio::test]
    async fn test_error_collection() {
        let mut router = Router::default();
        router.add_handler("#", ErroringHandler::new()).unwrap();
        router.add_handler("#", ErroringHandler::new()).unwrap();
        let counter = Arc::new(AtomicU32::new(0));
        router
            .add_handler("#", TestHandler::with_counter(counter.clone()))
            .unwrap();
        let errors = router.handle_message_ignore_errors("anything", &[0]).await;
        // found two errors
        assert!(matches!(errors, Err(a) if a.len() == 2));
        // last handler was called
        assert_eq!(counter.load(Ordering::SeqCst), 1)
    }

    #[tokio::test]
    async fn test_error_stop_handler_execution() {
        let mut router = Router::default();
        router.add_handler("#", ErroringHandler::new()).unwrap();
        let counter = Arc::new(AtomicU32::new(0));
        router
            .add_handler("#", TestHandler::with_counter(counter.clone()))
            .unwrap();
        let res = router.handle_message("anything", &[0]).await;
        // found two errors
        assert!(res.is_err());
        // last handler was called
        assert_eq!(counter.load(Ordering::SeqCst), 0)
    }

    #[tokio::test]
    async fn test_error_returns_ok_with_no_errors() {
        let mut router = Router::default();
        router.add_handler("#", TestHandler::new()).unwrap();
        router.add_handler("#", TestHandler::new()).unwrap();
        let res = router.handle_message_ignore_errors("anything", &[0]).await;
        assert!(matches!(res, Ok(true)));
    }

    #[tokio::test]
    async fn handler_returns_false_if_no_matches_found() {
        let mut router = Router::default();
        router.add_handler("foo", TestHandler::new()).unwrap();
        router.add_handler("bar", TestHandler::new()).unwrap();
        let res = router.handle_message_ignore_errors("anything", &[0]).await;
        assert!(matches!(res, Ok(false)));
    }

    #[tokio::test]
    async fn regular_handler_detects_invalid_topics() {
        let mut router = Router::default();
        let error = router.handle_message("#", &[0]).await;
        assert!(matches!(
            error,
            Err(RouterError::TryingToHandleTopicWithWildcards)
        ));
    }

    #[tokio::test]
    async fn error_ignoring_handler_detects_invalid_topics() {
        let mut router = Router::default();
        let error = router.handle_message_ignore_errors("#", &[0]).await;
        assert!(
            matches!(error, Err(list) if list.len() == 1 && matches!(list[0], RouterError::TryingToHandleTopicWithWildcards))
        );
    }

    #[tokio::test]
    async fn router_takes_async_closures() {
        let mut router = Router::default();
        router
            .add_closure_handler("foo", |_, _| async { Ok(()) })
            .unwrap();
        let res = router.handle_message_ignore_errors("foo", &[0]).await;
        assert!(matches!(res, Ok(true)));
    }
}
