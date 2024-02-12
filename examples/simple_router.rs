use mqtt_router::{async_trait, RouteHandler, Router};

pub struct ExampleHandler {}

impl ExampleHandler {
    pub fn new() -> Box<Self> {
        Box::new(Self {})
    }
}

#[async_trait]
impl RouteHandler for ExampleHandler {
    async fn call(&mut self, topic: &str, _content: &[u8]) -> Result<(), anyhow::Error> {
        println!("Handling mqtt message for {topic}");
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    let mut router = Router::default();
    router
        .add_handler("home/test", ExampleHandler::new())
        .unwrap();

    for topic in router.topics_for_subscription() {
        println!("{topic}");
    }

    router.handle_message("home/test", &[0]).await.unwrap();
}
