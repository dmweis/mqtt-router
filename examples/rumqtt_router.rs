use anyhow::Context;
use mqtt_router::{async_trait, RouteHandler, Router, RouterError};
use rumqttc::{AsyncClient, ConnAck, Event, Incoming, MqttOptions, Publish, QoS, SubscribeFilter};
use std::time::Duration;
use tokio::sync::mpsc::unbounded_channel;

pub struct ExampleHandler {}

impl ExampleHandler {
    pub fn new() -> Box<Self> {
        Box::new(Self {})
    }
}

#[async_trait]
impl RouteHandler for ExampleHandler {
    async fn call(&mut self, topic: &str, content: &[u8]) -> Result<(), RouterError> {
        let text = std::str::from_utf8(content).context("Wrapping an anyhow error")?;
        println!("{topic} -> {text:?}");
        Ok(())
    }
}

enum MqttUpdate {
    Message(Publish),
    Reconnection(ConnAck),
}

#[tokio::main]
async fn main() {
    let mut mqttoptions = MqttOptions::new("router_test_instance", "homepi.local", 1883);
    mqttoptions.set_keep_alive(Duration::from_secs(5));
    mqttoptions.set_pending_throttle(Duration::from_millis(1));
    // mqttoptions.set_connection_timeout(0);

    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);

    let (message_sender, mut message_receiver) = unbounded_channel();

    tokio::spawn(async move {
        loop {
            match eventloop.poll().await {
                Ok(notification) => match notification {
                    Event::Incoming(Incoming::Publish(publish)) => {
                        if let Err(e) = message_sender.send(MqttUpdate::Message(publish)) {
                            eprintln!("Error sending message {}", e);
                        }
                    }
                    Event::Incoming(Incoming::ConnAck(con_ack)) => {
                        if let Err(e) = message_sender.send(MqttUpdate::Reconnection(con_ack)) {
                            eprintln!("Error sending message {}", e);
                        }
                    }
                    _ => (),
                },
                Err(e) => {
                    eprintln!("Error processing eventloop notifications {}", e);
                }
            }
        }
    });

    let mut router = Router::default();
    router
        .add_handler("test_topic/#", ExampleHandler::new())
        .unwrap();
    let topics = router
        .topics_for_subscription()
        .map(|topic| SubscribeFilter {
            path: topic.to_owned(),
            qos: QoS::AtMostOnce,
        });
    client.subscribe_many(topics).await.unwrap();

    loop {
        let update = message_receiver.recv().await.unwrap();
        match update {
            MqttUpdate::Message(message) => {
                let found_handler = router
                    .handle_message_ignore_errors(&message.topic, &message.payload)
                    .await
                    .unwrap();
                println!("Found handler {found_handler}");
            }
            MqttUpdate::Reconnection(_) => {
                let topics = router
                    .topics_for_subscription()
                    .map(|topic| SubscribeFilter {
                        path: topic.to_owned(),
                        qos: QoS::AtMostOnce,
                    });
                client.subscribe_many(topics).await.unwrap();
            }
        }
    }
}
