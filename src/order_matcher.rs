use std::time::Duration;

use rand::rngs::StdRng;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use tokio::time::timeout;
use crate::models::{Order, OrderStatus};
use rand::{Rng, SeedableRng};

//yikai side
pub async fn consume_and_route_orders() {
    let brokers = "localhost:9092"; // Kafka brokers
    let group_id = "order-consumer-group"; // Consumer group ID
    let topics = vec!["orders"]; // Kafka topic to consume from
    // Topics for processed orders
    let completed_topic = "completed_order";
    let rejected_topic = "rejected_order";

    // Create Kafka consumer
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", group_id)
        //.set("enable.partition.eof", "false")
        //.set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "false")
        .create()
        .expect("Failed to create Kafka consumer");

    // Create Kafka producer
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .expect("Failed to create Kafka producer");

    // Subscribe to the topic
    consumer.subscribe(&topics).expect("Failed to subscribe to topics");

    //println!("Order consumer started, waiting for messages...");

    let mut rng = StdRng::from_entropy();

    let result = timeout(Duration::from_secs(50), async {
    loop {
        match consumer.recv().await {
            Ok(m) => {
                if let Some(payload) = m.payload() {
                    let message = String::from_utf8_lossy(payload);
                    //println!("Received order message: {}", message);

                    // Deserialize the JSON payload into Order
                    match serde_json::from_str::<Order>(&message) {
                        Ok(mut order) => {
                            // println!("Processing order: {:?}", order);

                            let outcome: f64 = rng.gen_range(0.0..1.0);
                            let target_topic = if outcome <= 0.4 {
                                // 40% chance: Completed
                                order.status = OrderStatus::Completed;
                                // Adjust price by 2% for completed orders
                                order.price = (order.price * 1.02 * 100.0).round() / 100.0;
                                completed_topic
                            } else if outcome <= 0.6 {
                                // Next 20% chance: Rejected
                                order.status = OrderStatus::Rejected;
                                rejected_topic
                            } else {
                                // Remaining 40%: Keep as Pending
                                continue; // Skip further processing for pending orders
                            };

                            //println!("Updated order: {:?}", order);

                            // Send the updated order back to the appropriate Kafka topic
                            if let Err(err) = send_updated_order_to_kafka(&producer, &order, target_topic).await {
                                println!("Failed to send updated order to Kafka: {}", err);
                            }
                        }
                        Err(err) => {
                            println!("Failed to deserialize order: {}", err);
                        }
                    }
                }

                // Commit the message offset after processing
                consumer.commit_message(&m, CommitMode::Async).unwrap();
            }
            Err(err) => {
                println!("Kafka error: {}", err);
            }
        }
    }
}).await;
    // Handle timeout
    if result.is_err() {
        println!("Stopping order consumer.");
    }
}

// Send Updated Orders Back to Kafka
pub async fn send_updated_order_to_kafka(
    producer: &FutureProducer,
    order: &Order,
    topic: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let payload = serde_json::to_string(order)?;

    producer
        .send(
            FutureRecord::to(topic)
                .key(&order.order_id)
                .payload(&payload),
            rdkafka::util::Timeout::Never,
        )
        .await
        .map_err(|(err, _)| err)?;
    Ok(())
}