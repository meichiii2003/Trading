use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::Message;
use tokio::time::timeout;
use std::time::Duration;
use crate::broker::update_client_portfolio_in_json;
use crate::models::{Order, OrderAction};
use std::collections:: HashSet;
use colored::*;
//trading side

pub async fn start_order_processor() {
    let brokers = "localhost:9092";
    let completed_topic = "completed_order";
    let rejected_topic = "rejected_order";

    let completed_consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", "completed-order-processor-group")
        .set("enable.auto.commit", "false")
        .set("fetch.min.bytes", "1")
        .set("fetch.wait.max.ms", "1")
        .create()
        .expect("Failed to create Kafka consumer for completed orders");

    let rejected_consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", "rejected-order-processor-group")
        .set("enable.auto.commit", "false")
        .set("fetch.min.bytes", "1")
        .set("fetch.wait.max.ms", "1")
        .create()
        .expect("Failed to create Kafka consumer for rejected orders");

    completed_consumer
        .subscribe(&[completed_topic])
        .expect("Failed to subscribe to completed topic");
    rejected_consumer
        .subscribe(&[rejected_topic])
        .expect("Failed to subscribe to rejected topic");

    //println!("Order processor started, waiting for messages...");

    // Process completed orders
    let processing_result = timeout(Duration::from_secs(50), async {
        let completed_task = tokio::spawn({
            let json_file_path = "src/data/client_holdings.json";
            async move {
            loop {
                match completed_consumer.recv().await {
                    Ok(m) => {
                        if let Some(payload) = m.payload() {
                            let message = String::from_utf8_lossy(payload);

                            if let Ok(order) = serde_json::from_str::<Order>(&message) {
                                match order.order_action {
                                    OrderAction::Buy => {
                                        println!(
                                            "{}",
                                            format!("Completed Buy Order Execute: {:?}", order)
                                                .bright_cyan()
                                                .bold()
                                        );
                                    }
                                    OrderAction::Sell => {
                                        println!(
                                            "{}",
                                            format!("Completed Sell Order Execute: {:?}", order)
                                                .bright_magenta()
                                                .bold()
                                        );
                                    }
                                    _ => {
                                        println!(
                                            "Invalid order action for order ID {}: {:?}",
                                            order.order_id, order.order_action
                                        );
                                        continue;
                                    }
                                }

                                // Unified portfolio update
                                update_client_portfolio_in_json(
                                    json_file_path,
                                    order.client_id,
                                    order.stock_symbol.clone(),
                                    order.quantity,
                                    matches!(order.order_action, OrderAction::Buy), // Pass `true` for Buy, `false` for Sell
                                    order.price,
                                )
                                .await;
                            } else {
                                println!("Failed to parse order message: {}", message);
                            }
                        }

                        completed_consumer
                            .commit_message(&m, CommitMode::Async)
                            .unwrap();
                    }
                    Err(err) => {
                        println!("Kafka error on completed topic: {}", err);
                    }
                }
            }
        }
    });

    // Process rejected orders
    let rejected_task =tokio::spawn({
        let mut processed_orders = HashSet::new(); // Track processed orders
        async move {
            loop {
                match rejected_consumer.recv().await {
                    Ok(m) => {
                        if let Some(payload) = m.payload() {
                            let message = String::from_utf8_lossy(payload);
                            if let Ok(order) = serde_json::from_str::<Order>(&message) {
                                // Skip if already processed
                                if !processed_orders.insert(order.order_id.clone()) {
                                    continue;
                                }

                                println!(
                                    "{}",
                                    format!("Order Rejected: {:?}", order).bright_black().bold()
                                );
                            }
                        }
                        rejected_consumer
                            .commit_message(&m, CommitMode::Async)
                            .unwrap();
                    }
                    Err(err) => {
                        println!("Kafka error on rejected topic: {}", err);
                    }
                }
            }
        }
    });
    let _ = tokio::join!(completed_task, rejected_task);
}).await;
}