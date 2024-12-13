use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::Message;
use std::sync::Arc;
use tokio::sync::Mutex;
use crate::broker::update_client_portfolio_in_json;
use crate::broker::broker::Broker;
use crate::broker::client;
use crate::models::{Order, OrderAction, OrderStatus};
use std::collections::HashMap;
//trading side
use tokio::task;
use tokio::sync::mpsc;
use futures::stream::StreamExt;

pub async fn start_order_processor(
    broker: Vec<Arc<Mutex<Broker>>>,
    market_prices: Arc<Mutex<HashMap<String, f64>>>,
) {
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

    println!("Order processor started, waiting for messages...");

    // Process completed orders
    tokio::spawn({
        let broker = broker.clone();
        async move {
            loop {
                match completed_consumer.recv().await {
                    Ok(m) => {
                        if let Some(payload) = m.payload() {
                            let message = String::from_utf8_lossy(payload);
        
                            if let Ok(order) = serde_json::from_str::<Order>(&message) {
                                println!("Completed Order Execute: {:?}", order);
                                let json_file_path = "src/data/client_holdings.json"; 
                                // Determine whether the order is a buy or sell
                                let is_buy = match order.order_action {
                                    OrderAction::Buy => true,
                                    OrderAction::Sell => false,
                                    _ => {
                                        println!(
                                            "Invalid order action for order ID {}: {:?}",
                                            order.order_id, order.order_action
                                        );
                                        continue;
                                    }
                                };

                                // Update the JSON
                                update_client_portfolio_in_json(
                                    json_file_path,
                                    order.client_id,
                                    order.stock_symbol.clone(),
                                    order.quantity,
                                    is_buy, // Pass buy/sell flag
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
    tokio::spawn({
        let brokers = brokers.clone();
        async move {
            loop {
                match rejected_consumer.recv().await {
                    Ok(m) => {
                        if let Some(payload) = m.payload() {
                            let message = String::from_utf8_lossy(payload);
                            if let Ok(order) = serde_json::from_str::<Order>(&message) {
                                for broker in &broker {
                                    let mut broker = broker.lock().await;
                                    broker.process_rejected_orders(vec![order.clone()]).await;
                                }
                                println!("Processed rejected order: {}", order.order_id);
                            }
                        }
                        rejected_consumer.commit_message(&m, CommitMode::Async).unwrap();
                    }
                    Err(err) => {
                        println!("Kafka error on rejected topic: {}", err);
                    }
                }
            }
        }
    });
}
