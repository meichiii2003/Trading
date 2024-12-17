use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::Message;
use serde_json::Value;
use std::fs::File;
use std::io::{Read, Write};
use std::sync::Arc;
use tokio::sync::Mutex;
use crate::broker::update_client_portfolio_in_json;
use crate::broker::broker::Broker;
use crate::broker::client;
use crate::models::{Order, OrderAction, OrderStatus};
use std::collections::{HashMap, HashSet};
use colored::*;
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
        async move {
            let json_file_path = "src/data/client_holdings.json";
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
    tokio::spawn({
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
}


// pub async fn start_order_processor(
//     broker: Vec<Arc<Mutex<Broker>>>,
//     market_prices: Arc<Mutex<HashMap<String, f64>>>,
// ) {
//     let brokers = "localhost:9092";
//     let completed_topic = "completed_order";
//     let rejected_topic = "rejected_order";

//     let completed_consumer: StreamConsumer = ClientConfig::new()
//         .set("bootstrap.servers", brokers)
//         .set("group.id", "completed-order-processor-group")
//         .set("enable.auto.commit", "false")
//         .set("fetch.min.bytes", "1")
//         .set("fetch.wait.max.ms", "1")
//         .create()
//         .expect("Failed to create Kafka consumer for completed orders");

//     let rejected_consumer: StreamConsumer = ClientConfig::new()
//         .set("bootstrap.servers", brokers)
//         .set("group.id", "rejected-order-processor-group")
//         .set("enable.auto.commit", "false")
//         .set("fetch.min.bytes", "1")
//         .set("fetch.wait.max.ms", "1")
//         .create()
//         .expect("Failed to create Kafka consumer for rejected orders");

//     completed_consumer
//         .subscribe(&[completed_topic])
//         .expect("Failed to subscribe to completed topic");
//     rejected_consumer
//         .subscribe(&[rejected_topic])
//         .expect("Failed to subscribe to rejected topic");

//     println!("Order processor started, waiting for messages...");

//     // Process completed orders
//     tokio::spawn({
//         let broker = broker.clone();
//         async move {
//             let json_file_path = "src/data/client_holdings.json";
//             loop {
//                 match completed_consumer.recv().await {
//                     Ok(m) => {
//                         if let Some(payload) = m.payload() {
//                             let message = String::from_utf8_lossy(payload);

//                             if let Ok(order) = serde_json::from_str::<Order>(&message) {
//                                 match order.order_action {
//                                     OrderAction::Buy => {
//                                         println!(
//                                             "{}",
//                                             format!("Completed Buy Order Execute: {:?}", order)
//                                                 .bright_cyan().bold()
//                                         );
//                                         // Update portfolio for Buy
//                                         update_client_portfolio_in_json(
//                                             json_file_path,
//                                             order.client_id,
//                                             order.stock_symbol.clone(),
//                                             order.quantity,
//                                             true, // Buy flag
//                                             order.price,
//                                         )
//                                         .await;
//                                     }
//                                     OrderAction::Sell => {
//                                         println!(
//                                             "{}",
//                                             format!("Completed Sell Order Execute: {:?}", order)
//                                                 .bright_magenta().bold()
//                                         );
//                                         // Update portfolio for Sell
//                                         process_sell_order(
//                                             json_file_path,
//                                             order.client_id,
//                                             order.stock_symbol.clone(),
//                                             order.quantity,
//                                         )
//                                         .await;
//                                     }
//                                     _ => {
//                                         println!(
//                                             "Invalid order action for order ID {}: {:?}",
//                                             order.order_id, order.order_action
//                                         );
//                                     }
//                                 }
//                             } else {
//                                 println!("Failed to parse order message: {}", message);
//                             }
//                         }

//                         completed_consumer
//                             .commit_message(&m, CommitMode::Async)
//                             .unwrap();
//                     }
//                     Err(err) => {
//                         println!("Kafka error on completed topic: {}", err);
//                     }
//                 }
//             }
//         }
//     });

//     // Process rejected orders
//     tokio::spawn({
//         let mut processed_orders = HashSet::new(); // Track processed orders
//         async move {
//             loop {
//                 match rejected_consumer.recv().await {
//                     Ok(m) => {
//                         if let Some(payload) = m.payload() {
//                             let message = String::from_utf8_lossy(payload);
//                             if let Ok(order) = serde_json::from_str::<Order>(&message) {
//                                 // Skip if already processed
//                                 if !processed_orders.insert(order.order_id.clone()) {
//                                     continue;
//                                 }

//                                 println!(
//                                     "{}",
//                                     format!("Order Rejected: {:?}", order).bright_black().bold()
//                                 );
//                             }
//                         }
//                         rejected_consumer
//                             .commit_message(&m, CommitMode::Async)
//                             .unwrap();
//                     }
//                     Err(err) => {
//                         println!("Kafka error on rejected topic: {}", err);
//                     }
//                 }
//             }
//         }
//     });
// }

/// Function to process sell orders by updating the client's portfolio in the JSON file
async fn process_sell_order(
    json_file_path: &str,
    client_id: u64,
    stock_symbol: String,
    quantity: u64,
) {
    // Read the JSON file
    let mut file = match File::open(json_file_path) {
        Ok(f) => f,
        Err(e) => {
            println!("Error opening JSON file: {}", e);
            return;
        }
    };

    let mut json_data = String::new();
    if let Err(e) = file.read_to_string(&mut json_data) {
        println!("Error reading JSON file: {}", e);
        return;
    }

    let mut data: Value = match serde_json::from_str(&json_data) {
        Ok(d) => d,
        Err(e) => {
            println!("Error parsing JSON data: {}", e);
            return;
        }
    };

    // Locate the client
    let brokers = data["brokers"].as_array_mut();
    if brokers.is_none() {
        println!("Error: Brokers array not found in JSON data.");
        return;
    }

    let mut updated = false;
    for broker in brokers.unwrap() {
        let clients = broker["clients"].as_array_mut();
        if clients.is_none() {
            continue;
        }

        for client in clients.unwrap() {
            if client["client_id"] == client_id {
                let portfolio = client["portfolio"].as_object_mut();
                if portfolio.is_none() {
                    println!("Error: Portfolio not found for client ID {}", client_id);
                    return;
                }

                let portfolio = portfolio.unwrap();
                if let Some(stock) = portfolio.get_mut(&stock_symbol) {
                    let current_quantity = stock["quantity"].as_u64().unwrap_or(0);
                    if current_quantity >= quantity {
                        // Deduct the quantity or remove the stock entry if quantity is zero
                        let new_quantity = current_quantity - quantity;
                        if new_quantity == 0 {
                            portfolio.remove(&stock_symbol);
                        } else {
                            stock["quantity"] = serde_json::json!(new_quantity);
                        }
                        
                        updated = true;
                        break;
                    } else {
                        println!(
                            "Error: Insufficient quantity of {} for client ID {} to sell.",
                            stock_symbol, client_id
                        );
                        return;
                    }
                } else {
                    println!(
                        "Error: Client ID {} does not hold {} in their portfolio.",
                        client_id, stock_symbol
                    );
                    return;
                }
            }
        }
        if updated {
            break;
        }
    }

    if !updated {
        println!("Error: Client ID {} not found in JSON data.", client_id);
        return;
    }

    // Write the updated JSON back to the file
    let updated_json = match serde_json::to_string_pretty(&data) {
        Ok(json) => json,
        Err(e) => {
            println!("Error serializing updated JSON data: {}", e);
            return;
        }
    };

    if let Err(e) = File::create(json_file_path).and_then(|mut f| f.write_all(updated_json.as_bytes())) {
        println!("Error writing updated JSON data: {}", e);
    } 
    // else {
    //     println!(
    //         "Successfully processed sell order for client ID {} and updated JSON.",
    //         client_id
    //     );
    // }
}
