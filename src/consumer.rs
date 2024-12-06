// use rdkafka::config::ClientConfig;
// use rdkafka::consumer::{Consumer, StreamConsumer};
// use rdkafka::Message;
// use serde::{Deserialize};
// use tokio;
// use futures::StreamExt;

// #[derive(Deserialize, Debug)]
// struct PriceUpdate {
//     symbol: String,
//     price: f64,
//     timestamp: i64,
// }

// pub async fn run() {
//     // Initialize the Kafka consumer
//     let consumer: StreamConsumer = ClientConfig::new()
//         .set("group.id", "stock-price-consumer-group")
//         .set("bootstrap.servers", "localhost:9092") // Connect to Kafka broker
//         .set("auto.offset.reset", "latest") // Start reading from the earliest message
//         .set("enable.auto.commit", "true") // Enable auto commit
//         .set("debug", "all") // Enable debug logs
//         .create()
//         .expect("Consumer creation failed");

//     // Subscribe to the 'stock_prices' topic
//     consumer.subscribe(&["stock"]).expect("Can't subscribe to specified topic");

//     println!("Consumer started, waiting for messages...");

//     let mut stream_consumer = consumer.stream(); // Get the consumer stream

//     // Continuously consume messages
//     while let Some(message) = stream_consumer.next().await {
//         match message {
//             Ok(m) => {
//                 if let Some(payload) = m.payload() {
//                     println!("Raw message payload: {}", String::from_utf8_lossy(payload));

//                     // Deserialize the payload to the PriceUpdate struct
//                     match serde_json::from_slice::<PriceUpdate>(payload) {
//                         Ok(price_update) => {
//                             println!("Received price update: {:?}", price_update);
//                         },
//                         Err(e) => {
//                             eprintln!("Error deserializing message: {:?}", e);
//                         }
//                     }
//                 }
//             },
//             Err(e) => {
//                 eprintln!("Error while consuming from stream: {:?}", e);
//             }
//         }
//     }
// }


// consumer.rs
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::Message;
use serde::Deserialize;
use tokio_stream::StreamExt;

use crate::models::PriceUpdate;

// #[derive(Deserialize, Debug)]
// struct PriceUpdate {
//     name: String,
//     price: f64,
// }

pub async fn run_consumer(price_tx: tokio::sync::broadcast::Sender<PriceUpdate>) {
    // Initialize the Kafka consumer
    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", "stock-price-consumer-group")
        .set("bootstrap.servers", "localhost:9092")
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "true")
        .create()
        .expect("Consumer creation failed");

    // Subscribe to the 'stock' topic
    consumer.subscribe(&["stock"]).expect("Can't subscribe to specified topic");

    println!("Consumer started, waiting for batch messages...");

    let mut message_stream = consumer.stream();

    // Continuously consume messages
    while let Some(message) = message_stream.next().await {
        match message {
            Ok(m) => {
                if let Some(payload) = m.payload() {
                    // Display raw payload
                    println!("Raw Batch Payload: {}", String::from_utf8_lossy(payload));

                    // Deserialize the payload to a Vec<PriceUpdate>
                    match serde_json::from_slice::<Vec<PriceUpdate>>(payload) {
                        Ok(batch) => {
                            // let receiver_count = batch_signal_tx.receiver_count();
                            // println!("Broadcasting batch signal to {} receivers.", receiver_count);

                            // if receiver_count > 0 {
                            //     if let Err(e) = batch_signal_tx.send(()) {
                            //         eprintln!("Failed to broadcast batch signal: {:?}", e);
                            //     } else {
                            //         println!("Broadcasting batch signal to all brokers.");
                            //     }
                            // } else {
                            //     eprintln!("No subscribers available for batch signal.");
                            // }

                            println!("Received Batch of Price Updates:");
                            for price_update in batch {
                                println!("  - Stock: {}, Price: {:.2}", price_update.name, price_update.price);

                                // Broadcast to brokers
                                if let Err(e) = price_tx.send(price_update.clone()) {
                                    eprintln!("Failed to broadcast price update: {:?}", e);
                                }
                                // } else {
                                //     println!("Sent price update to brokers: {:?}", price_update);
                                // }
                            }
                        }
                        Err(e) => {
                            eprintln!("Error deserializing batch message: {:?}", e);
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!("Error while consuming from stream: {:?}", e);
            }
        }
    }
}
