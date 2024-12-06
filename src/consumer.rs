use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::Message;
use serde::{Deserialize, Serialize};
use serde_json;
use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use tokio_stream::StreamExt;
use crate::models::PriceUpdate;

// #[derive(Serialize, Deserialize, Debug, Clone)]
// pub struct PriceUpdate {
//     pub name: String,
//     pub price: f64,
// }

const JSON_FILE_PATH: &str = "src/data/price_store.json";

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

    println!("Consumer started, waiting for messages...");

    let mut message_stream = consumer.stream();

    // Ensure the JSON file exists
    //initialize_json_storage().await;

    // Channel to broadcast price updates
    //let (price_tx, mut price_rx) = tokio::sync::mpsc::channel(100);

    // Continuously consume messages
    while let Some(message) = message_stream.next().await {
        match message {
            Ok(m) => {
                if let Some(payload) = m.payload() {
                    println!("Raw Payload: {}", String::from_utf8_lossy(payload));

                    // Deserialize the payload into a Vec<PriceUpdate>
                    match serde_json::from_slice::<PriceUpdate>(payload) {
                        Ok(price_update) => {
                            //println!("Received Price Update: Stock: {}, Price: {:.2}", price_update.name, price_update.price);

                            // Broadcast to brokers
                            if let Err(e) = price_tx.send(price_update.clone()) {
                                eprintln!("Failed to broadcast price update: {:?}", e);
                            }
                            // Update the JSON file with the new price
                            update_json_file(&price_update).await;
                        }
                        Err(e) => {
                            eprintln!("Error deserializing price update: {:?}", e);
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

// Update the JSON file with new prices
async fn update_json_file(price_update: &PriceUpdate) {
    // Read the existing data from the file
    let mut existing_data: HashMap<String, f64> = match fs::read_to_string(JSON_FILE_PATH) {
        Ok(content) => serde_json::from_str(&content).unwrap_or_else(|_| HashMap::new()),
        Err(_) => {
            eprintln!("Failed to read JSON file, starting with an empty HashMap.");
            HashMap::new()
        },
    };

    // Update the price for the given stock
    existing_data.insert(price_update.name.clone(), price_update.price);

    // Write the updated data back to the file
    let json_data = serde_json::to_string_pretty(&existing_data).expect("Failed to serialize prices");
    fs::write(JSON_FILE_PATH, json_data).expect("Failed to write updated JSON data");
}
