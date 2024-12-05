use std::collections::HashMap;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::{Serialize};
use tokio;
use chrono;
use rand::random;

#[derive(Serialize, Debug)]
struct PriceUpdate {
    name: String,
    price: f64,
}

#[derive(Serialize, Debug)]
struct Transaction {
    action: String,  // "buy" or "sell"
    name: String,
    amount: u32,
    price: f64,
    timestamp: i64,
}

// Define the Stock struct
#[derive(Debug)]
struct Stock {
    name: String,
    price: f64,
}

impl Stock {
    fn new(name: &str, price: f64) -> Self {
        Stock {
            name: name.to_string(),
            price,
        }
    }
}

#[tokio::main]
async fn main() {
    // Create the producer to send stock price updates
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092") // Connect to Kafka broker
        .create()
        .expect("Failed to create producer");

    // Kafka topic where stock price updates will be sent
    let topic = "stock";

    // Initialize stock data
    let mut stock_data = vec![
        Stock::new("APPL", 100.00),
        Stock::new("MSFT", 100.00),
        Stock::new("GOOG", 100.00),
        Stock::new("AMZN", 100.00),
        Stock::new("TSLA", 100.00),
        // Stock::new("NVDA", 100.00),
        // Stock::new("META", 100.00),
        // Stock::new("ORCL", 100.00),
        // Stock::new("IBM", 100.00),
        // Stock::new("AMD", 100.00),
        // Stock::new("ADM", 100.00),
        // Stock::new("BG", 100.00),
        // Stock::new("FMC", 100.00),
        // Stock::new("CTVA", 100.00),
        // Stock::new("DE", 100.00),
        // Stock::new("MOS", 100.00),
        // Stock::new("AGCO", 100.00),
        // Stock::new("CF", 100.00),
        // Stock::new("CALM", 100.00),
        // Stock::new("SMG", 100.00),
        // Stock::new("XOM", 100.00),
        // Stock::new("CVX", 100.00),
        // Stock::new("BP", 100.00),
        // Stock::new("COP", 100.00),
        // Stock::new("TOT", 100.00),
        // Stock::new("HAL", 100.00),
        // Stock::new("SLB", 100.00),
        // Stock::new("PSX", 100.00),
        // Stock::new("VLO", 100.00),
        // Stock::new("OXY", 100.00),
        // Stock::new("JNJ", 100.00),
        // Stock::new("PFE", 100.00),
        // Stock::new("MRK", 100.00),
        // Stock::new("UNH", 100.00),
        // Stock::new("ABBV", 100.00),
        // Stock::new("AMGN", 100.00),
        // Stock::new("TMO", 100.00),
        // Stock::new("BMY", 100.00),
        // Stock::new("GILD", 100.00),
        // Stock::new("BIIB", 100.00),
        // Stock::new("JPM", 100.00),
        // Stock::new("BAC", 100.00),
        // Stock::new("WFC", 100.00),
        // Stock::new("GS", 100.00),
        // Stock::new("MS", 100.00),
        // Stock::new("C", 100.00),
        // Stock::new("USB", 100.00),
        // Stock::new("BK", 100.00),
        // Stock::new("TFC", 100.00),
        // Stock::new("AXP", 100.00),
        // Stock::new("PG", 100.00),
        // Stock::new("KO", 100.00),
        // Stock::new("PEP", 100.00),
        // Stock::new("UL", 100.00),
        // Stock::new("NKE", 100.00),
        // Stock::new("COST", 100.00),
        // Stock::new("MCD", 100.00),
        // Stock::new("WMT", 100.00),
        // Stock::new("SBUX", 100.00),
        // Stock::new("HD", 100.00),
    ];

    // Infinite loop to continuously send price updates
    loop {
        let mut updates = Vec::new();

        // Update stock prices
        for stock in &mut stock_data {
            stock.price = update_price(stock.price);
            updates.push(PriceUpdate {
                name: stock.name.clone(),
                price: stock.price,
            });
        }

        // Serialize all updates into a JSON array
        let payload = serde_json::to_string(&updates).expect("Failed to serialize price updates");

        // Send the batch update as one message
        producer
            .send(
                FutureRecord::to(topic)
                    .key("all_stocks")
                    .payload(&payload),
                rdkafka::util::Timeout::Never,
            )
            .await
            .expect("Failed to send price updates");

        println!("Sent batch update to brokers: {:?}", updates);

        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    }
}

// Simulate price update with random fluctuation
fn update_price(current_price: f64) -> f64 {
    let change = rand::random::<f64>() * 2.0 - 1.0; // Random change between -1.0 and 1.0
    let updated_price = current_price + change;
    ((updated_price.max(0.0)) * 100.0).round() / 100.0 // Ensure price is >= 0 and round to 2 decimals
}
