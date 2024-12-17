use std::collections::HashMap;
use rand::seq::{IteratorRandom, SliceRandom};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::{Serialize};
use tokio;
use chrono;
use rand::{random, Rng};

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
#[derive(Debug, Clone)]
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

    // Store the latest prices
    let mut stored_prices: HashMap<String, f64> = HashMap::new();

    // Send initial prices one by one
    for stock in &stock_data {
        stored_prices.insert(stock.name.clone(), stock.price);

        let payload = serde_json::to_string(&PriceUpdate {
            name: stock.name.clone(),
            price: stock.price,
        })
        .expect("Failed to serialize price update");

        producer
            .send(
                FutureRecord::to(topic)
                    .key(&stock.name)
                    .payload(&payload),
                rdkafka::util::Timeout::Never,
            )
            .await
            .expect("Failed to send initial price");

        println!("Sent initial price for {}: {}", stock.name, stock.price);
    }

    // Add separator after sending initial prices
    println!("---------------------");

    // Wait for 5 seconds before sending updated prices
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    // Continuously send only changed prices
    loop {
        let mut updates_made = false;

        // Select 2-4 random stocks to update
        let mut rng = rand::thread_rng();
        let num_changes = rng.gen_range(2..=4);
        let mut stocks_to_update: Vec<&mut Stock> = stock_data.iter_mut().choose_multiple(&mut rng, num_changes).into_iter().collect();

        for stock in &mut stocks_to_update {
            // Update price
            let new_price = update_price(stock.price);

            // Check if the price has changed
            if (new_price - stock.price).abs() > f64::EPSILON {
                updates_made = true;

                // Update the stored price
                stored_prices.insert(stock.name.clone(), new_price);

                // Reflect the change in the stock data
                stock.price = new_price;

                // Serialize and send the update individually
                let payload = serde_json::to_string(&PriceUpdate {
                    name: stock.name.clone(),
                    price: new_price,
                })
                .expect("Failed to serialize price update");

                producer
                    .send(
                        FutureRecord::to(topic)
                            .key(&stock.name)
                            .payload(&payload),
                        rdkafka::util::Timeout::Never,
                    )
                    .await
                    .expect("Failed to send price update");

                println!("Sent updated price for {}: {}", stock.name, new_price);
            }
        }

        // If any updates were made, add the separator
        if updates_made {
            println!("---------------------");
        }

        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
}

// Simulate random price update
fn update_price(current_price: f64) -> f64 {
    let mut rng = rand::thread_rng();
    let change = rng.gen_range(-20.0..20.0); // Random change between -1.0 and 1.0
    let updated_price = current_price + change;
    ((updated_price.max(0.0)) * 100.0).round() / 100.0 // Ensure price is >= 0 and round to 2 decimals
}
