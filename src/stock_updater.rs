use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::Serialize;
use rand::{seq::IteratorRandom, Rng};
use rand::rngs::StdRng;
use rand::SeedableRng;
use tokio::time::{sleep, timeout, Duration};

#[derive(Serialize, Debug)]
struct PriceUpdate {
    name: String,
    price: f64,
}

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

pub async fn start_price_updater() {
    let topic = "stock";
    // Kafka producer
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .create()
        .expect("Failed to create Kafka producer");

    let mut rng = StdRng::from_entropy(); // Use StdRng for thread-safe RNG

    // Initialize stock data
    let mut stock_data = vec![
        Stock::new("APPL", 100.00),
        Stock::new("MSFT", 100.00),
        Stock::new("GOOG", 100.00),
        Stock::new("AMZN", 100.00),
        Stock::new("TSLA", 100.00),
    ];

    // Send initial stock prices
    for stock in &stock_data {
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
                Duration::from_secs(0),
            )
            .await
            .expect("Failed to send price update");
    }

    println!("Starting stock price updates for 30 seconds...");

    // Timeout after 30 seconds
    let result = timeout(Duration::from_secs(10), async {
        loop {
            let num_updates = rng.gen_range(2..=4);

            for stock in stock_data.iter_mut().choose_multiple(&mut rng, num_updates) {
                let change = rng.gen_range(-10.0..10.0);
                stock.price = (stock.price + change).max(1.0); // Avoid negative prices

                let payload = serde_json::to_string(&PriceUpdate {
                    name: stock.name.clone(),
                    price: stock.price,
                })
                .expect("Failed to serialize price update");

                // Send updated price to Kafka
                producer
                    .send(
                        FutureRecord::to(topic)
                            .key(&stock.name)
                            .payload(&payload),
                        Duration::from_secs(0),
                    )
                    .await
                    .expect("Failed to send price update");
            }

            // Sleep between updates
            sleep(Duration::from_secs(2)).await;
        }
    })
    .await;

    if result.is_err() {
        println!("30 seconds reached: Stopping stock price updates.");
    }
}
