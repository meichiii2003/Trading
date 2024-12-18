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
        Stock::new("NVDA", 100.00),
        Stock::new("META", 100.00),
        Stock::new("ORCL", 100.00),
        Stock::new("IBM", 100.00),
        Stock::new("AMD", 100.00),
        Stock::new("ADM", 100.00),
        Stock::new("BG", 100.00),
        Stock::new("FMC", 100.00),
        Stock::new("CTVA", 100.00),
        Stock::new("DE", 100.00),
        Stock::new("MOS", 100.00),
        Stock::new("AGCO", 100.00),
        Stock::new("CF", 100.00),
        Stock::new("CALM", 100.00),
        Stock::new("SMG", 100.00),
        Stock::new("XOM", 100.00),
        Stock::new("CVX", 100.00),
        Stock::new("BP", 100.00),
        Stock::new("COP", 100.00),
        Stock::new("TOT", 100.00),
        Stock::new("HAL", 100.00),
        Stock::new("SLB", 100.00),
        Stock::new("PSX", 100.00),
        Stock::new("VLO", 100.00),
        Stock::new("OXY", 100.00),
        Stock::new("JNJ", 100.00),
        Stock::new("PFE", 100.00),
        Stock::new("MRK", 100.00),
        Stock::new("UNH", 100.00),
        Stock::new("ABBV", 100.00),
        Stock::new("AMGN", 100.00),
        Stock::new("TMO", 100.00),
        Stock::new("BMY", 100.00),
        Stock::new("GILD", 100.00),
        Stock::new("BIIB", 100.00),
        Stock::new("JPM", 100.00),
        Stock::new("BAC", 100.00),
        Stock::new("WFC", 100.00),
        Stock::new("GS", 100.00),
        Stock::new("MS", 100.00),
        Stock::new("C", 100.00),
        Stock::new("USB", 100.00),
        Stock::new("BK", 100.00),
        Stock::new("TFC", 100.00),
        Stock::new("AXP", 100.00),
        Stock::new("PG", 100.00),
        Stock::new("KO", 100.00),
        Stock::new("PEP", 100.00),
        Stock::new("UL", 100.00),
        Stock::new("NKE", 100.00),
        Stock::new("COST", 100.00),
        Stock::new("MCD", 100.00),
        Stock::new("WMT", 100.00),
        Stock::new("SBUX", 100.00),
        Stock::new("HD", 100.00),
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

    //println!("Starting stock price updates for 50 seconds...");
    

    // Timeout after 30 seconds
    let result = timeout(Duration::from_secs(50), async {
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
            sleep(Duration::from_secs(1)).await;
        }
    })
    .await;

    if result.is_err() {
        println!("Stopping stock price updates.");
    }
}
