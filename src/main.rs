
// main.rs

mod stock_updater;

mod consumer;
mod broker;
mod models;
mod performance;
mod order_consumer;
mod order_processor;

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
pub use broker::initialize_brokers; 

use crate::broker::data::reset_client_holdings_json;
use rdkafka::producer::FutureProducer;
use rdkafka::ClientConfig;

#[tokio::main]
async fn main() {
    println!("MARKET OPEN");
    // 1. Global order counter for unique order IDs across all brokers
    let global_order_counter = Arc::new(AtomicU64::new(1)); // Start from Order 1
    
    // 2. Broadcast channel for stock price updates
    let buffer_size = 1000; // Buffer size for the broadcast channel
    let (price_tx, _) = tokio::sync::broadcast::channel(buffer_size); // Price update broadcast channel

    // 3. Number of brokers
    let total_brokers = 5; // Total number of brokers
    
    // 4. Initialize brokers
    // Initialize brokers using the helper function
    let brokers = initialize_brokers(total_brokers, price_tx.clone(), global_order_counter.clone());

    // Start all brokers
    let mut broker_handles = Vec::new();
    for broker in &brokers {
        let producer = create_producer();
        let broker_clone = broker.clone();
        let handle = tokio::spawn(async move {
            let mut broker = broker_clone.lock().await;
            broker.start(producer).await;
        });

        broker_handles.push(handle);
    }

    // 5. Reset client holdings and broker records
    let json_file_path = "src/data/client_holdings.json"; // Path to the JSON file
    let clients_per_broker = 3;
    // Reset all client portfolios to empty
    reset_client_holdings_json(json_file_path, total_brokers, clients_per_broker);

    // 6. Create the Kafka producer
    // Start the order processor
    fn create_producer() -> FutureProducer {
        ClientConfig::new()
            .set("bootstrap.servers", "localhost:9092")
            .set("message.timeout.ms", "5000")
            .create()
            .expect("Producer creation error")
    }
    let stop_signal = Arc::new(AtomicBool::new(false));
    
    // 7. Start the Kafka consumer (receive stock prices)
    let consumer_handle = tokio::spawn(async move {
        consumer::run_consumer(price_tx.clone()).await;
    });

    // 8. Start the order processor (receive completed and rejected orders)
    let processor_handle = tokio::spawn(async move {
        order_processor::start_order_processor().await;
    });

    //consumer_handle.await.unwrap();//过后用这个 不要order handle
    // 9. Start the order consumer (yikai side mix match)
    let order_consumer_handle = tokio::spawn(async move {
        order_consumer::start_order_consumer().await;
    });

    // 10. Start stock price updater (yikai side, generate random stock prices)
    let stock_updater_handle = tokio::spawn(async move {
        stock_updater::start_price_updater().await;
    });
    stop_signal.store(true, Ordering::SeqCst);  

    // 11. Wait for all broker tasks to complete
    let _ = tokio::join!(stock_updater_handle, processor_handle, consumer_handle, order_consumer_handle);
    //broker.stop();
    
    // 12. Generate client performance report
    println!("MARKET CLOSED");
    let json_file_path = "src/data/client_holdings.json";
    performance::generate_client_report(json_file_path);
}