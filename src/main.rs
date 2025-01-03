
// main.rs

mod stock_updater;

mod stock_price_consumer;
mod broker;
mod models;
mod performance;
mod order_matcher;
mod order_status_receiver;

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
            broker.start_broker_task(producer).await;
        });

        broker_handles.push(handle);
    }

    // 5. Reset client holdings and broker records
    let json_file_path = "src/data/client_holdings.json"; // Path to the JSON file
    let clients_per_broker = 3;
    // Reset all client portfolios to empty
    //reset_client_holdings_json(json_file_path, total_brokers, clients_per_broker);

    // 6. Create the Kafka producer
    // Start the order processor, to send order to kafka i think
    fn create_producer() -> FutureProducer {
        ClientConfig::new()
            .set("bootstrap.servers", "localhost:9092")
            .set("message.timeout.ms", "5000")
            .create()
            .expect("Producer creation error")
    }
    
    // 7. Create a stop signal for the broker tasks
    let stop_signal = Arc::new(AtomicBool::new(false));
    
    // 8. Start stock price updater (yikai side, generate stock prices and send to Kafka)
    let stock_updater_handle = tokio::spawn(async move {
        stock_updater::start_price_updater().await;
    });

    // 9. Start the Kafka consumer (i receive stock prices from kafka)
    let stock_price_consumer_handle = tokio::spawn(async move {
        stock_price_consumer::run_consumer(price_tx.clone()).await;
    });

    //consumer_handle.await.unwrap();//过后用这个 不要order handle
    // 10. Start the order consumer (yikai side, reject or complete the orders and send to kafka)
    let order_matcher_handle = tokio::spawn(async move {
        order_matcher::consume_and_route_orders().await;
    });

    // 11. Start the order processor (receive completed and rejected orders)
    let order_status_receiver_handle = tokio::spawn(async move {
        order_status_receiver::order_status_receiver().await;
    });
    
    stop_signal.store(true, Ordering::SeqCst);  

    // 12. Wait for all broker tasks to complete
    let _ = tokio::join!(stock_updater_handle, stock_price_consumer_handle, order_matcher_handle, order_status_receiver_handle);
    //broker.stop();
    
    // 13. Generate client performance report
    println!("MARKET CLOSED");
    let json_file_path = "src/data/client_holdings.json";
    performance::generate_client_report(json_file_path);
}