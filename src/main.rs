// mod consumer;

// #[tokio::main]
// async fn main() {
//     // Run the consumer directly
//     consumer::run().await;
// }

// main.rs

mod consumer;
mod broker;
mod models;
mod communication;
mod performance;
mod utils;


use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use tokio::sync::Barrier;
use tokio::sync::Mutex;
use std::sync::Arc;

use crate::broker::broker::Broker;
use crate::models::PriceUpdate;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;

#[tokio::main]
async fn main() {
    // 1. Global order counter for unique order IDs across all brokers
    let global_order_counter = Arc::new(AtomicU64::new(1)); // Start from Order 1
    
    // 2. Broadcast channel for stock price updates
    let buffer_size = 1000; // Buffer size for the broadcast channel
    let (price_tx, _) = tokio::sync::broadcast::channel(buffer_size); // Price update broadcast channel

    // 3. Number of brokers
    let total_brokers = 5; // Total number of brokers

    //let total_updates = 5; // Total updates in each batch
    let updates_per_batch = 5; // Updates per batch
    let tracker: Arc<Mutex<HashMap<u32, PriceUpdate>>> = Arc::new(Mutex::new(HashMap::new()));
    let barrier = Arc::new(Barrier::new(total_brokers as usize)); // Create a barrier for synchronization
    
    // 2. Create brokers
    // let mut broker_handles = Vec::new();
    // // Start brokers first
    // for broker_id in 1..=total_brokers {
    //     let mut broker = Broker::new(broker_id, price_tx.clone());
    //     let producer = create_producer(); // Assuming create_producer() returns a FutureProducer

    //     let handle = tokio::spawn(async move {
    //         broker.start(producer).await; // Start the broker
    //     });

    //     broker_handles.push(handle);
    // }
    // 4. Start brokers
    let mut broker_handles = Vec::new();
    for broker_id in 1..=total_brokers {
        let price_rx = price_tx.subscribe(); // Each broker gets its own receiver
        let global_counter = global_order_counter.clone(); // Shared global counter for order IDs
        let producer = create_producer(); // Create a Kafka producer

        // Create a broker instance and start it
        let mut broker = Broker::new(broker_id, price_tx.clone(), global_counter);
        let handle = tokio::spawn(async move {
            broker.start(producer).await; // Start the broker
        });

        broker_handles.push(handle);
    }

    // let mut broker_handles = Vec::new();
    // // Start brokers first
    // for broker_id in 1..=total_brokers {
    //     let mut broker = Broker::new(broker_id, price_tx.clone());
    //     let producer = create_producer(); // Assuming create_producer() returns a FutureProducer

    //     let handle = tokio::spawn(async move {
    //         broker.start(producer).await; // Start the broker
    //     });

    //     broker_handles.push(handle);
    // }
    // 3. Start the Kafka consumer task
    // task::spawn(async move {
    //     consumer::run_consumer(channels.price_tx.clone()).await;
    // });
    // 3. Initialize batch signal channel
    // Function to create a Kafka producer
    fn create_producer() -> FutureProducer {
        ClientConfig::new()
            .set("bootstrap.servers", "localhost:9092")
            .set("message.timeout.ms", "5000")
            .create()
            .expect("Producer creation error")
    }
    
     // 4. Start the Kafka consumer

    // 4. Start the Kafka consumer
    // let consumer_handle = tokio::spawn(async move {
    //     consumer::run_consumer(price_tx.clone()).await;
    // });
    let consumer_handle = tokio::spawn(async move {
        consumer::run_consumer(price_tx.clone()).await;
    });
    
    // Wait for the consumer task to complete
    consumer_handle.await.unwrap();

    // 4. Wait for the market to close (simulate trading hours)
    // For example, run for 8 hours
    // tokio::time::sleep(tokio::time::Duration::from_secs(8 * 3600)).await;

    // 5. Signal market close (you may need to implement a shutdown mechanism)
    // For simplicity, we can just cancel the tasks in this example

    // 6. Wait for all tasks to complete
    // for handle in broker_handles {
    //     handle.abort(); // Abort the broker tasks
    // }
    // consumer_handle.abort(); // Abort the consumer task

    // // 7. Calculate performance metrics
    // performance::generate_reports().await;

    // 8. Exit the application
}





// mod kafka;

// use tokio::sync::mpsc;
// use tokio;

// use kafka::KafkaConfig;
// use std::sync::Arc;


// #[tokio::main]
// async fn main() {
//     // Connect to kafka
//     let brokers = "localhost:9092";
//     let topic = "stock";
//     let group_id = "stock-price-consumer-group";

//     // let kafka_config = KafkaConfig::new(brokers, group_id);

//     let kafka_config = Arc::new(KafkaConfig::new(brokers, group_id));


//     let (stock_sender, stock_receiver) = mpsc::channel(32);
//     let (risk_sender, risk_receiver) = mpsc::channel(32);

//     let kafka_config_producer = Arc::clone(&kafka_config);
//     tokio::spawn(async move {
//         kafka_config_producer.producer_task(topic, stock_receiver).await;
//     });

//     // Clone Arc for the consumer task
//     let kafka_config_consumer = Arc::clone(&kafka_config);
//     tokio::spawn(async move {
//         kafka_config_consumer.consumer_task(topic, risk_sender).await;
//     });

    
//     // Allow tasks to run indefinitely
//     tokio::signal::ctrl_c().await.unwrap();
//     println!("Application exiting...");
// }


