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
use tokio::sync::Barrier;
use tokio::sync::Mutex;
use std::sync::Arc;

use crate::broker::broker::Broker;

#[tokio::main]
async fn main() {
    // // 1. Initialize communication channels
    // let (price_tx, price_rx) = mpsc::unbounded_channel();
    // let channels = CommunicationChannels::new(price_tx, price_rx);

    // 1. Initialize broadcast channel
    let buffer_size = 1000; // Buffer size for the broadcast channel
    let (price_tx, _) = tokio::sync::broadcast::channel(buffer_size); // Create the broadcast channel
    //let (batch_signal_tx, _) = tokio::sync::broadcast::channel(buffer_size);

    // Shared tracker for brokers
    let total_brokers = 5; // Total brokers
    //let total_updates = 5; // Total updates in each batch
    let updates_per_batch = 5; // Updates per batch
    let tracker = Arc::new(Mutex::new(HashMap::new()));
    let barrier = Arc::new(Barrier::new(total_brokers as usize)); // Create a barrier for synchronization
    
    // 2. Create brokers
    let mut broker_handles = Vec::new();
    // Start brokers first
    for broker_id in 1..=total_brokers {
        let mut broker = Broker::new(broker_id, price_tx.clone());
        let tracker_clone = tracker.clone();
        //let batch_signal_rx = batch_signal_tx.subscribe();
        let barrier_clone = barrier.clone();

        let handle = tokio::spawn(async move {
            broker
                .start(total_brokers, updates_per_batch, tracker_clone, barrier_clone)
                .await;
        });

        broker_handles.push(handle);
    }
    // 3. Start the Kafka consumer task
    // task::spawn(async move {
    //     consumer::run_consumer(channels.price_tx.clone()).await;
    // });
    // 3. Initialize batch signal channel
    //let (batch_signal_tx, _) = tokio::sync::broadcast::channel(buffer_size); // Create the batch signal channel

    // 4. Start the Kafka consumer
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


