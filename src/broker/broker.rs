// broker/broker.rs

use tokio::sync::{broadcast::Receiver, Mutex};
use std::sync::atomic::Ordering;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, atomic::AtomicU64};
use std::collections::HashMap;
use crate::models::{Order, PriceUpdate};
use crate::broker::client::Client;


pub struct Broker {
    pub id: u64,
    clients: Vec<Arc<Mutex<Client>>>,
    price_rx: Receiver<PriceUpdate>, // Broadcast receiver for stock updates
    stock_data: Arc<Mutex<HashMap<String, f64>>>, // HashMap to store stock prices
    global_order_counter: Arc<AtomicU64>, // Shared counter for Order IDs
    stop_signal: Arc<AtomicBool>, // Shared stop signal for the broker loop
}

pub fn initialize_brokers(
    total_brokers: u64,
    price_tx: tokio::sync::broadcast::Sender<PriceUpdate>,
    global_order_counter: Arc<AtomicU64>,
) -> Vec<Arc<Mutex<Broker>>> {
    (1..=total_brokers)
        .map(|broker_id| {
            Arc::new(Mutex::new(Broker::new(
                broker_id,
                price_tx.clone(),
                global_order_counter.clone(),
            )))
        })
        .collect()
}

impl Broker {
    pub fn new(
        id: u64,
        price_tx: tokio::sync::broadcast::Sender<PriceUpdate>,
        global_order_counter: Arc<AtomicU64>,
    ) -> Self {
        // Initialize clients with unique IDs per broker
        let mut clients = Vec::new();

        // Offset client IDs based on the broker's ID
        let start_client_id = (id - 1) * 3 + 1;
        let end_client_id = id * 3;

        for client_id in start_client_id..=end_client_id {
            let client = Arc::new(Mutex::new(Client::new(client_id)));
            clients.push(client);
        }

        Self {
            id,
            clients,
            price_rx: price_tx.subscribe(), // Subscribe to the broadcast channel
            stock_data: Arc::new(Mutex::new(HashMap::new())), // Initialize an empty HashMap
            global_order_counter,
            stop_signal: Arc::new(AtomicBool::new(false)), // Initialize the stop signal to false
        }
    }


    pub async fn start_broker_task(&mut self, producer: rdkafka::producer::FutureProducer) {
        let stock_data = self.stock_data.clone();

        // Task to listen for price updates
        let mut price_rx = self.price_rx.resubscribe();
        tokio::spawn({
            let stock_data = stock_data.clone();
            async move {
                while let Ok(price_update) = price_rx.recv().await {
                    let mut stock_data_guard = stock_data.lock().await;
                    stock_data_guard.insert(price_update.name.clone(), price_update.price);

                    // for client in &clients {
                    //     let mut client = client.lock().await;
                    //     client.handle_price_update(&price_update).await;
                    // }
                    // println!("Broker {} received update: {:?}", broker_id, price_update);
                }
            }
        });

        // Main broker loop for generating orders and sending to Kafka
        loop {
            if self.stop_signal.load(Ordering::SeqCst) {
                println!("Stopping broker loop");
                break; // Exit the loop if the stop signal is set
            }
            for client in &self.clients {
                let client = client.clone();
                let stock_data = self.stock_data.clone();
                let global_order_counter = self.global_order_counter.clone();
                let stop_signal = self.stop_signal.clone();
                let broker_id = self.id;
                let producer = producer.clone();
                tokio::spawn(async move {
                    let mut client = client.lock().await;
                    client
                        .generate_order(broker_id, stock_data, global_order_counter, "src/data/client_holdings.json", 5.0, 5.0, 3, stop_signal)
                        .await;
            
                    let orders = client.collect_orders();
                    for order in orders {
                        Broker::send_order_to_kafka_static(order, &producer).await;
                    }
                });
            }
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        }
    }

    async fn send_order_to_kafka_static(order: Order, producer: &rdkafka::producer::FutureProducer) {
        let payload = serde_json::to_string(&order).expect("Failed to serialize order");
        producer
            .send(
                rdkafka::producer::FutureRecord::to("orders")
                    .key(&order.order_id.to_string())
                    .payload(&payload),
                rdkafka::util::Timeout::Never,
            )
            .await
            .expect("Failed to send order to Kafka");

        //println!("Broker {} sent order to Kafka: {:?}", self.id, order);
    }
}
    