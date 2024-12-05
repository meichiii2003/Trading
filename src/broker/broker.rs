    // broker/broker.rs

use tokio::sync::broadcast::Receiver;
use tokio::sync::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use crate::models::PriceUpdate;
use crate::broker::client::Client;

pub struct Broker {
    pub id: u64,
    clients: Vec<Arc<Mutex<Client>>>,
    price_rx: Receiver<PriceUpdate>, // Broadcast receiver for stock updates
}

impl Broker {
    pub fn new(id: u64, price_tx: tokio::sync::broadcast::Sender<PriceUpdate>) -> Self {
        // Initialize clients
        let mut clients = Vec::new();

        // Each broker handles 3 fixed clients
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
        }
    }

    pub async fn start(&mut self,
        total_brokers: u64,
        total_updates: u64,
        tracker: Arc<Mutex<HashMap<u64, usize>>>,
        mut batch_signal_rx: tokio::sync::broadcast::Receiver<()>) {
            
        let clients = self.clients.clone();
        let id = self.id;
        let mut price_rx = self.price_rx.resubscribe();
        
        // Clone tracker for the price update processing task
        let tracker_clone = Arc::clone(&tracker);

        // Process price updates in this task
        tokio::spawn(async move {
            while let Ok(price_update) = price_rx.recv().await {
                //println!("Broker {} received price update: {:?}", id, price_update);

                // Handle price updates for all clients
                for client in &clients {
                    let mut client = client.lock().await;
                    client.handle_price_update(&price_update).await;
                }
                
                // Log that all clients for this broker processed the update
                //println!("Broker {} received & processed price update: {:?}", id, price_update);
                
                // Update tracker
                let mut tracker_guard = tracker_clone.lock().await;
                let counter = tracker_guard.entry(id).or_insert(0);
                *counter += 1;

                println!(
                    "Broker {} processed price update: {:?} (Tracker count: {})",
                    id, price_update, counter
                );

                // Check if all brokers have processed all updates
                if tracker_guard.len() == total_brokers as usize
                    && tracker_guard.values().all(|&count| count == total_updates as usize)
                {
                    println!("All brokers received all price updates.");
                }
                
            }
        });
        
        // Listen for batch signals to reset tracker
        tokio::spawn(async move {
            while let Ok(_) = batch_signal_rx.recv().await {
                let mut tracker_guard = tracker.lock().await;
        
                // Reset tracker to zero for all brokers
                for broker_id in 1..=total_brokers as u64 {
                    tracker_guard.insert(broker_id, 0);
                }
        
                println!("Broker {} reset tracker for new batch.", id);
            }
        });        
        


        // Main broker loop for generating orders
        loop {
            for client in &self.clients {
                let mut client = client.lock().await;
                client.generate_order().await;
            }

            // Sleep for 5 seconds before the next iteration
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
        }
    }
}
