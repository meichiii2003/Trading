    // broker/broker.rs

use rdkafka::producer::{self, FutureProducer, FutureRecord};
use tokio::sync::{broadcast::Receiver, Mutex};
use std::sync::{Arc, atomic::AtomicU64};
use std::collections::HashMap;
use crate::models::{KafkaOrderRequest, BrokerOrderRecord, OrderType, PriceUpdate};
use crate::broker::client::Client;

pub struct Broker {
    pub id: u64,
    clients: Vec<Arc<Mutex<Client>>>,
    price_rx: Receiver<PriceUpdate>, // Broadcast receiver for stock updates
    stock_data: Arc<Mutex<HashMap<String, f64>>>, // HashMap to store stock prices
    global_order_counter: Arc<AtomicU64>, // Shared counter for Order IDs
}

impl Broker {
    pub fn new(id: u64, price_tx: tokio::sync::broadcast::Sender<PriceUpdate>, global_order_counter: Arc<AtomicU64>) -> Self {
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
            stock_data: Arc::new(Mutex::new(HashMap::new())), // Initialize an empty HashMap
            global_order_counter,
        }
    }

    pub async fn start(&mut self, producer: rdkafka::producer::FutureProducer) {
        let stock_data = self.stock_data.clone();
        let clients = self.clients.clone();
        let global_order_counter = self.global_order_counter.clone(); // Access shared counter
        let broker_id = self.id;
        let mut price_rx = self.price_rx.resubscribe();
        tokio::spawn({
            let stock_data = stock_data.clone();
            let clients = clients.clone();
            let broker_id = broker_id;
            async move {
                while let Ok(price_update) = price_rx.recv().await {
                    let mut stock_data_guard = stock_data.lock().await;
                    stock_data_guard.insert(price_update.name.clone(), price_update.price);

                    for client in &clients {
                        let mut client = client.lock().await;
                        client.handle_price_update(&price_update).await;
                    }
                    println!("Broker {} received update: {:?}", broker_id, price_update);
                }
            }
        });
        // Main broker loop for generating orders
        loop {
            for client in &self.clients {
                let mut client = client.lock().await;
                client.generate_order(self.id, self.stock_data.clone(), global_order_counter.clone()).await;

                let orders = client.collect_orders();
                for order in orders {
                    self.send_order_to_kafka(order, &producer).await;
                }
            }

            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
        }
    }


    // pub async fn process_order(&self, order: KafkaOrderRequest) {
    //     match order.order_type {
    //         OrderType::MarketBuy { price, take_profit, stop_loss } => {
    //             println!(
    //                 "Broker {} processing MarketBuy Order: Stock: {}, Price: {:.2}, Take Profit: {:?}, Stop Loss: {:?}",
    //                 self.id, order.stock_symbol, price, take_profit, stop_loss
    //             );
    //             // Logic for market buy
    //         }
    //         OrderType::MarketSell { price, take_profit, stop_loss } => {
    //             println!(
    //                 "Broker {} processing MarketSell Order: Stock: {}, Price: {:.2}, Take Profit: {:?}, Stop Loss: {:?}",
    //                 self.id, order.stock_symbol, price, take_profit, stop_loss
    //             );
    //             // Logic for market sell
    //         }
    //         OrderType::LimitBuy { price, take_profit, stop_loss } => {
    //             println!(
    //                 "Broker {} processing LimitBuy Order: Stock: {}, Price: {:.2}, Take Profit: {:?}, Stop Loss: {:?}",
    //                 self.id, order.stock_symbol, price, take_profit, stop_loss
    //             );
    //             // Logic for limit buy
    //         }
    //         OrderType::LimitSell { price, take_profit, stop_loss } => {
    //             println!(
    //                 "Broker {} processing LimitSell Order: Stock: {}, Price: {:.2}, Take Profit: {:?}, Stop Loss: {:?}",
    //                 self.id, order.stock_symbol, price, take_profit, stop_loss
    //             );
    //             // Logic for limit sell
    //         }
    //     }
    // }

    // pub async fn monitor_prices_and_execute_orders(
    //     &self,
    //     price_update: &PriceUpdate,
    //     pending_orders: &mut Vec<Order>,) 
    //     {
    //     let stock_symbol = &price_update.name;
    
    //     // Check pending orders
    //     for order in pending_orders.iter_mut() {
    //         if order.stock_symbol == *stock_symbol {
    //             match order.order_type {
    //                 OrderType::MarketBuy { price, take_profit, stop_loss } => {
    //                     if let Some(tp) = take_profit {
    //                         if price_update.price >= tp {
    //                             println!(
    //                                 "Take Profit triggered for Order {}: Selling Stock {} at {:.2}",
    //                                 order.order_id, stock_symbol, price_update.price
    //                             );
    //                             // Logic to execute take profit
    //                         }
    //                     }
    //                     if let Some(sl) = stop_loss {
    //                         if price_update.price <= sl {
    //                             println!(
    //                                 "Stop Loss triggered for Order {}: Selling Stock {} at {:.2}",
    //                                 order.order_id, stock_symbol, price_update.price
    //                             );
    //                             // Logic to execute stop loss
    //                         }
    //                     }
    //                 }
    //                 // Similar handling for other order types
    //                 _ => {}
    //             }
    //         }
    //     }
    // }

    async fn send_order_to_kafka(&self, order: KafkaOrderRequest, producer: &rdkafka::producer::FutureProducer) {
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

        println!("Broker {} sent order to Kafka: {:?}", self.id, order);
    }

    pub async fn process_client_orders(&self, client: &Arc<Mutex<Client>>, producer: &FutureProducer) {
        // Collect orders from the client
        let mut client = client.lock().await;
        let client_orders = client.collect_orders();

        // Send each order one by one
        for order in client_orders {
            self.send_order_to_kafka(order, &producer).await;
        }
    }
    
}
    