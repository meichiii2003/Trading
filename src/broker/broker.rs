    // broker/broker.rs

use rdkafka::producer::{self, FutureProducer, FutureRecord};
use tokio::sync::{broadcast::Receiver, Mutex};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::sync::{Arc, atomic::AtomicU64};
use std::collections::HashMap;
use crate::broker::update_client_portfolio_in_json;
use crate::models::{Order, OrderAction, OrderType, PriceUpdate};
use crate::broker::client::{BrokerRecords, Client};


pub struct Broker {
    pub id: u64,
    clients: Vec<Arc<Mutex<Client>>>,
    price_rx: Receiver<PriceUpdate>, // Broadcast receiver for stock updates
    stock_data: Arc<Mutex<HashMap<String, f64>>>, // HashMap to store stock prices
    global_order_counter: Arc<AtomicU64>, // Shared counter for Order IDs
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
    pub fn get_clients(&self) -> &Vec<Arc<Mutex<Client>>> {
        &self.clients
    }
    

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
        }
    }

    


    pub async fn start(&mut self, producer: rdkafka::producer::FutureProducer) {
        let stock_data = self.stock_data.clone();
        let clients = self.clients.clone();
        let global_order_counter = self.global_order_counter.clone(); // Access shared counter
        let broker_id = self.id;

        // Task to listen for price updates
        let mut price_rx = self.price_rx.resubscribe();
        tokio::spawn({
            let stock_data = stock_data.clone();
            let clients = clients.clone();
            let broker_id = broker_id;
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
            for client in &self.clients {
                let mut client = client.lock().await;
                client.generate_order(self.id, self.stock_data.clone(), global_order_counter.clone(),"src/data/client_holdings.json", 5.0, 5.0).await;

                let orders = client.collect_orders();
                for order in orders {
                    self.send_order_to_kafka(order, &producer).await;
                }
    
            }
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
        }
    }


    // pub async fn process_order(&self, order: Order) {
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

    async fn send_order_to_kafka(&self, order: Order, producer: &rdkafka::producer::FutureProducer) {
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

    pub async fn process_completed_orders(
        &mut self,
        completed_orders: Vec<Order>,
        json_file_path: &str,
    ) {
        for completed_order in completed_orders {
            for client in &self.clients {
                let mut client = client.lock().await;
                if client.id == completed_order.client_id {
                    // Update the client's portfolio
                    client
                        .portfolio
                        .update_holdings(&completed_order.stock_symbol, completed_order.quantity as i64);
    
                    let is_buy = completed_order.order_action == OrderAction::Buy;
                    let total_cost = completed_order.quantity as f64 * completed_order.price;
    
                    if is_buy {
                        // Deduct the cost from the client's capital
                        if client.capital >= total_cost {
                            client.capital -= total_cost;
                        } else {
                            println!(
                                "Error: Client {} does not have sufficient capital for the completed buy order.",
                                client.id
                            );
                            continue;
                        }
                    } else {
                        // For sell orders, add the proceeds to the client's capital
                        client.capital += total_cost;
                    }
    
                    println!(
                        "Client {}: Updated capital after completed order: {:.2}",
                        client.id, client.capital
                    );
    
                    // Update the JSON file with the new portfolio and capital
                    update_client_portfolio_in_json(
                        json_file_path,
                        completed_order.client_id,
                        completed_order.stock_symbol.clone(),
                        completed_order.quantity,
                        is_buy,
                        completed_order.price, // Pass price per unit to update capital
                    )
                    .await;
    
                    // println!(
                    //     "Updated Portfolio for Client {}: {:?}",
                    //     client.id,
                    //     client.portfolio.get_holdings()
                    // );
                }
            }
        }
    }
    
    
    // pub async fn process_rejected_orders(&mut self, rejected_orders: Vec<Order>) {
    //     for order in rejected_orders {
    //         for client in &self.clients {
    //             let mut client = client.lock().await;
    //             if client.id == order.client_id {
    //                 client.handle_rejected_order(&order);
    //                 break;
    //             }
    //         }
    //     }
    // }


    // pub async fn process_rejected_orders(
    //     &mut self,
    //     rejected_orders: Vec<Order>,
    // ) {
    //     let mut processed_orders = std::collections::HashSet::new(); // Track processed orders
    
    //     for rejected_order in rejected_orders {
    //         // Skip already processed orders
    //         if !processed_orders.insert(rejected_order.order_id.clone()) {
    //             continue;
    //         }
    
    //         println!("Processing rejected order: {:?}", rejected_order);
    
    //         // Step 1: Restore capital
    //         for client in &self.clients {
    //             let mut client = client.lock().await;
    //             if client.id == rejected_order.client_id {
    //                 let amount_to_restore = rejected_order.quantity as f64 * rejected_order.price;
    //                 client.capital += amount_to_restore;
    
    //                 println!(
    //                     "Client {}: Restored {:.2} to capital for rejected order {}. New capital: {:.2}",
    //                     client.id, amount_to_restore, rejected_order.order_id, client.capital
    //                 );
    
    //                 // Remove the rejected order from pending orders
    //                 client.pending_orders.retain(|order| order.order_id != rejected_order.order_id);
    //             }
    //         }
    //     }
    // }
    
    

                    // // Remove from in-memory broker records
                    // client.broker_records.retain(|record| {
                    //     if record.order_id == rejected_order.order_id {
                    //         println!(
                    //             "Removing in-memory record: {{ client_id: {}, order_id: {}, stop_loss: {:?}, take_profit: {:?} }}",
                    //             record.client_id, record.order_id, record.stop_loss, record.take_profit
                    //         );
                    //         false
                    //     } else {
                    //         true
                    //     }
                    // });
            


        //     // Step 2: Read broker records from JSON file
        //     let mut file = match File::open(broker_records_file) {
        //         Ok(file) => file,
        //         Err(err) => {
        //             println!("Error opening JSON file: {}. Skipping order {}.", err, rejected_order.order_id);
        //             continue;
        //         }
        //     };

        //     let mut json_data = String::new();
        //     if let Err(err) = file.read_to_string(&mut json_data) {
        //         println!("Error reading JSON file: {}. Skipping order {}.", err, rejected_order.order_id);
        //         continue;
        //     }

        //     let mut broker_records: BrokerRecords = match serde_json::from_str(&json_data) {
        //         Ok(records) => records,
        //         Err(err) => {
        //             println!("Error parsing JSON file: {}. Skipping order {}.", err, rejected_order.order_id);
        //             continue;
        //         }
        //     };

        //     // Step 3: Remove the rejected order from broker records
        //     let initial_count = broker_records.records.len();
        //     broker_records.records.retain(|record| record.order_id != rejected_order.order_id);

        //     if broker_records.records.len() < initial_count {
        //         println!(
        //             "Successfully removed rejected order {} from broker records JSON.",
        //             rejected_order.order_id
        //         );
        //     } else {
        //         println!(
        //             "Order {} not found in broker records JSON. No removal performed.",
        //             rejected_order.order_id
        //         );
        //     }

        //     // Step 4: Write updated records back to the JSON file
        //     let updated_json = match serde_json::to_string_pretty(&broker_records) {
        //         Ok(json) => json,
        //         Err(err) => {
        //             println!(
        //                 "Error serializing updated JSON: {}. Skipping write for order {}.",
        //                 err, rejected_order.order_id
        //             );
        //             continue;
        //         }
        //     };

        //     if let Err(err) = OpenOptions::new()
        //         .write(true)
        //         .truncate(true)
        //         .open(broker_records_file)
        //         .and_then(|mut file| file.write_all(updated_json.as_bytes()))
        //     {
        //         println!(
        //             "Error writing updated JSON file: {}. Skipping write for order {}.",
        //             err, rejected_order.order_id
        //         );
        //     } else {
        //         println!(
        //             "Broker records JSON updated successfully after removing order {}.",
        //             rejected_order.order_id
        //         );
        //     }
        // }
    

       


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
    