// broker/client.rs;

use crate::broker::portfolio::Portfolio;
use crate::models::{Order, OrderAction, OrderStatus, OrderType, PriceUpdate};

use rand::seq::SliceRandom;
use rand::Rng;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::sync::atomic::Ordering;
use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use tokio::sync::Mutex;
use std::fs;
use colored::*;

pub struct Client {
    pub id: u64,
    pub initial_capital: f64,
    pub capital: f64,
    pub portfolio: Portfolio,
    pub pending_orders: Vec<Order>,
    pub broker_records: Vec<BrokerRecord>,
    // Additional fields as needed
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BrokerRecord {
    pub client_id: u64,
    pub order_id: String,
    pub stop_loss: Option<f64>,
    pub take_profit: Option<f64>,
}

#[derive(Serialize, Deserialize)]
pub struct BrokerRecords {
    pub records: Vec<BrokerRecord>,
}


pub fn reset_broker_records(file_path: &str) {
    if fs::write(file_path, r#"{"records": []}"#).is_err() {
        println!("Failed to reset broker records JSON file.");
    } else {
        println!("Broker records JSON file has been reset.");
    }
}

// pub fn update_broker_records_json(file_path: &str, new_records: Vec<BrokerRecord>) {
//     // Step 1: Read the existing broker records from the file
//     let mut existing_records = match File::open(file_path) {
//         Ok(mut file) => {
//             let mut json_data = String::new();
//             if let Err(err) = file.read_to_string(&mut json_data) {
//                 println!("Error reading broker records JSON file: {}", err);
//                 Vec::new()
//             } else {
//                 serde_json::from_str::<BrokerRecords>(&json_data)
//                     .map(|data| data.records)
//                     .unwrap_or_else(|_| Vec::new())
//             }
//         }
//         Err(_) => Vec::new(), // File doesn't exist; start fresh
//     };

//     // Step 2: Prepend new records if they are not duplicates
//     for new_record in new_records.into_iter().rev() {
//         if !existing_records.iter().any(|r| r.order_id == new_record.order_id) {
//             existing_records.insert(0, new_record); // Add to the front
//         }
//     }

//     // Step 3: Serialize the combined records to JSON
//     let updated_broker_records = BrokerRecords {
//         records: existing_records,
//     };
//     let json_data = serde_json::to_string_pretty(&updated_broker_records)
//         .expect("Failed to serialize broker records to JSON");

//     // Step 4: Write the updated JSON back to the file
//     let mut file = OpenOptions::new()
//         .write(true)
//         .truncate(true)
//         .create(true)
//         .open(file_path)
//         .expect("Failed to open broker records JSON file");

//     file.write_all(json_data.as_bytes())
//         .expect("Failed to write broker records JSON");

//     println!("Broker records JSON updated successfully.");
// }



impl Client {
    pub fn new(id: u64) -> Self {
        //let initial_capital = 10_000.0 + rand::thread_rng().gen_range(0.0..10_000.0); // Random initial capital between $10,000 and $20,000
        let initial_capital = 10_000.0; // Fixed initial capital for simplicity
        Self {
            id,
            initial_capital,
            capital: initial_capital,
            portfolio: Portfolio::new(),
            pending_orders: Vec::new(),
            broker_records: Vec::new(),
            // Initialize other fields
        }
    }

    pub async fn generate_order(
        &mut self,
        broker_id: u64,
        stock_data: Arc<Mutex<HashMap<String, f64>>>,
        global_order_counter: Arc<AtomicU64>,
        json_file_path: &str, // Path to the client holdings JSON file
        upper_threshold: f64,
        lower_threshold: f64,
    ) {
        let stock_data_guard = stock_data.lock().await;
    
        // Load client portfolio from the JSON file
        let mut json_data = String::new();
        let mut file = File::open(json_file_path).expect("Failed to open JSON file");
        file.read_to_string(&mut json_data).expect("Failed to read JSON file");
    
        let parsed_data: Value = serde_json::from_str(&json_data).expect("Failed to parse JSON");
    
        // Get the client's portfolio based on broker_id and client_id
        let client_portfolio = parsed_data["brokers"]
            .as_array()
            .and_then(|brokers| {
                brokers.iter().find(|broker| broker["broker_id"] == broker_id)
            })
            .and_then(|broker| {
                broker["clients"]
                    .as_array()
                    .and_then(|clients| {
                        clients.iter().find(|client| client["client_id"] == self.id)
                    })
            })
            .and_then(|client| client["portfolio"].as_object());
    
        if client_portfolio.is_none() {
            println!(
                "Client {} (Broker {}) has no portfolio. Skipping sell order generation.",
                self.id, broker_id
            );
        }
    
        let portfolio = client_portfolio.unwrap();
    
        // Generate orders for both buy and sell cases
        let available_stocks: Vec<(&String, &f64)> = stock_data_guard.iter().collect();
        for (stock_symbol, &market_price) in available_stocks.iter() {
            let mut rng = rand::thread_rng();
            let is_limit_order = rng.gen_bool(0.7); // 70% Limit Orders
            let is_buy_order = rng.gen_bool(0.5);   // 50% Buy, 50% Sell
            let quantity = rng.gen_range(1..=10); // Random quantity between 1 and 10
    
            let price_modifier = {
                let random_percent = rng.gen_range(0..100); // Generate a random percentage (0-99)
                if random_percent < 90 {
                    rng.gen_range(0.8..=1.2) // 80% chance within valid range (80%-120%)
                } else if random_percent < 95 {
                    rng.gen_range(0.7..0.8) // 10% chance below valid range (70%-80%)
                } else {
                    rng.gen_range(1.2..1.3) // 10% chance above valid range (120%-130%)
                }
            };
    
            let limit_price = market_price * price_modifier;
            let rounded_limit_price = (limit_price * 100.0).round() / 100.0;
            let mut valid_order = None;
    
            if is_buy_order {
                // BUY ORDER LOGIC
                if is_limit_order {
                    valid_order = Some(Order {
                        broker_id,
                        client_id: self.id,
                        order_id: String::new(), // Placeholder for now
                        stock_symbol: stock_symbol.to_string(),
                        order_type: OrderType::Limit,
                        order_action: OrderAction::Buy,
                        price: rounded_limit_price,
                        quantity,
                        status: OrderStatus::Pending,
                    });
                } else {
                    let rounded_market_price = (market_price * 100.0).round() / 100.0;
                    valid_order = Some(Order {
                        broker_id,
                        client_id: self.id,
                        order_id: String::new(),
                        stock_symbol: stock_symbol.to_string(),
                        order_type: OrderType::Market,
                        order_action: OrderAction::Buy,
                        price: rounded_market_price,
                        quantity,
                        status: OrderStatus::Pending,
                    });
                }
            } else {
                // SELL ORDER LOGIC
                if let Some(stock_data) = portfolio.get(stock_symbol.as_str()) {
                    let current_quantity = stock_data["quantity"].as_u64().unwrap_or(0);
                    let average_price = stock_data["average_price"].as_f64().unwrap_or(0.0);

                    if current_quantity > 0 {
                        let price_increase_threshold = average_price * (1.0 + upper_threshold / 100.0);
                        let price_decrease_threshold = average_price * (1.0 - lower_threshold / 100.0);

                        println!(
                            "Stock: {}, Avg Price: {:.2}, Market Price: {:.2}, Profit Threshold: {:.2}, Loss Threshold: {:.2}",
                            stock_symbol, average_price, market_price, price_increase_threshold, price_decrease_threshold
                        );

                        if market_price >= price_increase_threshold {
                            // Generate a Limit Sell Order for profit-taking
                            let limit_price = market_price * 1.05; // Set limit price 5% above current market price
                            valid_order = Some(Order {
                                broker_id,
                                client_id: self.id,
                                order_id: String::new(), // Placeholder
                                stock_symbol: stock_symbol.to_string(),
                                order_type: OrderType::Limit,
                                order_action: OrderAction::Sell,
                                price: (limit_price * 100.0).round() / 100.0,
                                quantity: current_quantity,
                                status: OrderStatus::Pending,
                            });
                        } else if market_price <= price_decrease_threshold {
                            // Generate a Limit Sell Order for loss mitigation
                            let limit_price = market_price * 0.90; // Set limit price 10% below current market price
                            valid_order = Some(Order {
                                broker_id,
                                client_id: self.id,
                                order_id: String::new(), // Placeholder
                                stock_symbol: stock_symbol.to_string(),
                                order_type: OrderType::Limit,
                                order_action: OrderAction::Sell,
                                price: (limit_price * 100.0).round() / 100.0,
                                quantity: current_quantity,
                                status: OrderStatus::Pending,
                            });
                        }
                    }
                }
            }

            if let Some(mut order) = valid_order {
                // Assign a unique order ID after validation
                order.order_id = format!(
                    "Order {}",
                    global_order_counter.fetch_add(1, Ordering::SeqCst)
                );

                // Log Buy or Sell details
                if order.order_action == OrderAction::Buy {
                    let total_cost = order.quantity as f64 * order.price;
                    if self.capital >= total_cost {
                        println!(
                            "{}",
                            format!(
                                "Client {}: Placing Buy order for {} ({}, {} shares at {:.2} per share). Total Cost: {:.2}, Capital Remaining: {:.2}",
                                self.id, order.stock_symbol, order.order_id, order.quantity, order.price, total_cost, self.capital - total_cost
                            )
                            .bright_blue().bold()
                        );
                        self.capital -= total_cost;
                    } else {
                        println!(
                            "{}",
                            format!(
                                "Client {}: Insufficient funds for Buy order on {}. Required: {:.2}, Available: {:.2}",
                                self.id, order.stock_symbol, total_cost, self.capital
                            )
                            .bright_yellow().bold()
                        );
                        continue;
                    }
                } else {
                    println!(
                        "{}",
                        format!(
                            "Client {}: Placing Sell order for {} ({}, {} shares at {:.2} per share).",
                            self.id, order.stock_symbol, order.order_id, order.quantity, order.price
                        )
                        .bright_red().bold()
                    );
                }

                // Add the order to the pending orders list
                self.pending_orders.push(order);
            }
        }
    
    }


    pub fn collect_orders(&mut self) -> Vec<Order> {
        let orders = self.pending_orders.clone();
        self.pending_orders.clear();
        orders
    }

    pub fn monitor_broker_orders(
        &mut self,
        stock_data: Arc<Mutex<HashMap<String, f64>>>,
    ) -> Vec<Order> {
        let mut new_kafka_orders = Vec::new();
        let stock_data_guard = stock_data.blocking_lock();

        self.broker_records.retain(|record| {
            if let Some(&price) = stock_data_guard.get(&record.order_id) {
                if let Some(tp) = record.take_profit {
                    if price >= tp {
                        println!(
                            "Take Profit triggered for Order {}: Selling Stock at {:.2}",
                            record.order_id, price
                        );
                        new_kafka_orders.push(Order {
                            broker_id: 0, // Broker will populate
                            client_id: self.id,
                            order_id: uuid::Uuid::new_v4().to_string(),
                            stock_symbol: record.order_id.clone(),
                            order_type: OrderType::Market,
                            order_action: OrderAction::Sell,
                            price,
                            quantity: 10, // Example quantity
                            status: OrderStatus::Pending,
                        });
                        return false;
                    }
                }
                if let Some(sl) = record.stop_loss {
                    if price <= sl {
                        println!(
                            "Stop Loss triggered for Order {}: Selling Stock at {:.2}",
                            record.order_id, price
                        );
                        new_kafka_orders.push(Order {
                            broker_id: 0, // Broker will populate
                            client_id: self.id,
                            order_id: uuid::Uuid::new_v4().to_string(),
                            stock_symbol: record.order_id.clone(),
                            order_type: OrderType::Market,
                            order_action: OrderAction::Sell,
                            price,
                            quantity: 10, // Example quantity
                            status: OrderStatus::Pending,
                        });
                        return false;
                    }
                }
            }
            true
        });

        new_kafka_orders
    }

    // pub async fn handle_completed_order(
    //     &mut self,
    //     order: &Order,
    //     initial_price: f64,
    //     stock_data: Arc<Mutex<HashMap<String, f64>>>,
    // ) {
    //     // Update capital
    //     let quantity = order.quantity as i64;
    //     let completed_price = order.price;
    //     self.capital += initial_price * quantity as f64;
    //     self.capital -= completed_price * quantity as f64;
    
    //     // Update portfolio
    //     match order.order_action {
    //         OrderAction::Buy => self.portfolio.update_holdings(&order.stock_symbol, quantity),
    //         OrderAction::Sell => self.portfolio.update_holdings(&order.stock_symbol, -quantity),
    //         _ => (),
    //     }
    
    //     // Fetch market price
    //     let market_price = stock_data.lock().await.get(&order.stock_symbol).cloned().unwrap_or(0.0);
    //     let unrealized_pnl = (market_price - completed_price) * quantity as f64;
    
    //     // println!(
    //     //     "Client {}: Processed completed order. Capital: {:.2}, Portfolio: {:?}, Market Price: {:.2}, Unrealized PnL: {:.2}",
    //     //     self.id, self.capital, self.portfolio.get_holdings(), market_price, unrealized_pnl
    //     // );
    //     // println!(
    //     //     "Client {}: Updated portfolio: {:?}",
    //     //     self.id,
    //     //     self.portfolio.get_holdings()
    //     // );
        
    // }
    

    // pub fn handle_rejected_order(&mut self, order: &Order, json_file_path: &str) {
    //     let initial_price = order.price;

    //     // Step 1: Restore the initial price to the capital
    //     self.capital += initial_price * order.quantity as f64;

    //     println!(
    //         "Client {}: Restored {:.2} to capital due to rejected order {}. New Capital: {:.2}",
    //         self.id, initial_price * order.quantity as f64, order.order_id, self.capital
    //     );
    //     // Step 2: Remove the order from broker records
    //     self.broker_records.retain(|record| record.order_id != order.order_id);
    //     println!(
    //         "Client {}: Broker records after handling rejected order: {:?}",
    //         self.id, self.broker_records
    //     );
    //     // Step 3: Remove the order from the JSON file
    //     Self::remove_order_from_json(json_file_path, &order.order_id);
    // }
    // fn remove_order_from_json(json_file_path: &str, order_id: &str) {
    //     // Read the existing JSON data
    //     let mut file = match File::open(json_file_path) {
    //         Ok(file) => file,
    //         Err(err) => {
    //             println!("Error opening JSON file: {}", err);
    //             return;
    //         }
    //     };
    //     let mut json_data = String::new();
    //     if let Err(err) = file.read_to_string(&mut json_data) {
    //         println!("Error reading JSON file: {}", err);
    //         return;
    //     }
    //     let mut broker_records: BrokerRecords = match serde_json::from_str(&json_data) {
    //         Ok(records) => records,
    //         Err(err) => {
    //             println!("Error parsing JSON: {}", err);
    //             return;
    //         }
    //     };
    //     // Remove the matching order_id
    //     let initial_len = broker_records.records.len();
    //     broker_records.records.retain(|record| record.order_id != order_id);
    
    //     if broker_records.records.len() < initial_len {
    //         println!("Order {} removed from broker records JSON.", order_id);
    //     } else {
    //         println!("Order {} not found in broker records JSON.", order_id);
    //     }
    
    //     // Write back the updated JSON
    //     let updated_json = match serde_json::to_string_pretty(&broker_records) {
    //         Ok(json) => json,
    //         Err(err) => {
    //             println!("Error serializing updated JSON: {}", err);
    //             return;
    //         }
    //     };
    
    //     if let Err(err) = File::create(json_file_path)
    //         .and_then(|mut file| file.write_all(updated_json.as_bytes()))
    //     {
    //         println!("Error writing updated JSON: {}", err);
    //     } else {
    //         println!("Broker records JSON updated successfully.");
    //     }
    // }
    

    pub async fn handle_price_update(&mut self, price_update: &PriceUpdate) {
        let stock_symbol = &price_update.name;
        let current_market_price = price_update.price;

        // Calculate the total market value of current holdings for the stock
        let total_market_value = current_market_price * self.portfolio.get_quantity(stock_symbol) as f64;

        // Update the capital to reflect current market value
        self.capital = self.initial_capital + total_market_value;

        println!(
            "Client {}: Updated capital to {:.2} with market price update for {}",
            self.id, self.capital, stock_symbol
        );
    }

    async fn estimate_order_cost(&self, order: &Order) -> f64 {
        // For simplicity, assume we have access to the latest price
        // In a real application, you'd need to maintain a price cache
        let price_per_share = 100.0; // Placeholder value
        price_per_share * (order.quantity as f64)
    }

    pub fn update_on_execution(&mut self, executed_order: &Order, execution_price: f64) {
        match executed_order.order_type {
            OrderType::Market { .. } | OrderType::Limit { .. } => {
                self.portfolio.update_holdings(&executed_order.stock_symbol, executed_order.quantity as i64);
                // Adjust capital if needed (e.g., for execution price difference)
            }
            OrderType::Market { .. } | OrderType::Limit { .. } => {
                self.portfolio.update_holdings(&executed_order.stock_symbol, -(executed_order.quantity as i64));
                // Increase capital based on execution price
                let proceeds = execution_price * (executed_order.quantity as f64);
                self.capital += proceeds;
            }
        }

        // Remove the order from pending orders if necessary
        self.pending_orders.retain(|o| o.order_id != executed_order.order_id);
    }
}
