// broker/client.rs;

use crate::broker::portfolio::Portfolio;
use crate::models::{Order, OrderAction, OrderStatus, OrderType};

use rand::Rng;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fs::File;
use std::io::Read;
use std::sync::atomic::{AtomicBool, Ordering};
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
    pub current_orders: usize,
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


impl Client {
    pub fn new(id: u64) -> Self {
        //let initial_capital = 10_000.0 + rand::thread_rng().gen_range(0.0..10_000.0); // Random initial capital between $10,000 and $20,000
        let initial_capital = 20_000.0; // Fixed initial capital for simplicity
        Self {
            id,
            initial_capital,
            capital: initial_capital,
            portfolio: Portfolio::new(),
            pending_orders: Vec::new(),
            broker_records: Vec::new(),
            current_orders: 0,
        }
    }

    pub async fn generate_order(
        &mut self,
        broker_id: u64,
        stock_data: Arc<Mutex<HashMap<String, f64>>>,
        global_order_counter: Arc<AtomicU64>,
        json_file_path: &str, 
        upper_threshold: f64,
        lower_threshold: f64,
        max_orders: usize, 
        stop_signal: Arc<AtomicBool>, 
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
            return;
        }
    
        let portfolio = client_portfolio.unwrap();
    
        // Generate orders for both buy and sell cases
        let available_stocks: Vec<(&String, &f64)> = stock_data_guard.iter().collect();
        let mut orders_generated = 0; 
        for (stock_symbol, &market_price) in available_stocks.iter() {
            if orders_generated >= max_orders {
                break;
            }
            if stop_signal.load(Ordering::SeqCst) {
                println!("Stopping order generation for Client {}", self.id);
                return; // Exit if stop signal is set
            }
            
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

                        // println!(
                        //     "Stock: {}, Avg Price: {:.2}, Market Price: {:.2}, Profit Threshold: {:.2}, Loss Threshold: {:.2}",
                        //     stock_symbol, average_price, market_price, price_increase_threshold, price_decrease_threshold
                        // );

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
                    //if self.capital >= total_cost {
                        println!(
                            "{}",
                            format!(
                                "Client {}: Placing Buy order for {} ({}, {} shares at {:.2} per share). Total Cost: {:.2}",
                                self.id, order.stock_symbol, order.order_id, order.quantity, order.price, total_cost
                            )
                            .bright_blue().bold()
                        );
                        //self.capital -= total_cost;
                   // } 
                    // else {
                    //     println!(
                    //         "{}",
                    //         format!(
                    //             "Client {}: Insufficient funds for Buy order on {}. Required: {:.2}, Available: {:.2}",
                    //             self.id, order.stock_symbol, total_cost, self.capital
                    //         )
                    //         .bright_yellow().bold()
                    //     );
                    //     continue;
                    // }
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
                orders_generated += 1; // Increment the counter
            }
        }
    
    }


    pub fn collect_orders(&mut self) -> Vec<Order> {
        let orders = self.pending_orders.clone();
        self.pending_orders.clear();
        orders
    }
}
