// broker/client.rs;

use crate::broker::portfolio::Portfolio;
use crate::models::{BrokerOrderRecord, Order, OrderAction, OrderStatus, OrderType, PriceUpdate};
use crate::broker::broker::Broker;

use crate::utils;
use rand::seq::SliceRandom;
use rand::Rng;
use serde_json::Value;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use std::sync::atomic::Ordering;
use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

use super::broker;

pub struct Client {
    pub id: u64,
    pub initial_capital: f64,
    pub capital: f64,
    pub portfolio: Portfolio,
    pending_orders: Vec<Order>,
    broker_records: Vec<BrokerOrderRecord>,
    // Additional fields as needed
}

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
    ) {
        let stock_data_guard = stock_data.lock().await;
    
        // Get all available stocks and their market prices
        let available_stocks: Vec<(&String, &f64)> = stock_data_guard.iter().collect();
    
        // Prioritize stocks with low or no holdings
        let mut prioritized_stocks: Vec<&(&String, &f64)> = available_stocks
            .iter()
            .filter(|(symbol, _)| self.portfolio.get_quantity(symbol) == 0)
            .collect();
    
        if prioritized_stocks.is_empty() {
            // If all stocks have holdings, prioritize the least-held stocks
            let mut sorted_stocks: Vec<_> = available_stocks.iter().collect();
            sorted_stocks.sort_by_key(|(symbol, _)| self.portfolio.get_quantity(symbol));
            prioritized_stocks = sorted_stocks;
        }
    
        if let Some(&(stock_symbol, &market_price)) = prioritized_stocks.choose(&mut rand::thread_rng()) {
            let mut rng = rand::thread_rng();
            let is_limit_order = rng.gen_bool(0.7); // 70% Limit Orders
            let is_buy_order = rng.gen_bool(0.5);   // 50% Buy, 50% Sell
            let quantity = rng.gen_range(1..=10); // Random quantity between 1 and 10
            
            // Generate price based on distribution
            let price_modifier = {
                let random_percent = rng.gen_range(0..100); // Generate a random percentage (0-99)
                if random_percent < 90 {
                    // 80% chance: within valid range (80%-120%)
                    rng.gen_range(0.8..=1.2)
                } else if random_percent < 95 {
                    // 10% chance: below valid range (70%-80%)
                    rng.gen_range(0.7..0.8)
                } else {
                    // 10% chance: above valid range (120%-130%)
                    rng.gen_range(1.2..1.3)
                }
            };

            let limit_price = market_price * price_modifier;
            let rounded_limit_price = (limit_price * 100.0).round() / 100.0;
            
            let mut valid_order = None;
    
            if is_buy_order {
                // Generate Buy Order
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
                    // MarketBuy Order
                    let rounded_market_price = (market_price * 100.0).round() / 100.0;
                    valid_order = Some(Order {
                        broker_id,
                        client_id: self.id,
                        order_id: String::new(), // Placeholder for now
                        stock_symbol: stock_symbol.to_string(),
                        order_type: OrderType::Market,
                        order_action: OrderAction::Buy,
                        price: rounded_market_price,
                        quantity,
                        status: OrderStatus::Pending,
                    });
                }
            } else if self.portfolio.get_quantity(stock_symbol) >= quantity {
                // Generate Sell Order
                if is_limit_order {
                    valid_order = Some(Order {
                        broker_id,
                        client_id: self.id,
                        order_id: String::new(), // Placeholder for now
                        stock_symbol: stock_symbol.to_string(),
                        order_type: OrderType::Limit,
                        order_action: OrderAction::Sell,
                        price: rounded_limit_price,
                        quantity,
                        status: OrderStatus::Pending,
                    });
                } else {
                    let rounded_market_price = (market_price * 100.0).round() / 100.0;
                    valid_order = Some(Order {
                        broker_id,
                        client_id: self.id,
                        order_id: String::new(), // Placeholder for now
                        stock_symbol: stock_symbol.to_string(),
                        order_type: OrderType::Market,
                        order_action: OrderAction::Sell,
                        price: rounded_market_price,
                        quantity,
                        status: OrderStatus::Pending,
                    });
                }
            }
    
            if let Some(mut order) = valid_order {
                // Assign a unique order ID after validation
                order.order_id = format!("Order {}", global_order_counter.fetch_add(1, Ordering::SeqCst));

                if order.order_action == OrderAction::Buy {
                    let total_cost = order.quantity as f64 * order.price;
            
                    if self.capital >= total_cost {
                        println!(
                            "Client {}: Placing Buy order for {} ({}, {} shares at {:.2} per share). Total Cost: {:.2}, Initial Capital: {:.2}, Available Capital after order: {:.2}",
                            self.id, order.stock_symbol, order.order_id, order.quantity, order.price, total_cost, self.capital, self.capital - total_cost
                        );
                        self.capital -= total_cost;
                    } else {
                        println!(
                            "Client {}: Insufficient funds for Buy order on {} ({}, {} shares at {:.2} per share). Total Cost: {:.2}, Available Capital: {:.2}",
                            self.id, order.stock_symbol, order.order_id, order.quantity, order.price, total_cost, self.capital
                        );
                        return;
                    }
                }
            
                // Add the order to the pending orders list
                self.pending_orders.push(order);
    
                // Print the generated order
                // println!(
                //     "Client {} generated order: {:?}",
                //     self.id, self.pending_orders.last().unwrap()
                // );
    
                // Add Broker Records for Buy Orders with Stop-Loss/Take-Profit
                if self.pending_orders.last().unwrap().order_action == OrderAction::Buy {
                    let rounded_stop_loss = (market_price * 0.9 * 100.0).round() / 100.0;
                    let rounded_take_profit = (market_price * 1.1 * 100.0).round() / 100.0;
    
                    self.broker_records.push(BrokerOrderRecord {
                        order_id: self.pending_orders.last().unwrap().order_id.clone(),
                        stop_loss: Some(rounded_stop_loss),
                        take_profit: Some(rounded_take_profit),
                    });
                    // if let Some(last_record) = self.broker_records.last() {
                    //     println!(
                    //         "Client {} recorded internal broker order: {{ order_id: \"{}\", stop_loss: {:.2}, take_profit: {:.2} }}",
                    //         self.id,
                    //         last_record.order_id,
                    //         last_record.stop_loss.unwrap_or_default(),
                    //         last_record.take_profit.unwrap_or_default()
                    //     );
                    // }
                }
                
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
    

    pub fn handle_rejected_order(&mut self, order: &Order) {
        let initial_price = order.price; // Price initially deducted when placing the order

        // Step 1: Restore the initial price to the capital
        self.capital += initial_price * order.quantity as f64;

        // Step 2: Remove rejected order from broker records
        self.broker_records.retain(|record| record.order_id != order.order_id);

        // println!(
        //     "Client {}: Processed rejected order. Capital: {:.2}, Portfolio: {:?}",
        //     self.id, self.capital, self.portfolio.get_holdings()
        // );
    }

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
