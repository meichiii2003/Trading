// broker/client.rs

use crate::broker::portfolio::Portfolio;
use crate::models::{BrokerOrderRecord, KafkaOrderRequest, OrderAction, OrderStatus, OrderType, PriceUpdate};
use crate::utils;
use rand::seq::SliceRandom;
use rand::Rng;
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
    pending_orders: Vec<KafkaOrderRequest>,
    broker_records: Vec<BrokerOrderRecord>,
    // Additional fields as needed
}

impl Client {
    pub fn new(id: u64) -> Self {
        let initial_capital = 10_000.0 + rand::thread_rng().gen_range(0.0..10_000.0); // Random initial capital between $10,000 and $20,000
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
            let order_id = format!("Order {}", global_order_counter.fetch_add(1, Ordering::SeqCst));
            let quantity = rng.gen_range(1..=10); // Random quantity between 1 and 100
    
            if is_buy_order {
                // Generate Buy Order
                if is_limit_order {
                    // LimitBuy Order Pricing
                    let limit_price = if rng.gen_bool(0.95) {
                        // 95% of LimitBuy orders below market price
                        market_price * (0.90 + rng.gen_range(0.0..0.05)) // Between 90% and 95% of market price
                    } else {
                        // 5% of LimitBuy orders above market price
                        market_price * (1.05 + rng.gen_range(0.0..0.05)) // Between 105% and 110% of market price
                    };
                    let rounded_limit_price = (limit_price * 100.0).round() / 100.0; // Round to 2 decimal places
    
                    self.pending_orders.push(KafkaOrderRequest {
                        broker_id,
                        client_id: self.id,
                        order_id: order_id.clone(),
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
                    self.pending_orders.push(KafkaOrderRequest {
                        broker_id,
                        client_id: self.id,
                        order_id: order_id.clone(),
                        stock_symbol: stock_symbol.to_string(),
                        order_type: OrderType::Market,
                        order_action: OrderAction::Buy,
                        price: rounded_market_price,
                        quantity,
                        status: OrderStatus::Pending,
                    });
                }
            } else if self.portfolio.get_quantity(stock_symbol) >= quantity {
                // Generate Sell Order (only if holdings are sufficient)
                if is_limit_order {
                    // LimitSell Order Pricing
                    let limit_price = if rng.gen_bool(0.95) {
                        // 95% of LimitBuy orders below market price
                        market_price * (0.90 + rng.gen_range(0.0..0.05)) // Between 90% and 95% of market price
                    } else {
                        // 5% of LimitBuy orders above market price
                        market_price * (1.05 + rng.gen_range(0.0..0.05)) // Between 105% and 110% of market price
                    };
                    let rounded_limit_price = (limit_price * 100.0).round() / 100.0; // Round to 2 decimal places
    
                    self.pending_orders.push(KafkaOrderRequest {
                        broker_id,
                        client_id: self.id,
                        order_id: order_id.clone(),
                        stock_symbol: stock_symbol.to_string(),
                        order_type: OrderType::Limit,
                        order_action: OrderAction::Sell,
                        price: rounded_limit_price,
                        quantity,
                        status: OrderStatus::Pending,
                    });
                } else {
                    // MarketSell Order
                    let rounded_market_price = (market_price * 100.0).round() / 100.0;
                    self.pending_orders.push(KafkaOrderRequest {
                        broker_id,
                        client_id: self.id,
                        order_id: order_id.clone(),
                        stock_symbol: stock_symbol.to_string(),
                        order_type: OrderType::Market,
                        order_action: OrderAction::Sell,
                        price: rounded_market_price,
                        quantity,
                        status: OrderStatus::Pending,
                    });
                }
            }
    
            if let Some(last_order) = self.pending_orders.last() {
                println!(
                    "Client {} generated order: {:?}",
                    self.id, last_order
                );
    
                // Add Broker Records for Buy Orders with Stop-Loss/Take-Profit
                if last_order.order_action == OrderAction::Buy {
                    let rounded_stop_loss = (market_price * 0.9 * 100.0).round() / 100.0;
                    let rounded_take_profit = (market_price * 1.1 * 100.0).round() / 100.0;
                    
                    self.broker_records.push(BrokerOrderRecord {
                        order_id: last_order.order_id.clone(),
                        stop_loss: Some(rounded_stop_loss),   // Example: 10% loss threshold
                        take_profit: Some(rounded_take_profit), // Example: 10% profit target
                    });
                    if let Some(last_record) = self.broker_records.last() {
                        println!(
                            "Client {} recorded internal broker order: BrokerOrderRecord {{ order_id: \"{}\", stop_loss: {:.2}, take_profit: {:.2} }}",
                            self.id,
                            last_record.order_id,
                            last_record.stop_loss.unwrap_or_default(),
                            last_record.take_profit.unwrap_or_default()
                        );
                    }
                    
                }
            }
        }
        
        // // Deduct capital immediately for buy orders
        // let estimated_cost = self.estimate_order_cost(&order).await;
        // if self.capital >= estimated_cost {
        //     self.capital -= estimated_cost;
        //     self.pending_orders.push(order);
        // } else {
        //     // Not enough capital
        // }
    }


    pub fn collect_orders(&mut self) -> Vec<KafkaOrderRequest> {
        let orders = self.pending_orders.clone();
        self.pending_orders.clear();
        orders
    }

    pub fn monitor_broker_orders(
        &mut self,
        stock_data: Arc<Mutex<HashMap<String, f64>>>,
    ) -> Vec<KafkaOrderRequest> {
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
                        new_kafka_orders.push(KafkaOrderRequest {
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
                        new_kafka_orders.push(KafkaOrderRequest {
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

    pub async fn handle_price_update(&mut self, _price_update: &PriceUpdate) {
        // Update portfolio or pending orders based on price updates
        // ...
    }

    async fn estimate_order_cost(&self, order: &KafkaOrderRequest) -> f64 {
        // For simplicity, assume we have access to the latest price
        // In a real application, you'd need to maintain a price cache
        let price_per_share = 100.0; // Placeholder value
        price_per_share * (order.quantity as f64)
    }

    pub fn update_on_execution(&mut self, executed_order: &KafkaOrderRequest, execution_price: f64) {
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
