// broker/client.rs

use crate::broker::portfolio::Portfolio;
use crate::models::{BrokerOrderRecord, KafkaOrderRequest, OrderAction, OrderStatus, OrderType, PriceUpdate};
use crate::utils;
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
        order_counter: Arc<AtomicU64>,
    ) {
        let stock_data_guard = stock_data.lock().await;

        let mut kafka_order = None;
        let mut price = 0.0;
        
        if let Some(&p) = stock_data_guard.get("APPL") {
            let order_id = format!("Order {}", order_counter.fetch_add(1, Ordering::SeqCst));
            price = p;
            kafka_order = Some(KafkaOrderRequest {
                broker_id,
                client_id: self.id,
                order_id,
                stock_symbol: "APPL".to_string(),
                order_type: OrderType::Market,
                order_action: OrderAction::Buy,
                price,
                quantity: 1,
                status: OrderStatus::Pending,
            });
        }

        if let Some(kafka_order) = kafka_order {
            let broker_record = BrokerOrderRecord {
                order_id: kafka_order.order_id.clone(),
                stop_loss: Some(price * 0.9),   // Example: 10% loss threshold
                take_profit: Some(price * 1.1), // Example: 10% profit target
            };

            println!("Client {} generated order: {:?}", self.id, kafka_order);
            self.pending_orders.push(kafka_order);

            println!("Client {} recorded internal broker order: {:?}", self.id, broker_record);
            self.broker_records.push(broker_record);
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
