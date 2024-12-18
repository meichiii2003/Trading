// // models.rs

use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PriceUpdate {
    pub name: String,
    pub price: f64,
}

#[derive(Serialize, Debug, Deserialize, Clone)]
pub enum OrderType {
    Market,
    Limit,
}

#[derive(Serialize, Debug, Deserialize, Clone, PartialEq)]
pub enum OrderAction {
    Buy,
    Sell,
    Cancel,
}

#[derive(Serialize, Debug, Deserialize, Clone)]
pub enum OrderStatus {
    Pending,
    Completed,
    Rejected,
}

#[derive(Serialize, Debug, Deserialize, Clone)]
pub struct Order {
    pub broker_id: u64,
    pub client_id: u64,
    pub order_id: String,
    pub stock_symbol: String,
    pub order_type: OrderType,
    pub order_action: OrderAction,
    pub price: f64,
    pub quantity: u64,
    pub status: OrderStatus,
}