// // models.rs

use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PriceUpdate {
    pub name: String,
    pub price: f64,
}

// #[derive(Serialize, Deserialize, Debug, Clone)]
// pub enum OrderType {
//     MarketBuy{
//         price: f64,
//         take_profit: Option<f64>,
//         stop_loss: Option<f64>,
//     },
//     MarketSell{
//         price: f64,
//         take_profit: Option<f64>,
//         stop_loss: Option<f64>,
//     },
//     LimitBuy {
//         price: f64,
//         take_profit: Option<f64>,
//         stop_loss: Option<f64>,
//     },
//     LimitSell {
//         price: f64,
//         take_profit: Option<f64>,
//         stop_loss: Option<f64>,
//     },
// }

// #[derive(Serialize, Deserialize, Debug, Clone)]
// pub struct Order {
//     pub order_id: u64,
//     pub client_id: u64,
//     pub stock_symbol: String,
//     pub quantity: i64,
//     pub order_type: OrderType,
// }

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

// #[derive(Serialize, Debug, Deserialize, Clone)]
// pub struct BrokerOrderRecord {
//     pub client_id: u64,
//     pub order_id: String,
//     pub stop_loss: Option<f64>,
//     pub take_profit: Option<f64>,
// }
