// models.rs

use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PriceUpdate {
    pub name: String,
    pub price: f64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum OrderType {
    MarketBuy,
    MarketSell,
    LimitBuy {
        price: f64,
        take_profit: Option<f64>,
        stop_loss: Option<f64>,
    },
    LimitSell {
        price: f64,
        take_profit: Option<f64>,
        stop_loss: Option<f64>,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Order {
    pub id: u64,
    pub client_id: u64,
    pub stock_symbol: String,
    pub quantity: i64,
    pub order_type: OrderType,
}
