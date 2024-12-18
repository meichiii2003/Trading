// broker/portfolio.rs

use std::collections::HashMap;

pub struct Portfolio {
    holdings: HashMap<String, u64>, // stock_symbol -> quantity
}

impl Portfolio {
    pub fn new() -> Self {
        Self {
            holdings: HashMap::new(),
        }
    }
    
    /// Update the holdings when a buy or sell order is executed.
    pub fn update_holdings(&mut self, stock_symbol: &str, quantity_change: i64) {
        let entry = self.holdings.entry(stock_symbol.to_string()).or_insert(0);
        if quantity_change < 0 {
            // Selling shares
            let qty_to_remove = (-quantity_change) as u64;
            if *entry >= qty_to_remove {
                *entry -= qty_to_remove;
            } else {
                println!(
                    "Attempted to sell more shares than owned for {}. Setting holdings to 0.",
                    stock_symbol
                );
                *entry = 0; // Prevent negative holdings
            }
        } else {
            // Buying shares
            *entry += quantity_change as u64;
        }
    }
}
