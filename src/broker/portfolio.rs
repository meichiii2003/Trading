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
    

    // /// Get the quantity of a specific stock.
    // pub fn get_quantity(&self, stock_symbol: &str) -> u64 {
    //     *self.holdings.get(stock_symbol).unwrap_or(&0)
    // }

    // /// Calculate the total value of the portfolio based on current prices.
    // pub fn get_total_value(&self, current_prices: &HashMap<String, f64>) -> f64 {
    //     self.holdings
    //         .iter()
    //         .map(|(symbol, qty)| {
    //             let price = current_prices.get(symbol).cloned().unwrap_or(0.0);
    //             price * (*qty as f64)
    //         })
    //         .sum()
    // }

    // /// Get the holdings as a readable format.
    // pub fn get_holdings(&self) -> &HashMap<String, u64> {
    //     &self.holdings
    // }

    // /// Check if a stock is held in the portfolio.
    // pub fn can_sell(&self, stock_symbol: &str, quantity: u64) -> bool {
    //     self.get_quantity(stock_symbol) >= quantity
    // }

    // // The calculated value of the portfolio based on the current market prices.
    // pub fn calculate_dynamic_value(&self, current_prices: &HashMap<String, f64>) -> f64 {
    //     self.holdings
    //         .iter()
    //         .map(|(symbol, &qty)| {
    //             current_prices
    //                 .get(symbol)
    //                 .map(|&price| price * qty as f64)
    //                 .unwrap_or(0.0)
    //         })
    //         .sum()
    // }

    // pub fn remove_stock(&mut self, stock_symbol: &str) {
    //     self.holdings.remove(stock_symbol);
    // }
}
