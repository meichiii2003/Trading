use serde_json::Value;
use std::collections::HashMap;
use std::fs;
use colored::*; // Use colored crate for text colors

const INITIAL_CAPITAL: f64 = 20_000.0;

#[derive(Debug)]
struct ClientPerformance {
    client_id: u64,
    total_investment: f64,
    remaining_capital: f64,
    total_transactions: u64,
    portfolio: HashMap<String, (u64, f64)>, // Stock symbol -> (Quantity, Average Price)
    total_value: f64,
    pnl: f64, // Profit & Loss
}

pub fn generate_client_report(json_file_path: &str) {
    // Step 1: Read the JSON data from the file
    let json_data = fs::read_to_string(json_file_path)
        .expect("Failed to read client data JSON file.");
    let data: Value = serde_json::from_str(&json_data)
        .expect("Failed to parse JSON data.");

    let mut reports: Vec<ClientPerformance> = Vec::new();

    // Step 2: Parse the JSON data
    if let Some(brokers) = data["brokers"].as_array() {
        for broker in brokers {
            if let Some(clients) = broker["clients"].as_array() {
                for client in clients {
                    let client_id = client["client_id"].as_u64().unwrap_or(0);
                    let capital = client["capital"].as_f64().unwrap_or(0.0);
                    let buy_count = client["buy_transaction_count"].as_u64().unwrap_or(0);
                    let sell_count = client["sell_transaction_count"].as_u64().unwrap_or(0);
                    let mut portfolio: HashMap<String, (u64, f64)> = HashMap::new();
                    let mut total_investment = 0.0;

                    // Step 3: Parse the client's portfolio
                    if let Some(portfolio_data) = client["portfolio"].as_object() {
                        for (stock, details) in portfolio_data {
                            let quantity = details["quantity"].as_u64().unwrap_or(0);
                            let avg_price = details["average_price"].as_f64().unwrap_or(0.0);
                            portfolio.insert(stock.clone(), (quantity, avg_price));

                            // Calculate total investment
                            total_investment += quantity as f64 * avg_price;
                        }
                    }

                    let total_value = total_investment + capital;
                    let pnl = total_value - INITIAL_CAPITAL; // P&L calculation

                    // Step 4: Add to the report
                    reports.push(ClientPerformance {
                        client_id,
                        total_investment,
                        remaining_capital: capital,
                        total_transactions: buy_count + sell_count,
                        portfolio,
                        total_value,
                        pnl,
                    });
                }
            }
        }
    }

    // Step 5: Display the report
    println!("========== Client Performance Report ==========");
    for report in reports {
        println!("Client ID: {}", report.client_id);
        println!("  Total Investment: {:.2}", report.total_investment);
        println!("  Remaining Capital: {:.2}", report.remaining_capital);
        println!("  Total Transactions: {}", report.total_transactions);

        println!("  Portfolio:");
        for (stock, (quantity, avg_price)) in &report.portfolio {
            println!(
                "    - {}: {} shares at {:.2} average price",
                stock, quantity, avg_price
            );
        }

        println!("  Total Value (Investment + Capital): {:.2}", report.total_value);

        // Display P&L with color
        if report.pnl >= 0.0 {
            println!(
                "  Profit & Loss (P&L): {}",
                format!("+{:.2}", report.pnl).green()
            );
        } else {
            println!(
                "  Profit & Loss (P&L): {}",
                format!("{:.2}", report.pnl).red()
            );
        }

        println!("----------------------------------------------");
    }
}
