use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Write};

#[derive(Serialize, Deserialize)]
struct ClientData {
    client_id: u64,
    portfolio: HashMap<String, StockHolding>, // Stock symbol to StockHolding mapping
    buy_transaction_count: u64,  // Count of completed buy transactions
    sell_transaction_count: u64, // Count of completed sell transactions
    capital: f64,
}
#[derive(Serialize, Deserialize)]
struct StockHolding {
    quantity: u64,
    average_price: f64,
}



#[derive(Serialize, Deserialize)]
struct BrokerData {
    broker_id: u64,
    clients: Vec<ClientData>,
}

#[derive(Serialize, Deserialize)]
struct BrokersData {
    brokers: Vec<BrokerData>,
}

pub fn reset_client_holdings_json(file_path: &str, total_brokers: u64, clients_per_broker: u64) {
    let initial_capital = 10_000.0; // Default initial capital
    let mut brokers_data = Vec::new();

    for broker_id in 1..=total_brokers {
        let mut clients = Vec::new();
        for client_id in ((broker_id - 1) * clients_per_broker + 1)
            ..=(broker_id * clients_per_broker)
        {
            clients.push(ClientData {
                client_id,
                portfolio: HashMap::new(),
                buy_transaction_count: 0,
                sell_transaction_count: 0,
                capital: initial_capital,
            });
        }
        brokers_data.push(BrokerData {
            broker_id,
            clients,
        });
    }

    let data = BrokersData { brokers: brokers_data };

    let json_data = serde_json::to_string_pretty(&data).expect("Failed to serialize data to JSON");

    let mut file = File::create(file_path).expect("Failed to create/reset JSON file");
    file.write_all(json_data.as_bytes())
        .expect("Failed to write to JSON file");

    //println!("JSON file reset with empty portfolios, transaction counts, and initial capital.");
}


pub async fn update_client_portfolio_in_json(
    file_path: &str,
    client_id: u64,
    stock_symbol: String,
    quantity: u64,
    is_buy: bool,
    price_per_unit: f64,
) {
    // Read the existing JSON file
    let mut file = match File::open(file_path) {
        Ok(f) => f,
        Err(e) => {
            println!("Error opening JSON file: {}", e);
            return;
        }
    };

    let mut json_data = String::new();
    if let Err(e) = file.read_to_string(&mut json_data) {
        println!("Error reading JSON file: {}", e);
        return;
    }

    // Parse the JSON data
    let mut data: BrokersData = match serde_json::from_str(&json_data) {
        Ok(d) => d,
        Err(e) => {
            println!("Error parsing JSON data: {}", e);
            return;
        }
    };

    // Update the client portfolio and capital
    let mut updated = false;
    for broker in data.brokers.iter_mut() {
        for client in broker.clients.iter_mut() {
            if client.client_id == client_id {
                if is_buy {
                    // Update portfolio for Buy
                    let holding = client.portfolio.entry(stock_symbol.clone()).or_insert(StockHolding {
                        quantity: 0,
                        average_price: 0.0,
                    });

                    let total_cost = quantity as f64 * price_per_unit;

                    // Calculate the new average price
                    holding.average_price = ((holding.average_price * holding.quantity as f64) + total_cost)
                        / (holding.quantity + quantity) as f64;
                    holding.quantity += quantity;

                    // Deduct capital
                    client.capital -= total_cost;
                    client.buy_transaction_count += 1; // Increment buy transaction count
                } else {
                    // Update portfolio for Sell
                    if let Some(holding) = client.portfolio.get_mut(&stock_symbol) {
                        if holding.quantity >= quantity {
                            holding.quantity -= quantity;

                            // Add capital
                            let total_revenue = quantity as f64 * price_per_unit;
                            client.capital += total_revenue;
                            client.sell_transaction_count += 1; // Increment sell transaction count

                            // Remove stock entry if quantity becomes zero
                            if holding.quantity == 0 {
                                client.portfolio.remove(&stock_symbol);
                            }
                        } else {
                            println!(
                                "Error: Client {} has insufficient shares of {} to sell.",
                                client_id, stock_symbol
                            );
                            return;
                        }
                    } else {
                        println!(
                            "Error: Client {} does not own any shares of {} to sell.",
                            client_id, stock_symbol
                        );
                        return;
                    }
                }
                updated = true;
                break;
            }
        }
        if updated {
            break;
        }
    }

    if !updated {
        println!("Client ID {} not found in JSON data.", client_id);
        return;
    }

    // Write the updated JSON back to the file
    let updated_json = match serde_json::to_string_pretty(&data) {
        Ok(json) => json,
        Err(e) => {
            println!("Error serializing updated JSON data: {}", e);
            return;
        }
    };

    if let Err(e) = File::create(file_path).and_then(|mut f| f.write_all(updated_json.as_bytes())) {
        println!("Error writing updated JSON data: {}", e);
    } 
}



