use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};

#[derive(Serialize, Deserialize)]
struct ClientData {
    client_id: u64,
    portfolio: HashMap<String, u64>,
    buy_transaction_count: u64,  // Count of completed buy transactions
    sell_transaction_count: u64, // Count of completed sell transactions
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

    println!("JSON file reset with empty portfolios and transaction counts.");
}

pub async fn update_client_portfolio_in_json(
    file_path: &str,
    client_id: u64,
    stock_symbol: String,
    quantity: u64,
    is_buy: bool,
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

    // Update the client portfolio and transaction counts
    let mut updated = false;
    for broker in data.brokers.iter_mut() {
        for client in broker.clients.iter_mut() {
            if client.client_id == client_id {
                if is_buy {
                    *client.portfolio.entry(stock_symbol.clone()).or_insert(0) += quantity;
                    client.buy_transaction_count += 1; // Increment buy transaction count
                } else {
                    let current_qty = client.portfolio.entry(stock_symbol.clone()).or_insert(0);
                    if *current_qty >= quantity {
                        *current_qty -= quantity;
                        client.sell_transaction_count += 1; // Increment sell transaction count
                    } else {
                        println!(
                            "Error: Client {} has insufficient shares of {} to sell.",
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
    // else {
    //     println!("Successfully updated JSON for client ID {}", client_id);
    // }
}
