use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::Mutex;

#[derive(Serialize, Deserialize, Debug)]
struct Database {
    store: HashMap<String, String>,
}

impl Database {
    fn new() -> Self {
        Database {
            store: HashMap::new(),
        }
    }

    fn insert(&mut self, key: String, value: String) {
        self.store.insert(key, value);
    }

    fn update(&mut self, key: String, value: String) {
        self.store.insert(key, value);
    }

    fn get(&self, key: &String) -> Option<String> {
        self.store.get(key).cloned()
    }
}

async fn handle_client(stream: TcpStream, db: Arc<Mutex<Database>>) { 
    let mut reader = BufReader::new(stream); 
    let mut line = String::new(); 
    loop {
        line.clear();

        match reader.read_line(&mut line).await {
            Ok(0) => break, 
            Ok(_) => {
                let command: Vec<&str> = line.trim().split_whitespace().collect();
                let response;

                match command.as_slice() {
                    ["INSERT", key, value] => {
                        let mut db = db.lock().await;
                        db.insert(key.to_string(), value.to_string());
                        response = json!({ "status": "success", "action": "inserted" });
                    }
                    ["UPDATE", key, value] => {
                        let mut db = db.lock().await;
                        db.update(key.to_string(), value.to_string());
                        response = json!({ "status": "success", "action": "updated" });
                    }
                    ["GET", key] => {
                        let db = db.lock().await;
                        if let Some(value) = db.get(&key.to_string()) {
                            response = json!({ "status": "success", "value": value });
                        } else {
                            response = json!({ "status": "error", "message": "key not found" });
                        }
                    }
                    _ => {
                        response = json!({ "status": "error", "message": "invalid command" });
                    }
                }

                if let Err(e) = reader.get_mut().write_all(response.to_string().as_bytes()).await {
                    eprintln!("Failed to send response: {}", e);
                    break;
                }
            }
            Err(e) => {
                eprintln!("Failed to read from stream: {}", e);
                break;
            }
        }
    }
}

#[tokio::main]
async fn main() {
    let db = Arc::new(Mutex::new(Database::new()));
    let listener = TcpListener::bind("0.0.0.0:8080").await.unwrap();

    println!("Server running on port 8080...");

    loop {
        let (stream, _) = listener.accept().await.unwrap();
        let db = Arc::clone(&db);
        tokio::spawn(async move {
            handle_client(stream, db).await;
        });
    }
}
