use std::ops::Index;

use futures::{SinkExt, StreamExt};
use tmq::{Multipart, Message};

#[derive(thiserror::Error, Debug)]
pub enum LookupError {
    #[error("Record for key `{0}` does not exist")]
    NotFound(String),
    #[error("Unknown data store error")]
    Unknown,
}

#[async_trait::async_trait]
trait DataStore {
    async fn lookup(&self, name: &str) -> Result<String, LookupError>;
}

struct MemoryStore {
    data: std::collections::HashMap<String, String>,
}

impl std::default::Default for MemoryStore {
    fn default() -> Self {
        Self {
            data: Default::default(),
        }
    }
}

impl MemoryStore {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn add_record(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.data.insert(key.into(), value.into());
    }
}

#[async_trait::async_trait]
impl DataStore for MemoryStore {
    async fn lookup(&self, name: &str) -> Result<String, LookupError> {
        if let Some(value) = self.data.get(name) {
            Ok(value.clone())
        } else {
            Err(LookupError::NotFound(name.to_string()))
        }
    }
}

#[derive(Debug, Clone)]
struct RequestDescription {
    client_id: Vec<u8>,
    key: String,
}

impl RequestDescription {
    pub fn new(client_id: impl Into<Vec<u8>>, key: impl Into<String>) -> Self {
        Self {
            client_id: client_id.into(),
            key: key.into(),
        }
    }
}

impl std::fmt::Display for RequestDescription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "RequestDescription client_id={:?} key={}",
            self.client_id, self.key
        ))
    }
}

fn parse_request_from_zmq(msg: &Multipart) -> Option<RequestDescription> {
    if msg.len() == 3 {
        let client_id = msg.0.index(0);
        let key = msg.0.index(2);
        Some(RequestDescription {
            client_id: client_id.to_vec(),
            key: key.as_str()?.to_string(),
        })
    } else {
        None
    }
}

#[tokio::main(flavor="multi_thread", worker_threads=1)]
async fn main() -> tmq::Result<()> {
    fast_log::init(fast_log::Config::new().console().chan_len(Some(1_000_000)))
        .expect("Failed to init logging");

    let context = tmq::Context::new();
    let router = tmq::router::router(&context).bind("tcp://127.0.0.1:9999")?;
    let (mut tx, mut rx) = router.split::<Multipart>();
    let (txq, mut rxq) = tokio::sync::mpsc::channel::<Multipart>(1_000_000);

    let mut store = MemoryStore::new();
    store.add_record("1", "abcd");
    store.add_record("2", "howdy");
    store.add_record("3", "929191");

    let sender = tokio::spawn(async move {
        log::info!("Internal sender started");
        while let Some(msg) = rxq.recv().await {
            match tx.send(msg).await {
                Ok(_) => {}
                Err(err) => {
                    log::error!("Error sending message to client {}", err);
                }
            };
        }
        log::info!("Internal sender terminated");

    });

    let receiver = tokio::spawn(async move {
        log::info!("Receiver started");
        while let Some(msg) = rx.next().await {
            match msg {
                Ok(msg) => {
                    // Get the value to lookup
                    if let Some(req) = parse_request_from_zmq(&msg) {
                        log::info!("Got request {}", &req);
                        let mut response = Multipart::default();
                        response.push_back(Message::from(&req.client_id));
                        response.push_back(Message::new());
                        // Do we have a record?
                        match store.lookup(&req.key).await {
                            Ok(value) => {
                                log::info!("Record found, value=`{}` for request={}", &value, &req);
                                response.push_back(Message::from(value.as_bytes()));
                            }
                            Err(LookupError::NotFound(key)) => {
                                log::info!("No record for key={}", key);
                                response.push_back(Message::from([b'\x03'].to_vec()));
                            }
                            Err(_) => {
                                log::error!("Unknown error during processing!");
                            }
                        };
                        // Send response
                        txq.send(response).await.expect("Unable to publish to internal send queue");
                    }
                }
                Err(err) => {
                    log::error!("Error receiving message {}", err);
                }
            }
        }
        log::info!("Receiver terminated");
    });

    sender.await;
    receiver.await;

    Ok(())
}
