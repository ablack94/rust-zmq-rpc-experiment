use std::{ops::Index, sync::Arc};

use atomic_counter::AtomicCounter;
use futures::TryStreamExt;
use futures::{SinkExt, StreamExt};
use num_format::Locale;
use num_format::ToFormattedString;
use tmq::{Message, Multipart};

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
async fn main() {
    console_subscriber::init();

    fast_log::init(fast_log::Config::new().console().chan_len(Some(1_000_000)))
        .expect("Failed to init logging");

    let context = tmq::Context::new();
    context.set_io_threads(2).unwrap();
    log::info!("IO threads {}", context.get_io_threads().unwrap());
    let router = tmq::router::router(&context)
        .bind("tcp://127.0.0.1:9999")
        .expect("Unable to bind router");
    let (mut tx, mut rx) = router.split::<Multipart>();
    let (txq, mut rxq) = tokio::sync::mpsc::unbounded_channel::<Multipart>();

    let msg_counter = Arc::new(atomic_counter::RelaxedCounter::new(0));

    let mut store = MemoryStore::new();
    store.add_record("1", "abcd");
    store.add_record("2", "howdy");
    store.add_record("3", "929191");

    let store = Arc::new(store);

    let receiver = tokio::spawn({
        let msg_counter = msg_counter.clone();
        async move {
            while let Some(Ok(mut msg)) = rx.next().await {
                msg_counter.inc();
                let start = tokio::time::Instant::now();
                match store.data.get(msg.0[1].as_str().unwrap()) {
                    Some(value) => {
                        msg.0[1] = value.into();
                    },
                    None => {
                        msg.0[1] = "".into();
                    }
                };
                tx.send(msg).await.expect("send failed");
                let end = tokio::time::Instant::now();
                //log::info!("delay={} us", (end - start).as_micros());
            }
        }
    });

    std::thread::spawn({
        let msg_counter = msg_counter.clone();
        move || loop {
            let start = std::time::Instant::now();
            std::thread::sleep(std::time::Duration::from_secs(1));
            let end = std::time::Instant::now();
            let seconds = (end - start).as_secs();
            let value = msg_counter.reset() as u64 / seconds;
            log::info!("msgs/s {}", value.to_formatted_string(&Locale::en));
        }
    });

    receiver.await;

}
