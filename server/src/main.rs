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

#[tokio::main(flavor = "multi_thread", worker_threads = 50)]
async fn main() -> tmq::Result<()> {
    fast_log::init(fast_log::Config::new().console().chan_len(Some(1_000_000)))
        .expect("Failed to init logging");

    let context = tmq::Context::new();
    let router = tmq::router::router(&context).bind("tcp://127.0.0.1:9999")?;
    let (mut tx, mut rx) = router.split::<Multipart>();
    //let (txq, mut rxq) = tokio::sync::mpsc::channel::<Multipart>(1_000_000);
    let (txq, mut rxq) = crossbeam::channel::unbounded::<Multipart>();

    let msg_counter = Arc::new(atomic_counter::RelaxedCounter::new(0));

    let mut store = MemoryStore::new();
    store.add_record("1", "abcd");
    store.add_record("2", "howdy");
    store.add_record("3", "929191");

    let store = Arc::new(store);

    let sender = std::thread::spawn(move || {

        let rt = tokio::runtime::Runtime::new().expect("Unable to create runtime");       
        rt.block_on(async {
            'a: loop {
                let mut count = 0;

                while count < 1_000_000 {
                    match rxq.try_recv() {
                        Ok(msg) => {
                            tx.feed(msg).await.expect("feed failed");
                            count += 1;
                        },
                        Err(crossbeam::channel::TryRecvError::Empty) => break,
                        Err(_) => break 'a,
                    };
                }

                if count > 0 {
                    let start = tokio::time::Instant::now();
                    tx.flush().await.expect("flush failed");
                    let end = tokio::time::Instant::now();
                    log::info!("flushing {} messages {} us", count, (end - start).as_micros());
                }
            }
        })

    });

    let receiver = std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().expect("Unable to make receiver runtime");
        let msg_counter = msg_counter.clone();
        rt.block_on(async move {
            loop {
                if let Some(msg) = rx.next().await {

                }
            }
        })
    });


    let receiver = tokio::spawn({
        let msg_counter = msg_counter.clone();
        async move {
            log::info!("Receiver started");
            loop {

                //let start = tokio::time::Instant::now();
                if let Some(msg) = rx.next().await {
                    //let end = tokio::time::Instant::now();
                    //log::info!("time {}", (end - start).as_micros());

                    match msg {
                        Ok(msg) => {
                            msg_counter.inc();
                            tokio::spawn({
                                let store = store.clone();
                                let txq = txq.clone();
                                async move {
                                    let mut msg = msg;
                                    let a = msg.0[2].as_str().unwrap();

                                    match store.data.get(a) {
                                        Some(value) => {
                                            msg.0[2] = (&value).into();
                                            txq.send(msg)
                                                //.await
                                                .expect("Send to internal txq failed");
                                        },
                                        None => {
                                            msg.0[2] = "No record".into();
                                            txq.send(msg)
                                                //.await
                                                .expect("Send to internal txq failed");
                                        },
                                    };
                                }
                            });
                        }
                        Err(err) => {
                            log::error!("Error receiving message {}", err);
                        }
                    }
                }
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

    sender.join();
    receiver.await;

    Ok(())
}
