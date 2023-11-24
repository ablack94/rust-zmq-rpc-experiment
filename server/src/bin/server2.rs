use std::{ops::Index, sync::Arc};

use atomic_counter::AtomicCounter;
use futures::{SinkExt, StreamExt};
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

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> tmq::Result<()> {
    fast_log::init(fast_log::Config::new().console().chan_len(Some(1_000_000)))
        .expect("Failed to init logging");

    let mut store = MemoryStore::new();
    store.add_record("1", "abcd");
    store.add_record("2", "howdy");
    store.add_record("3", "929191");

    let ctx = Arc::new(zmq::Context::new());

    let (txq, mut rxq) = crossbeam::channel::unbounded::<Multipart>();

    let msg_counter = Arc::new(atomic_counter::RelaxedCounter::new(0));

    let mut workers = Vec::new();

    for _ in 0..5 {
        let worker = std::thread::spawn({
            let ctx = ctx.clone();
            let msg_counter = msg_counter.clone();
            move || {
                let socket = Arc::new(
                    ctx.socket(zmq::ROUTER)
                        .expect("Unable to create router socket"),
                );
                socket
                    .bind("tcp://127.0.0.1:9999")
                    .expect("Unable to bind to local port");
                let mut msgs = (
                    zmq::Message::with_size(1024),
                    zmq::Message::with_size(1024),
                    zmq::Message::with_size(1024),
                );
                let mut lens = (0, 0, 0);
                let mut dummy_msg = zmq::Message::new();
                loop {
                    /*
                     * Receive
                     */
                    // Get client ID
                    lens.0 = socket.recv_into(&mut msgs.0, 0).expect("recv failed");
                    if !socket.get_rcvmore().expect("Unable to check rcvmore flag") {
                        continue;
                    }
                    // Get blank envelope message
                    lens.1 = socket.recv_into(&mut msgs.1, 0).expect("recv failed");
                    if !socket.get_rcvmore().expect("Unable to check rcvmore flag") {
                        continue;
                    }
                    // Get payload
                    lens.2 = socket.recv_into(&mut msgs.2, 0).expect("recv failed");
                    // Consume any straggler message parts, we don't care about them
                    while socket.get_rcvmore().expect("Unable to check rcvmore flag") {
                        socket.recv_into(&mut dummy_msg, 0).expect("recv failed");
                    }

                    /*
                     * Process
                     */
                    msg_counter.inc();
                    /*
                     * Send response
                     */
                    socket
                        .send(&msgs.0[0..lens.0], zmq::SNDMORE)
                        .expect("send failed");
                    socket
                        .send(&msgs.1[0..lens.1], zmq::SNDMORE)
                        .expect("send failed");
                    socket.send(&msgs.2[0..lens.2], 0).expect("send failed");
                }
            }
        });
        workers.push(worker);
    }

    std::thread::spawn({
        let msg_counter = msg_counter.clone();
        move || loop {
            let start = std::time::Instant::now();
            std::thread::sleep(std::time::Duration::from_secs(1));
            let end = std::time::Instant::now();
            let seconds = (end - start).as_secs();
            let value = msg_counter.reset() as u64 / seconds;
            log::info!("msgs/s {}", value);
        }
    });

    for worker in workers {
        worker.join();
    }

    Ok(())
}
