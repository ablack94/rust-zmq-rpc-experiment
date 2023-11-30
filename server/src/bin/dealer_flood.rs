use std::collections::HashMap;
use std::{ops::Index, sync::Arc};

use atomic_counter::AtomicCounter;
use futures::TryStreamExt;
use futures::{SinkExt, StreamExt};
use num_format::Locale;
use num_format::ToFormattedString;
use tmq::{Message, Multipart, SocketExt};

struct RespHandle {}

struct State {
    reqs: HashMap<u128, RespHandle>,
}

impl std::default::Default for State {
    fn default() -> Self {
        Self {
            reqs: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
struct Response {
    cid: u128,
    value: String,
}

impl Response {
    pub fn parse_zmq(mut msg: Multipart) -> Option<Response> {
        msg.pop_front()?; // don't care about client_id
        let cid = msg.pop_front()?.as_str()?.parse::<u128>().ok()?;
        let value = msg.pop_front()?.as_str()?.to_string();
        Some(Response { cid, value })
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 1)]
async fn main() {
    //console_subscriber::init();

    fast_log::init(
        fast_log::Config::new()
            .console()
            .chan_len(Some(100_000_000)),
    )
    .expect("Failed to init logging");

    let context = tmq::Context::new();
    let dealer = tmq::dealer(&context)
        .connect("tcp://127.0.0.1:9999")
        .expect("Unable to connect to router");

    let (mut tx, mut rx) = dealer.split::<Multipart>();

    let msg_counter = Arc::new(atomic_counter::RelaxedCounter::new(0));

    // let receiver = tokio::spawn(async move {
    //     while let Some(res) = rx.next().await {
    //         match res {
    //             Ok(msg) => {
    //                 log::info!(
    //                     "recv {:?}",
    //                     msg.into_iter().map(|x| x.as_str().unwrap().to_string())
    //                 );
    //             }
    //             Err(err) => {
    //                 log::error!("receiver error {}", err);
    //             }
    //         };
    //     }
    // });

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

    loop {
        let cur = msg_counter.inc();
        tx.send(vec!["1", &format!("{}", cur)].into())
            .await
            .expect("send failed");
        //tokio::task::yield_now().await;
    }
}
