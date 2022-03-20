use futures::{stream, StreamExt};
use reqwest::Client;
use std::{env, fs::write, time::SystemTime};

fn now() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();

    let sym = args[1].to_string();
    let intrvl_sec = args[2].parse::<u64>().unwrap();
    let concurrent_reqs = args[3].parse::<usize>().unwrap();

    let end = 1313668800;

    let mut timestamps = Vec::new();
    let mut ts = now();
    while ts > end {
        ts -= intrvl_sec * 1000;
        timestamps.push(ts);
    }

    let client = Client::new();
    stream::iter(timestamps)
        .map(|ts| {
            let client = client.clone();
            let base_url = format!("https://www.bitstamp.net/api/v2/ohlc/{}", sym);
            tokio::spawn(async move {
                let query = format!("limit=1000&step={}&end={}", intrvl_sec, ts);
                let url = format!("{}?{}", base_url, query);
                println!("GET {}", url);

                let resp = client.get(url).send().await;
                let file = format!("data/{}.json", ts);
                (file, resp)
            })
        })
        .buffer_unordered(concurrent_reqs)
        .for_each(|thread_result| async {
            match thread_result {
                Ok((file, resp)) => match resp {
                    Ok(data) => write(file, data.bytes().await.unwrap()).unwrap(),
                    Err(e) => eprintln!("Error for file {}: {}", file, e),
                },
                Err(e) => {
                    eprintln!("Tokio error: {}", e);
                }
            }
        })
        .await;
}
