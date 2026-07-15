use crate::config::Config;
use crate::record::LogLine;
use std::time::Duration;
use tokio::sync::mpsc::Receiver;
use tokio::time::interval;

async fn flush(client: &reqwest::Client, url: &str, batch: &mut Vec<LogLine>) {
    if batch.is_empty() {
        return;
    }

    match client.post(url).json(batch).send().await {
        Ok(resp) if resp.status().is_success() => {}
        Ok(resp) => eprintln!(
            "logtap: destination responded with status {}",
            resp.status()
        ),
        Err(err) => eprintln!(
            "logtap: failed to send batch ({} items): {err}",
            batch.len()
        ),
    }

    batch.clear();
}
pub async fn run_sink(cfg: Config, mut rx: Receiver<LogLine>) {
    let client = reqwest::Client::new();
    let mut batch: Vec<LogLine> = Vec::with_capacity(cfg.batch_size);
    let mut ticker = interval(Duration::from_secs(cfg.flush_interval_secs));

    loop {
        tokio::select! {
            Some(log) = rx.recv() => {
                batch.push(log);
                if batch.len() >= cfg.batch_size {
                    flush(&client, &cfg.sink_url, &mut batch).await;
                }
            }
            _ = ticker.tick() => {
                if !batch.is_empty() {
                    flush(&client, &cfg.sink_url, &mut batch).await;
                }
            }
        }
    }
}
