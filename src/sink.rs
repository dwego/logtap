use crate::config::Config;
use crate::record::LogLine;
use std::fs::OpenOptions;
use std::io::Write;
use std::time::Duration;
use tokio::sync::mpsc::Receiver;
use tokio::time::{interval, sleep};

const DEAD_LETTER_PATH: &str = "logtap.failed.jsonl";

pub async fn run_sink(cfg: Config, mut rx: Receiver<LogLine>) {
    let client = reqwest::Client::new();
    let mut batch: Vec<LogLine> = Vec::with_capacity(cfg.batch_size);
    let mut ticker = interval(Duration::from_secs(cfg.flush_interval_secs));

    loop {
        tokio::select! {
            Some(log) = rx.recv() => {
                batch.push(log);
                if batch.len() >= cfg.batch_size {
                    flush_with_retry(&client, &cfg, &mut batch).await;
                }
            }
            _ = ticker.tick() => {
                if !batch.is_empty() {
                    flush_with_retry(&client, &cfg, &mut batch).await;
                }
            }
        }
    }
}

async fn flush_with_retry(client: &reqwest::Client, cfg: &Config, batch: &mut Vec<LogLine>) {
    if batch.is_empty() {
        return;
    }

    let mut attempt: u32 = 0;

    loop {
        match client.post(&cfg.sink_url).json(batch).send().await {
            Ok(resp) if resp.status().is_success() => {
                batch.clear();
                return;
            }
            Ok(resp) => {
                eprintln!(
                    "logtap: attempt {}/{} failed — server responded with status {}",
                    attempt + 1,
                    cfg.max_retries,
                    resp.status()
                );
            }
            Err(err) => {
                eprintln!(
                    "logtap: attempt {}/{} failed — error sending batch ({} items): {err}",
                    attempt + 1,
                    cfg.max_retries,
                    batch.len()
                );
            }
        }

        attempt += 1;

        if attempt >= cfg.max_retries {
            eprintln!(
                "logtap: abandoning batch of {} items after {} attempts — writing to {DEAD_LETTER_PATH}",
                batch.len(),
                cfg.max_retries
            );

            write_dead_letter(batch);
            batch.clear();
            return;
        }

        let backoff_ms = cfg
            .retry_backoff_initial_ms
            .saturating_mul(1u64 << (attempt - 1));
        let backoff =
            Duration::from_millis(backoff_ms).min(Duration::from_secs(cfg.retry_backoff_max_secs));

        eprintln!("logtap: waiting {backoff:?} before next attempt");
        sleep(backoff).await;
    }
}

// Reopens the file on every call instead of keeping a long-lived handle —
// a handle stays bound to the underlying inode even if the file is later
// renamed out from under it (e.g. for a manual replay), which would make
// new failures silently land in the renamed-away file.
fn write_dead_letter(batch: &[LogLine]) {
    let mut file = match OpenOptions::new()
        .create(true)
        .append(true)
        .open(DEAD_LETTER_PATH)
    {
        Ok(file) => file,
        Err(err) => {
            eprintln!(
                "logtap: could not open {DEAD_LETTER_PATH} ({err}) — {} item(s) lost for good",
                batch.len()
            );
            return;
        }
    };

    for log in batch {
        if let Err(err) = writeln!(file, "{log}") {
            eprintln!("logtap: failed writing to {DEAD_LETTER_PATH}: {err}");
            return;
        }
    }
}
