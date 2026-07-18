use crate::config::Config;
use crate::record::LogLine;
use std::fs;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::time::Duration;
use tokio::sync::mpsc::Receiver;
use tokio::time::{interval, sleep};

/// Path used to store events that could not be delivered.
const DEAD_LETTER_PATH: &str = "logtap.failed.jsonl";

/// Runs the sink pipeline.
///
/// Receives log entries from a channel, groups them into batches, and sends
/// them to the configured destination. Failed batches are handled by the
/// retry/dead-letter mechanism.
///
/// The sink flushes data when either:
/// - the configured batch size is reached;
/// - the flush interval expires.
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

            write_dead_letter(cfg, batch);
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
fn write_dead_letter(cfg: &Config, batch: &[LogLine]) {
    rotate_dead_letter_if_full(cfg);

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

fn rotated_dead_letter_path(n: u64) -> PathBuf {
    PathBuf::from(format!("{DEAD_LETTER_PATH}.{n}"))
}

// Caps how big logtap.failed.jsonl is allowed to get. Same idea as
// logrotate: once the current file crosses dead_letter_max_bytes, it's
// shifted into .1, the old .1 becomes .2, and so on up to
// dead_letter_max_files — at which point the oldest file is discarded to
// make room. That eviction is real, permanent data loss, so it's always
// logged loudly rather than happening quietly.
fn rotate_dead_letter_if_full(cfg: &Config) {
    let current_size = match fs::metadata(DEAD_LETTER_PATH) {
        Ok(meta) => meta.len(),
        Err(_) => return, // no file yet — nothing to rotate
    };

    if current_size < cfg.dead_letter_max_bytes as u64 {
        return;
    }

    if cfg.dead_letter_max_files == 0 {
        if let Err(err) = fs::remove_file(DEAD_LETTER_PATH) {
            eprintln!("logtap: failed to reset full dead-letter file: {err}");
        } else {
            eprintln!(
                "logtap: dead-letter file hit {current_size} bytes and dead_letter_max_files is 0 — discarding it entirely"
            );
        }
        return;
    }

    let oldest = rotated_dead_letter_path(cfg.dead_letter_max_files);
    if oldest.exists() {
        eprintln!(
            "logtap: dead-letter rotation limit ({} files) reached — discarding oldest file {}",
            cfg.dead_letter_max_files,
            oldest.display()
        );
    }

    for n in (1..cfg.dead_letter_max_files).rev() {
        let from = rotated_dead_letter_path(n);
        if from.exists() {
            let to = rotated_dead_letter_path(n + 1);
            if let Err(err) = fs::rename(&from, &to) {
                eprintln!(
                    "logtap: failed to rotate {} -> {}: {err}",
                    from.display(),
                    to.display()
                );
            }
        }
    }

    if let Err(err) = fs::rename(DEAD_LETTER_PATH, rotated_dead_letter_path(1)) {
        eprintln!("logtap: failed to rotate {DEAD_LETTER_PATH}: {err}");
    }
}
