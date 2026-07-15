use crate::config::Config;
use anyhow::Result;
use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use std::fs::File;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::sync::mpsc as std_mpsc;
use std::time::Duration;
use tokio::sync::mpsc::Sender;

// How often the watch loop wakes up even without a filesystem event, just to
// check whether the downstream receiver is gone. Without this, the loop can
// block forever on `notify_rx.recv()` after the rest of the pipeline shut
// down, since there is no more filesystem activity left to notice it.
const SHUTDOWN_POLL_INTERVAL: Duration = Duration::from_millis(200);

pub fn run_source(cfg: Config, tx: Sender<String>) -> Result<()> {
    let file = File::open(&cfg.source_path)?;
    let mut reader = BufReader::new(file);
    let mut offset = reader.seek(SeekFrom::End(0))?;

    let (notify_tx, notify_rx) = std_mpsc::channel::<notify::Result<Event>>();
    let mut watcher: RecommendedWatcher = notify::recommended_watcher(move |res| {
        let _ = notify_tx.send(res);
    })?;
    watcher.watch(&cfg.source_path, RecursiveMode::NonRecursive)?;

    let mut line = String::new();

    loop {
        let event = match notify_rx.recv_timeout(SHUTDOWN_POLL_INTERVAL) {
            Ok(res) => res?,
            Err(std_mpsc::RecvTimeoutError::Timeout) => {
                if tx.is_closed() {
                    return Ok(());
                }
                continue;
            }
            Err(std_mpsc::RecvTimeoutError::Disconnected) => return Ok(()),
        };

        if !matches!(event.kind, EventKind::Modify(_)) {
            continue;
        }

        reader.seek(SeekFrom::Start(offset))?;

        loop {
            line.clear();
            let bytes_read = reader.read_line(&mut line)?;

            if bytes_read == 0 {
                break;
            }

            offset += bytes_read as u64;
            let trimmed = line.trim_end().to_string();

            if tx.blocking_send(trimmed).is_err() {
                return Ok(());
            }
        }
    }
}
