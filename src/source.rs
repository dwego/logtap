use crate::config::Config;
use anyhow::Result;
use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use std::fs::File;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::sync::mpsc as std_mpsc;
use std::time::Duration;
use tokio::sync::mpsc::Sender;

/// Interval used to periodically check whether the output channel is closed.
///
/// Without this timeout, the watcher loop could block forever waiting for a
/// filesystem event after the rest of the pipeline has already shut down.
const SHUTDOWN_POLL_INTERVAL: Duration = Duration::from_millis(200);

/// Reads new lines from a file as it changes and sends them to the pipeline.
///
/// The source starts reading from the current end of the file and only
/// processes lines appended after startup.
///
/// The function watches the file for modifications and stops when the
/// receiver side is closed.
pub fn run_source(cfg: Config, tx: Sender<String>) -> Result<()> {
    let file = File::open(&cfg.source_path)?;
    let mut inode = file.metadata()?.ino();
    let mut reader = BufReader::new(file);
    let mut offset = reader.seek(SeekFrom::End(0))?;

    let (notify_tx, notify_rx) = std_mpsc::channel::<notify::Result<Event>>();
    let mut _watcher = watch(&cfg.source_path, notify_tx.clone())?;

    let mut line = String::new();

    loop {
        let event = match notify_rx.recv_timeout(SHUTDOWN_POLL_INTERVAL) {
            Ok(res) => res?,
            Err(std_mpsc::RecvTimeoutError::Timeout) => {
                if tx.is_closed() {
                    return Ok(());
                }

                // The path may briefly not resolve mid-rotation (rename in
                // progress) — that's transient, just retry on the next tick.
                let Ok(current_inode) = std::fs::metadata(&cfg.source_path).map(|m| m.ino()) else {
                    continue;
                };

                if current_inode == inode {
                    continue;
                }

                // The file at source_path is no longer the one we have open
                // (standard logrotate "rename + create new" behavior). Flush
                // whatever was written to the old file right before the
                // swap, then switch to the new one from the start.
                if let DrainOutcome::ReceiverGone =
                    drain_new_lines(&mut reader, &mut offset, &tx, &mut line)?
                {
                    return Ok(());
                }

                let new_file = File::open(&cfg.source_path)?;
                inode = new_file.metadata()?.ino();
                reader = BufReader::new(new_file);
                offset = 0;
                _watcher = watch(&cfg.source_path, notify_tx.clone())?;

                continue;
            }
            Err(std_mpsc::RecvTimeoutError::Disconnected) => return Ok(()),
        };

        if !matches!(event.kind, EventKind::Modify(_)) {
            continue;
        }

        if let DrainOutcome::ReceiverGone =
            drain_new_lines(&mut reader, &mut offset, &tx, &mut line)?
        {
            return Ok(());
        }
    }
}
