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

// How often the watch loop wakes up even without a filesystem event, just to
// check whether the downstream receiver is gone, and whether the file at
// source_path has been swapped out from under us (log rotation). Without
// this, the loop can block forever on `notify_rx.recv()` once there's no
// more filesystem activity on the file it's actually still holding open.
const SHUTDOWN_POLL_INTERVAL: Duration = Duration::from_millis(200);

/// Whether the caller should keep going or the downstream receiver is gone.
enum DrainOutcome {
    Continue,
    ReceiverGone,
}

/// Reads every complete line available from `offset` onward and forwards it
/// down the channel, advancing `offset` as it goes.
fn drain_new_lines(
    reader: &mut BufReader<File>,
    offset: &mut u64,
    tx: &Sender<String>,
    line: &mut String,
) -> Result<DrainOutcome> {
    reader.seek(SeekFrom::Start(*offset))?;

    loop {
        line.clear();
        let bytes_read = reader.read_line(line)?;

        if bytes_read == 0 {
            return Ok(DrainOutcome::Continue);
        }

        *offset += bytes_read as u64;
        let trimmed = line.trim_end().to_string();

        if tx.blocking_send(trimmed).is_err() {
            return Ok(DrainOutcome::ReceiverGone);
        }
    }
}

/// Creates a filesystem watcher for the given path.
///
/// The watcher sends filesystem events through the provided channel.
/// Only events from the target file itself are monitored.
fn watch(
    path: &Path,
    notify_tx: std_mpsc::Sender<notify::Result<Event>>,
) -> Result<RecommendedWatcher> {
    let mut watcher: RecommendedWatcher = notify::recommended_watcher(move |res| {
        let _ = notify_tx.send(res);
    })?;
    watcher.watch(path, RecursiveMode::NonRecursive)?;
    Ok(watcher)
}

/// Starts watching a source file and forwards newly appended lines to the
/// provided channel.
///
/// The function begins reading from the current end of the file and waits for
/// filesystem modification events to detect new content.
///
/// When the file is replaced during log rotation, the old file is drained,
/// the new file is opened, and reading continues from the beginning of the
/// replacement file.
///
/// Processing stops when the downstream receiver is closed.
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
