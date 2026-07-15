use std::fs;
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use notify::{EventKind, RecursiveMode, Watcher};

#[test]
fn notify_detects_file_modification() {
    let file_name = format!(
        "notify_test_{}.log",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );

    let path = PathBuf::from(file_name);

    fs::write(&path, "").unwrap();

    let (tx, rx) = std::sync::mpsc::channel();

    let mut watcher = notify::recommended_watcher(move |result| {
        tx.send(result).unwrap();
    })
        .unwrap();

    watcher.watch(&path, RecursiveMode::NonRecursive).unwrap();

    fs::write(&path, "changed line\n").unwrap();

    let timeout = Duration::from_secs(3);
    let deadline = Instant::now() + timeout;
    let mut received_kinds = Vec::new();
    let mut saw_modify = false;

    while Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(Instant::now());

        let event = rx
            .recv_timeout(remaining)
            .expect("timeout: notify did not detect file modification")
            .expect("notify returned error");

        received_kinds.push(event.kind.clone());

        if matches!(event.kind, EventKind::Modify(_)) {
            saw_modify = true;
            break;
        }
    }

    fs::remove_file(&path).ok();

    assert!(
        saw_modify,
        "expected Modify, but received events were: {:?}",
        received_kinds
    );
}