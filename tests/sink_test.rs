use std::path::PathBuf;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::Mutex;

use logtap::config::Config;
use logtap::record::LogLine;
use logtap::sink::run_sink;

// The dead-letter path is a fixed constant in sink.rs, not a Config field,
// so every test that exercises it shares the same file on disk. cargo test
// runs tests in the same binary concurrently by default — this lock keeps
// the dead-letter tests from stepping on each other's file. Needs to be a
// tokio Mutex (not std) since the guard is held across await points.
static DEAD_LETTER_TEST_LOCK: Mutex<()> = Mutex::const_new(());

async fn read_http_request_body(stream: &mut tokio::net::TcpStream) -> String {
    let mut buffer = Vec::new();
    let mut temp = [0_u8; 1024];

    loop {
        let read = stream.read(&mut temp).await.unwrap();
        assert!(read > 0, "connection closed before headers were read");

        buffer.extend_from_slice(&temp[..read]);

        if buffer.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
    }

    let headers_end = buffer
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .unwrap()
        + 4;

    let headers = String::from_utf8_lossy(&buffer[..headers_end]);
    let content_length = headers
        .lines()
        .find_map(|line| {
            line.to_ascii_lowercase()
                .strip_prefix("content-length:")
                .map(|value| value.trim().parse::<usize>().unwrap())
        })
        .unwrap();

    while buffer.len() < headers_end + content_length {
        let read = stream.read(&mut temp).await.unwrap();
        assert!(read > 0, "connection closed before body was read");

        buffer.extend_from_slice(&temp[..read]);
    }

    String::from_utf8(buffer[headers_end..headers_end + content_length].to_vec()).unwrap()
}

#[tokio::test]
async fn sink_posts_logs_when_batch_size_is_reached() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();

        let body = read_http_request_body(&mut stream).await;

        stream
            .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\nconnection: close\r\n\r\n")
            .await
            .unwrap();

        body
    });

    let cfg = Config {
        source_path: PathBuf::from("unused.log"),
        sink_url: format!("http://{address}/logs"),
        batch_size: 2,
        flush_interval_secs: 60,
        channel_capacity: 10,
        max_retries: 3,
        retry_backoff_initial_ms: 100,
        retry_backoff_max_secs: 5,
        filter_rules: vec![],
        mask_common_patterns: false,
        dead_letter_max_bytes: 0,
        dead_letter_max_files: 0,
    };

    let (tx, rx) = tokio::sync::mpsc::channel::<LogLine>(cfg.channel_capacity);

    let sink = tokio::spawn(run_sink(cfg, rx));

    tx.send(serde_json::json!({ "message": "first log" }))
        .await
        .unwrap();

    tx.send(serde_json::json!({ "message": "second log" }))
        .await
        .unwrap();

    let body = tokio::time::timeout(std::time::Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap();

    sink.abort();

    let received_logs: serde_json::Value = serde_json::from_str(&body).unwrap();

    assert_eq!(
        received_logs,
        serde_json::json!([
            { "message": "first log" },
            { "message": "second log" }
        ])
    );
}

#[tokio::test]
async fn sink_writes_batch_to_dead_letter_file_after_exhausting_retries() {
    let _guard = DEAD_LETTER_TEST_LOCK.lock().await;

    let dead_letter_path = PathBuf::from("logtap.failed.jsonl");
    std::fs::remove_file(&dead_letter_path).ok();

    // Bind a port, then drop the listener immediately — nothing will ever
    // answer there, so every attempt fails fast with "connection refused".
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);

    let cfg = Config {
        source_path: PathBuf::from("unused.log"),
        sink_url: format!("http://{address}/logs"),
        batch_size: 1,
        flush_interval_secs: 60,
        channel_capacity: 10,
        max_retries: 2,
        retry_backoff_initial_ms: 1,
        retry_backoff_max_secs: 1,
        filter_rules: vec![],
        mask_common_patterns: false,
        dead_letter_max_bytes: 1024 * 1024,
        dead_letter_max_files: 5,
    };

    let (tx, rx) = tokio::sync::mpsc::channel::<LogLine>(cfg.channel_capacity);

    let sink = tokio::spawn(run_sink(cfg, rx));

    tx.send(serde_json::json!({ "message": "never delivered" }))
        .await
        .unwrap();

    // 2 attempts with ~1ms backoff exhaust almost immediately; give it some slack.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    sink.abort();

    let contents = std::fs::read_to_string(&dead_letter_path)
        .expect("expected dead-letter file to have been created");

    std::fs::remove_file(&dead_letter_path).ok();

    let lines: Vec<serde_json::Value> = contents
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();

    assert_eq!(
        lines,
        vec![serde_json::json!({ "message": "never delivered" })]
    );
}

#[tokio::test]
async fn sink_rotates_dead_letter_file_once_it_exceeds_max_bytes() {
    let _guard = DEAD_LETTER_TEST_LOCK.lock().await;

    let current = PathBuf::from("logtap.failed.jsonl");
    let rotated = PathBuf::from("logtap.failed.jsonl.1");
    std::fs::remove_file(&current).ok();
    std::fs::remove_file(&rotated).ok();

    // Bind a port, then drop the listener immediately — nothing will ever
    // answer there, so every attempt fails fast with "connection refused".
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);

    let cfg = Config {
        source_path: PathBuf::from("unused.log"),
        sink_url: format!("http://{address}/logs"),
        batch_size: 1,
        flush_interval_secs: 60,
        channel_capacity: 10,
        max_retries: 1,
        retry_backoff_initial_ms: 1,
        retry_backoff_max_secs: 1,
        filter_rules: vec![],
        mask_common_patterns: false,
        // Tiny on purpose: the very first failed batch already exceeds this,
        // so the *second* failure is guaranteed to trigger a rotation.
        dead_letter_max_bytes: 10,
        dead_letter_max_files: 1,
    };

    let (tx, rx) = tokio::sync::mpsc::channel::<LogLine>(cfg.channel_capacity);

    let sink = tokio::spawn(run_sink(cfg, rx));

    tx.send(serde_json::json!({ "message": "first failure" }))
        .await
        .unwrap();
    tx.send(serde_json::json!({ "message": "second failure" }))
        .await
        .unwrap();

    // Each batch gives up after a single failed attempt (max_retries: 1),
    // and the sink processes messages one at a time, so this is plenty.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    sink.abort();

    let current_contents =
        std::fs::read_to_string(&current).expect("expected a fresh logtap.failed.jsonl");
    let rotated_contents =
        std::fs::read_to_string(&rotated).expect("expected logtap.failed.jsonl.1 from rotation");

    std::fs::remove_file(&current).ok();
    std::fs::remove_file(&rotated).ok();

    let current_log: serde_json::Value = serde_json::from_str(current_contents.trim()).unwrap();
    let rotated_log: serde_json::Value = serde_json::from_str(rotated_contents.trim()).unwrap();

    assert_eq!(
        current_log,
        serde_json::json!({ "message": "second failure" })
    );
    assert_eq!(
        rotated_log,
        serde_json::json!({ "message": "first failure" })
    );
}
