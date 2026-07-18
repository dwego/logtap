// Stress / load-shaped tests — distinct from the correctness-per-function
// tests elsewhere in this directory. These exercise the full pipeline under
// conditions meant to trigger backpressure and sustained failure, not just
// verify a single behavior in isolation.

use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::time::Duration;

use logtap::config::Config;
use tokio::net::TcpListener;

const LOG_COUNT: usize = 20;

#[tokio::test]
async fn outage_does_not_lose_logs_when_destination_is_down() {
    // Bind a port, then drop the listener immediately — nothing answers
    // there, so every send fails fast with "connection refused", simulating
    // the destination being down for the entire test.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);

    let source_path = PathBuf::from("stress_outage.log");
    let dead_letter_path = PathBuf::from("logtap.failed.jsonl");
    fs::remove_file(&dead_letter_path).ok();
    fs::write(&source_path, "").unwrap();

    let cfg = Config {
        source_path: source_path.clone(),
        sink_url: format!("http://{address}/logs"),
        batch_size: 1,
        flush_interval_secs: 60,
        // Deliberately small — with 20 logs written at once, this forces the
        // channels to fill up and back the source's reads up behind them,
        // rather than everything sailing through unobstructed.
        channel_capacity: 5,
        max_retries: 2,
        retry_backoff_initial_ms: 1,
        retry_backoff_max_secs: 1,
        filter_rules: vec![],
        mask_common_patterns: false,
        dead_letter_max_bytes: 1024 * 1024,
        dead_letter_max_files: 5,
    };

    let app = tokio::spawn(async move {
        logtap::run(cfg).await.unwrap();
    });

    tokio::time::sleep(Duration::from_millis(300)).await;

    // Write all 20 lines in a single burst, well beyond channel_capacity, so
    // the source can't just breeze through them one at a time.
    {
        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(&source_path)
            .unwrap();

        for seq in 0..LOG_COUNT {
            writeln!(file, r#"{{"seq":{seq}}}"#).unwrap();
        }
    }

    // Poll the dead-letter file instead of guessing a fixed sleep — every
    // batch here is doomed to fail, so this is where all 20 logs should
    // eventually land.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut seen = Vec::new();

    while tokio::time::Instant::now() < deadline {
        if let Ok(contents) = fs::read_to_string(&dead_letter_path) {
            seen = contents
                .lines()
                .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
                .collect();

            if seen.len() >= LOG_COUNT {
                break;
            }
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    app.abort();
    fs::remove_file(&source_path).ok();
    fs::remove_file(&dead_letter_path).ok();

    let mut seqs: Vec<u64> = seen
        .iter()
        .map(|v| v["seq"].as_u64().expect("dead-letter entry missing seq"))
        .collect();
    seqs.sort_unstable();

    let expected: Vec<u64> = (0..LOG_COUNT as u64).collect();
    assert_eq!(
        seqs, expected,
        "dead-letter file should contain exactly the {LOG_COUNT} logs written, no more, no fewer, no duplicates"
    );
}
