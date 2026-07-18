use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::time::Duration;

use logtap::config::Config;
use logtap::source::run_source;
use tokio::sync::mpsc;
use tokio::time::timeout;

#[tokio::test]
async fn test_run_source_emite_linhas_novas() {
    let path = PathBuf::from("teste_run_source.log");
    fs::write(&path, "").unwrap();

    let (tx, mut rx) = mpsc::channel::<String>(100);

    let cfg = Config {
        source_path: path.clone(),
        sink_url: "http://localhost:8080".to_string(),
        batch_size: 100,
        flush_interval_secs: 5,
        channel_capacity: 100,
        max_retries: 3,
        retry_backoff_initial_ms: 100,
        retry_backoff_max_secs: 5,
        filter_rules: vec![],
        mask_common_patterns: false,
        dead_letter_max_bytes: 1024 * 1024,
        dead_letter_max_files: 5,
    };

    let _handle = tokio::task::spawn_blocking(move || run_source(cfg, tx));

    tokio::time::sleep(Duration::from_millis(100)).await;

    {
        let mut file = fs::OpenOptions::new().append(true).open(&path).unwrap();
        writeln!(file, "linha de teste").unwrap();
    }

    let received = timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("timeout: nenhuma linha chegou em 2s")
        .expect("canal fechou antes de receber a linha");

    assert_eq!(received, "linha de teste");

    fs::remove_file(&path).ok();
}
