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

#[tokio::test]
async fn test_run_source_detects_log_rotation() {
    let path = PathBuf::from("teste_run_source_rotacao.log");
    let rotated_path = PathBuf::from("teste_run_source_rotacao.log.1");
    fs::remove_file(&rotated_path).ok();
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
        writeln!(file, "antes da rotacao").unwrap();
    }

    let before = timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("timeout: linha de antes da rotação não chegou")
        .expect("canal fechou antes de receber a linha");
    assert_eq!(before, "antes da rotacao");

    // Simulate standard logrotate behavior: rename the current file away,
    // then create a brand new empty file at the same path.
    fs::rename(&path, &rotated_path).unwrap();
    fs::write(&path, "").unwrap();

    // Give the 200ms rotation-detection tick room to notice the inode swap.
    tokio::time::sleep(Duration::from_millis(400)).await;

    {
        let mut file = fs::OpenOptions::new().append(true).open(&path).unwrap();
        writeln!(file, "depois da rotacao").unwrap();
    }

    let after = timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("timeout: linha de depois da rotação não chegou — rotação não detectada")
        .expect("canal fechou antes de receber a linha");
    assert_eq!(after, "depois da rotacao");

    fs::remove_file(&path).ok();
    fs::remove_file(&rotated_path).ok();
}
