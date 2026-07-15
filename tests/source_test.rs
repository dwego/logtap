use std::path::PathBuf;

use logtap::config::Config;
use logtap::source::run_source;
use tokio::sync::mpsc;

#[tokio::test]
async fn test_run_source() {
    let (tx, mut rx) = mpsc::channel::<String>(100);

    let cfg = Config {
        source_path: PathBuf::from("teste.log"),
        sink_url: "http://localhost:8080".to_string(),
        batch_size: 100,
        flush_interval_secs: 5,
        channel_capacity: 100,
    };

    tokio::spawn(async move {
        run_source(cfg, tx).await.unwrap();
    });

    while let Some(line) = rx.recv().await {
        println!("{line}");
    }
}
