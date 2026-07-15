use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::time::Duration;

use logtap::config::Config;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

async fn read_http_request_body(stream: &mut TcpStream) -> String {
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
async fn integration_source_parser_filter_sink_sends_log_to_http_server() {
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

    let source_path = PathBuf::from("integration_source_parser_filter_sink.log");
    fs::write(&source_path, "").unwrap();

    let cfg = Config {
        source_path: source_path.clone(),
        sink_url: format!("http://{address}/logs"),
        batch_size: 1,
        flush_interval_secs: 60,
        channel_capacity: 10,
        max_retries: 0,
        retry_backoff_initial_ms: 0,
        retry_backoff_max_secs: 0,
        filter_rules: vec![],
        mask_common_patterns: false,
    };

    let app = tokio::spawn(async move {
        logtap::run(cfg).await.unwrap();
    });

    tokio::time::sleep(Duration::from_millis(200)).await;

    {
        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(&source_path)
            .unwrap();

        writeln!(
            file,
            r#"{{"level":"info","message":"integration test worked"}}"#
        )
        .unwrap();
    }

    let body = tokio::time::timeout(Duration::from_secs(3), server)
        .await
        .expect("timeout: sink did not send request to test HTTP server")
        .unwrap();

    app.abort();

    fs::remove_file(&source_path).ok();

    let received_logs: serde_json::Value = serde_json::from_str(&body).unwrap();

    assert_eq!(
        received_logs,
        serde_json::json!([
            {
                "level": "info",
                "message": "integration test worked"
            }
        ])
    );
}

#[tokio::test]
async fn integration_loads_real_config_file_and_delivers_filtered_log() {
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

    let source_path = PathBuf::from("integration_config_file.log");
    fs::write(&source_path, "").unwrap();

    let config_path = PathBuf::from("integration_config_file.toml");
    let config_toml = format!(
        r#"
source_path = "{source}"
sink_url = "http://{address}/logs"
batch_size = 1
flush_interval_secs = 60
channel_capacity = 10
max_retries = 0
retry_backoff_initial_ms = 0
retry_backoff_max_secs = 0
mask_common_patterns = false

[[filter_rules]]
field = "level"
op = "equals"
value = "debug"
action = "drop"
"#,
        source = source_path.display(),
        address = address
    );
    fs::write(&config_path, config_toml).unwrap();

    let cfg =
        logtap::Config::load(config_path.to_str().unwrap()).expect("failed to load logtap.toml");

    let app = tokio::spawn(async move {
        logtap::run(cfg).await.unwrap();
    });

    tokio::time::sleep(Duration::from_millis(200)).await;

    {
        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(&source_path)
            .unwrap();

        writeln!(file, r#"{{"level":"debug","message":"should be dropped"}}"#).unwrap();
        writeln!(file, r#"{{"level":"info","message":"config file worked"}}"#).unwrap();
    }

    let body = tokio::time::timeout(Duration::from_secs(3), server)
        .await
        .expect("timeout: sink did not send request to test HTTP server")
        .unwrap();

    app.abort();

    fs::remove_file(&source_path).ok();
    fs::remove_file(&config_path).ok();

    let received_logs: serde_json::Value = serde_json::from_str(&body).unwrap();

    assert_eq!(
        received_logs,
        serde_json::json!([
            {
                "level": "info",
                "message": "config file worked"
            }
        ])
    );
}
