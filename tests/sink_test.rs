use std::path::PathBuf;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use logtap::config::Config;
use logtap::record::LogLine;
use logtap::sink::run_sink;

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
            .write_all(
                b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\nconnection: close\r\n\r\n",
            )
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