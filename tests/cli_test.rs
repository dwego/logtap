use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

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

// This spawns the actual compiled `logtap` binary as a subprocess — unlike
// the other integration tests, which call `logtap::run` directly, this is
// the only test that goes through `main.rs` itself (arg parsing, config
// path handling), so it's the only one that would catch the CLI wiring
// breaking even if every library-level test still passed.
#[tokio::test]
async fn cli_reads_config_file_and_delivers_a_log_line() {
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

    let source_path = PathBuf::from("cli_test.log");
    fs::write(&source_path, "").unwrap();

    let config_path = PathBuf::from("cli_test.toml");
    let config_toml = format!(
        r#"
source_path = "{source}"
sink_url = "http://{address}/logs"
batch_size = 1
flush_interval_secs = 60
max_retries = 0
"#,
        source = source_path.display(),
    );
    fs::write(&config_path, config_toml).unwrap();

    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_logtap"))
        .arg("--config-path")
        .arg(&config_path)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to start the logtap binary");

    // Spawning a whole new OS process (not just a task inside the test
    // binary) has more startup overhead than the other integration tests —
    // give it real time to parse args, load the config, and register the
    // file watcher before writing the line it needs to pick up.
    tokio::time::sleep(Duration::from_secs(1)).await;

    {
        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(&source_path)
            .unwrap();

        writeln!(file, r#"{{"level":"info","message":"cli test worked"}}"#).unwrap();
    }

    let body = tokio::time::timeout(Duration::from_secs(3), server)
        .await
        .expect("timeout: the logtap binary never delivered the log line")
        .unwrap();

    let _ = child.kill().await;
    let _ = child.wait().await;

    fs::remove_file(&source_path).ok();
    fs::remove_file(&config_path).ok();

    let received: serde_json::Value = serde_json::from_str(&body).unwrap();

    assert_eq!(
        received,
        serde_json::json!([{ "level": "info", "message": "cli test worked" }])
    );
}
