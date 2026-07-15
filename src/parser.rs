use crate::record::LogLine;
use serde_json::json;
use tokio::sync::mpsc::{Receiver, Sender};

pub async fn run_parser(mut rx: Receiver<String>, tx: Sender<LogLine>) {
    while let Some(line) = rx.recv().await {
        let log_line = parse_line(&line);
        if tx.send(log_line).await.is_err() {
            break;
        }
    }
}


pub fn parse_line(line: &str) -> LogLine {
    match serde_json::from_str::<LogLine>(line) {
        Ok(value) if value.is_object() => value,

        Ok(value) => {
            eprintln!("logtap: line was valid JSON, but is not object: {value}");
            json!({ "message": line, "level": "UNKNOWN", "parse_issue": "not_an_object" })
        }

        Err(err) => {
            eprintln!("logtap: falied to process as JSON: {err}");
            json!({ "message": line, "level": "UNKNOWN", "parse_issue": "invalid_json" })
        }
    }
}