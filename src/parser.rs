use crate::record::LogLine;
use serde_json::json;
use tokio::sync::mpsc::{Receiver, Sender};

/// Parses incoming log lines and forwards structured log entries.
///
/// Receives raw text lines, converts them into `LogLine` values, and sends
/// the parsed entries to the next stage of the pipeline.
pub async fn run_parser(mut rx: Receiver<String>, tx: Sender<LogLine>) {
    while let Some(line) = rx.recv().await {
        let log_line = parse_line(&line);
        if tx.send(log_line).await.is_err() {
            break;
        }
    }
}

/// Converts a raw log line into a structured JSON log entry.
///
/// Valid JSON objects are returned unchanged.
/// Valid JSON values that are not objects and invalid JSON are wrapped into
/// a fallback log entry containing the original message and a parse issue.
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
