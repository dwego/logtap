use std::collections::HashMap;
use serde_json::Value;
use crate::record::LogLine;
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
    match serde_json::from_str::<HashMap<String, Value>>(line) {
        Ok(fields) => LogLine{
            raw: line.to_string(),
            fields,
        },
        Err(_) => {
            let mut fields = HashMap::new();
            fields.insert("message".to_string(), Value::String(line.to_string()));
            fields.insert("level".to_string(), Value::String("UNKNOWN".to_string()));
            
            LogLine {
                raw: line.to_string(),
                fields,
            }
        }
    }
}
