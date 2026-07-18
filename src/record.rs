// Represents a single log entry as a JSON value.
///
/// Each log line is expected to contain a JSON object with fields that can be
/// processed by filters and sent to the configured sink.
pub type LogLine = serde_json::Value;
