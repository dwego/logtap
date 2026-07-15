use serde_json::Value;
use std::collections::HashMap;

pub struct LogLine {
    pub raw: String,
    pub fields: HashMap<String, Value>,
}