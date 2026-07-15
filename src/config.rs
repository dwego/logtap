use crate::filter::FilterRule;
use serde::Deserialize;
use std::path::PathBuf;

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub source_path: PathBuf,
    pub sink_url: String,
    pub batch_size: usize,
    pub flush_interval_secs: u64,
    pub channel_capacity: usize,

    #[serde(default = "filter_rules_default")]
    pub filter_rules: Vec<FilterRule>,
}

fn filter_rules_default() -> Vec<FilterRule> {
    vec![]
}
