use crate::filter::FilterRule;
use anyhow::Result;
use serde::Deserialize;
use std::path::PathBuf;

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub source_path: PathBuf,
    pub sink_url: String,

    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    #[serde(default = "default_flush_interval")]
    pub flush_interval_secs: u64,

    #[serde(default = "default_channel_capacity")]
    pub channel_capacity: usize,

    #[serde(default = "default_max_retries")]
    pub max_retries: u32,

    #[serde(default = "default_retry_backoff_initial_ms")]
    pub retry_backoff_initial_ms: u64,

    #[serde(default = "default_retry_backoff_max_secs")]
    pub retry_backoff_max_secs: u64,

    #[serde(default)]
    pub filter_rules: Vec<FilterRule>,

    #[serde(default = "default_mask_common_patterns")]
    pub mask_common_patterns: bool,

    #[serde(default = "default_dead_letter_max_bytes")]
    pub dead_letter_max_bytes: usize,

    #[serde(default = "default_dead_letter_max_files")]
    pub dead_letter_max_files: u64,
}

fn default_batch_size() -> usize {
    50
}
fn default_flush_interval() -> u64 {
    5
}
fn default_channel_capacity() -> usize {
    1000
}
fn default_max_retries() -> u32 {
    5
}
fn default_retry_backoff_initial_ms() -> u64 {
    500
}
fn default_retry_backoff_max_secs() -> u64 {
    30
}
fn default_mask_common_patterns() -> bool {
    true
}

fn default_dead_letter_max_bytes() -> usize {
    1024 * 1024 * 1024
}

fn default_dead_letter_max_files() -> u64 {
    5
}

impl Config {
    pub fn load(path: &str) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("não consegui ler {path}: {e}"))?;
        let cfg: Config = toml::from_str(&text)?;
        Ok(cfg)
    }
}
