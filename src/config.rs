//! Configuration management.
//!
//! This module defines the application's configuration structure and provides
//! functionality to load configuration values from a TOML file.
//!
//! Missing fields are automatically populated with sensible default values
//! through Serde's `default` attribute.

use crate::filter::FilterRule;
use anyhow::Result;
use serde::Deserialize;
use std::path::PathBuf;

/// Application configuration loaded from TOML file.
#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    /// Path to the input souce.
    pub source_path: PathBuf,

    /// URL of the destination sink.
    pub sink_url: String,

    /// Number of events processed in each batch. 
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    /// Time in seconds before flushing pending events.
    #[serde(default = "default_flush_interval")]
    pub flush_interval_secs: u64,

    /// Maximum number off buffered messages.
    #[serde(default = "default_channel_capacity")]
    pub channel_capacity: usize,

    /// Maximum retry attempts after a failed send.
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,

    /// Initial retry delay in millyseconds.
    #[serde(default = "default_retry_backoff_initial_ms")]
    pub retry_backoff_initial_ms: u64,

    /// Maximum retry backoff delay in seconds.
    #[serde(default = "default_retry_backoff_max_secs")]
    pub retry_backoff_max_secs: u64,

    /// Event filtering rules. Empty means no filtering.
    #[serde(default)]
    pub filter_rules: Vec<FilterRule>,

    /// Enable masking of common sensitive patterns.
    #[serde(default = "default_mask_common_patterns")]
    pub mask_common_patterns: bool,

    /// Maximum dead-letter storage size in bytes.
    #[serde(default = "default_dead_letter_max_bytes")]
    pub dead_letter_max_bytes: usize,

    /// Maximum number of dead-letter files kept;
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
    /// Loads configuration from a TOML file.
    ///
    /// Missing optional fields use default values.
    pub fn load(path: &str) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("não consegui ler {path}: {e}"))?;
        let cfg: Config = toml::from_str(&text)?;
        Ok(cfg)
    }
}
