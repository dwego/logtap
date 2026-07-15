use std::path::PathBuf;

pub struct Config {
    pub source_path: PathBuf,
    pub sink_url: String,
    pub batch_size: usize,
    pub flush_interval_secs: u64,
    pub channel_capacity: usize,
}
