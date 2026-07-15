use std::fs::File;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::thread::sleep;
use std::time::Duration;

pub fn run_source(
    cfg: crate::config::Config,
    tx: tokio::sync::mpsc::Sender<String>,
) -> anyhow::Result<()> {
    let mut offset: u64 = 0;

    loop {
        let file = File::open(&cfg.source_path)?;
        let mut reader = BufReader::new(file);
        reader.seek(SeekFrom::Start(offset))?;

        let mut line = String::new();
        loop {
            line.clear();
            let bytes_read = reader.read_line(&mut line)?;

            if bytes_read == 0 {
                break;
            }

            offset += bytes_read as u64;
            let trimmed = line.trim_end().to_string();

            if tx.blocking_send(trimmed).is_err() {
                return Ok(());
            }
        }

        sleep(Duration::from_millis(500));
    }
}