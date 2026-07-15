use crate::config::Config;
use crate::record::LogLine;
use std::time::Duration;
use tokio::sync::mpsc::Receiver;
use tokio::time::{interval, sleep};

pub async fn run_sink(cfg: Config, mut rx: Receiver<LogLine>) {
    let client = reqwest::Client::new();
    let mut batch: Vec<LogLine> = Vec::with_capacity(cfg.batch_size);
    let mut ticker = interval(Duration::from_secs(cfg.flush_interval_secs));

    loop {
        tokio::select! {
            Some(log) = rx.recv() => {
                batch.push(log);
                if batch.len() >= cfg.batch_size {
                    flush_with_retry(&client, &cfg, &mut batch).await;
                }
            }
            _ = ticker.tick() => {
                if !batch.is_empty() {
                    flush_with_retry(&client, &cfg, &mut batch).await;
                }
            }
        }
    }
}

async fn flush_with_retry(client: &reqwest::Client, cfg: &Config, batch: &mut Vec<LogLine>) {
    if batch.is_empty() {
        return;
    }

    let mut attempt: u32 = 0;

    loop {
        match client.post(&cfg.sink_url).json(batch).send().await {
            Ok(resp) if resp.status().is_success() => {
                batch.clear();
                return;
            }
            Ok(resp) => {
                eprintln!(
                    "logtap: tentativa {}/{} falhou — destino respondeu com status {}",
                    attempt + 1,
                    cfg.max_retries,
                    resp.status()
                );
            }
            Err(err) => {
                eprintln!(
                    "logtap: tentativa {}/{} falhou — erro ao enviar lote ({} itens): {err}",
                    attempt + 1,
                    cfg.max_retries,
                    batch.len()
                );
            }
        }

        attempt += 1;

        if attempt >= cfg.max_retries {
            // TODO(fase 1 do roadmap): dead-letter em vez de descartar aqui.
            // Por enquanto o lote é perdido depois de esgotar as tentativas.
            eprintln!(
                "logtap: desistindo do lote ({} itens) após {} tentativas",
                batch.len(),
                cfg.max_retries
            );
            batch.clear();
            return;
        }

        let backoff_ms = cfg
            .retry_backoff_initial_ms
            .saturating_mul(1u64 << (attempt - 1));
        let backoff =
            Duration::from_millis(backoff_ms).min(Duration::from_secs(cfg.retry_backoff_max_secs));

        eprintln!("logtap: esperando {backoff:?} antes da próxima tentativa");
        sleep(backoff).await;
    }
}
