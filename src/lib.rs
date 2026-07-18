pub mod config;
pub mod filter;
pub mod parser;
pub mod record;
pub mod sink;
pub mod source;

pub use config::Config;
pub use record::LogLine;

/// Application entry point that starts the processing pipeline.
///
/// Creates the communication channels and launches the source, parser,
/// filter, and sink tasks.
///
/// The pipeline flow is:
///
/// ```text
/// source -> parser -> filter -> sink
/// ```
///
/// Tasks communicate through bounded channels using the configured capacity.
pub async fn run(cfg: Config) -> anyhow::Result<()> {
    let (raw_tx, raw_rx) = tokio::sync::mpsc::channel::<String>(cfg.channel_capacity);
    let (parsed_tx, parsed_rx) = tokio::sync::mpsc::channel::<LogLine>(cfg.channel_capacity);
    let (clean_tx, clean_rx) = tokio::sync::mpsc::channel::<LogLine>(cfg.channel_capacity);

    let source_cfg = cfg.clone();
    let mask_common_patterns = source_cfg.mask_common_patterns;
    let source_handle = tokio::task::spawn_blocking(move || source::run_source(source_cfg, raw_tx));

    let parser_handle = tokio::spawn(parser::run_parser(raw_rx, parsed_tx));

    let rules = cfg.filter_rules.clone();
    let filter_handle = tokio::spawn(filter::run_filter(
        parsed_rx,
        clean_tx,
        rules,
        mask_common_patterns,
    ));

    let sink_cfg = cfg.clone();
    let sink_handle = tokio::spawn(sink::run_sink(sink_cfg, clean_rx));

    let (src_res, _, _, _) = tokio::join!(source_handle, parser_handle, filter_handle, sink_handle);

    if let Ok(Err(e)) = src_res {
        eprintln!("logtap: source encerrou com erro: {e}");
    }

    Ok(())
}
