use clap::Parser;
use logtap::Config;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(version, about = "Logtap CLI")]
struct Args {
    #[arg(short, long, value_name = "logtap.toml")]
    config_path: String,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let path = PathBuf::from(&args.config_path);

    if !path.exists() {
        eprintln!("Config file not found: {:?}", args.config_path);
        return;
    }

    let config = Config::load(&args.config_path).expect("failed to load config file");

    println!("Loaded config: {:?}", config);

    logtap::run(config).await.expect("logtap failed to run");
}
