use clap::Parser;
use logtap::Config;

#[derive(Parser, Debug)]
#[command(version, about = "Logtap CLI")]
struct Args {
    #[arg(short, long, value_name = "logtap.toml", default_value = "logtap.toml")]
    config_path: String,
}

#[tokio::main]
async fn main() -> Result<(), String> {
    let args = Args::parse();

    let config = Config::load(&args.config_path).map_err(|e| e.to_string())?;

    println!("Loaded config: {:?}", config);

    logtap::run(config).await.map_err(|e| e.to_string())?;

    Ok(())
}
