use clap::Parser;
use kafka_proxy::cli::Args;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let args = Args::parse();
    kafka_proxy::run(args).await
}
