use clap::Parser;

#[derive(Parser, Debug, Clone)]
#[command(name = "kafka-proxy")]
#[command(about = "SNI-based Kafka TLS proxy (config-driven)", long_about = None)]
pub struct Args {
    /// YAML config file path
    #[arg(short = 'c', long, default_value = "config.yaml")]
    pub config: String,

    /// Generate ca.pem, server.pem, key.pem in the current directory and exit
    #[arg(long)]
    pub generate_certs: bool,

    /// Fetch server certificate chain and save issuer cert as server_ca.pem
    #[arg(long)]
    pub save_ca_certs: bool,

    /// Target server in host:port format for --save-ca-certs
    #[arg(long)]
    pub server: Option<String>,

    /// SNI suffix (required only with --generate-certs)
    #[arg(long)]
    pub sni_suffix: Option<String>,
}
