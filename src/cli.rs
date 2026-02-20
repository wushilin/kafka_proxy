use clap::Parser;

#[derive(Parser, Debug, Clone)]
#[command(name = "kafka-proxy")]
#[command(about = "SNI-based Kafka TLS proxy", long_about = None)]
pub struct Args {
    /// Bind address (e.g., 0.0.0.0:9092)
    #[arg(long, default_value = "0.0.0.0:9092")]
    pub bind: String,

    /// SNI suffix (e.g., kafka.abc.com)
    #[arg(long)]
    pub sni_suffix: String,

    /// Server certificate file
    #[arg(long, default_value = "server.pem")]
    pub cert: String,

    /// Server private key file
    #[arg(long, default_value = "key.pem")]
    pub key: String,

    /// CA certificates file for client verification
    #[arg(long)]
    pub ca_certs: Option<String>,

    /// Enable mutual TLS (mTLS)
    #[arg(long, conflicts_with = "mtls_disable")]
    pub mtls_enable: bool,

    /// Disable mutual TLS (mTLS)
    #[arg(long, conflicts_with = "mtls_enable")]
    pub mtls_disable: bool,

    /// Upstream CA certificates file for upstream TLS verification
    #[arg(long)]
    pub upstream_ca_certs: Option<String>,

    /// Trust upstream certificates without verification
    #[arg(long, conflicts_with = "upstream_ca_certs")]
    pub trust_upstream_certs: bool,

    /// Generate ca.pem, server.pem, key.pem in the current directory and exit
    #[arg(long)]
    pub generate_certs: bool,

    /// Optional hostname mapping rule in format "<regex>::<replacement>"
    /// Example: "(.*).conflu(.*).cloud::$1-$2.beatbox.com"
    #[arg(long)]
    pub broker_mapping_rule: Option<String>,

    /// Optional hosts file with wildcard host mappings for upstream dialing
    #[arg(long)]
    pub hosts_file: Option<String>,

    /// Upstream brokers (comma-separated, e.g., b1.upstream.com:9092,b2.upstream.com:9092)
    #[arg(long, value_delimiter = ',')]
    pub upstream: Vec<String>,
}
