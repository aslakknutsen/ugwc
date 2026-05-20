//! Gateway API control plane binary — see `design/implementation-plan.md`.

use clap::{Parser, Subcommand};

mod admin;
mod config;
mod implementation_profiles;
mod leader;
mod leader_election;
mod logging;
mod metrics;
mod readiness;
mod run;
mod xds_dump;

#[derive(Parser, Debug)]
#[command(name = "ugwc", version = version_string())]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
    /// Dump effective configuration as JSON and exit.
    #[arg(long)]
    dump_config: bool,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Open an ADS stream and print `DiscoveryResponse` summaries (debugging).
    XdsDump(xds_dump::XdsDumpArgs),
}

const fn version_string() -> &'static str {
    concat!(
        env!("CARGO_PKG_VERSION"),
        " (",
        env!("UGWC_BUILD_COMMIT"),
        ")"
    )
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let cli = Cli::parse();
    if let Some(Commands::XdsDump(args)) = cli.command {
        return xds_dump::run(args).await;
    }

    let cfg = config::Config::from_env()?;
    if cli.dump_config {
        println!("{}", serde_json::to_string_pretty(&cfg)?);
        return Ok(());
    }

    logging::init(cfg.log_json);
    tracing::info!(summary = %cfg.log_summary(), commit = env!("UGWC_BUILD_COMMIT"), "startup");
    if cfg.xds_insecure {
        tracing::warn!("UGWC_XDS_INSECURE is set — plaintext xDS (prototype only)");
    }
    run::run(cfg).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_has_sha_token() {
        assert!(version_string().contains('('));
    }
}
