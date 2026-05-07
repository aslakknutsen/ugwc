//! Gateway API control plane binary — see `design/implementation-plan.md`.

use clap::Parser;

mod admin;
mod config;
mod leader;
mod logging;
mod metrics;
mod run;

#[derive(Parser, Debug)]
#[command(name = "ugwc", version = version_string())]
struct Cli {
    /// Dump effective configuration as JSON and exit.
    #[arg(long)]
    dump_config: bool,
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
