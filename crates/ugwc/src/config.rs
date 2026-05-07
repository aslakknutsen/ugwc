//! Environment-backed configuration (`UGWC_…` prefix via `envy`).

use std::net::SocketAddr;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use thiserror::Error;

fn default_admin() -> String {
    "127.0.0.1:8080".into()
}
fn default_xds() -> String {
    "127.0.0.1:18000".into()
}
fn default_controllers() -> String {
    "example.com/gateway-controller".into()
}
fn default_ns() -> String {
    "default".into()
}
fn default_profile() -> String {
    "dev".into()
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    #[serde(default = "default_admin")]
    pub admin_listen_addr: String,
    #[serde(default = "default_xds")]
    pub xds_listen_addr: String,
    #[serde(default = "default_controllers")]
    pub controller_names: String,
    #[serde(default)]
    pub xds_insecure: bool,
    pub xds_tls_cert_path: Option<PathBuf>,
    pub xds_tls_key_path: Option<PathBuf>,
    pub xds_tls_ca_path: Option<PathBuf>,
    #[serde(default)]
    pub log_json: bool,
    #[serde(default)]
    pub kube_enable: bool,
    #[serde(default = "default_ns")]
    pub kube_namespace: String,
    #[serde(default)]
    pub xds_delta: bool,
    #[serde(default = "default_profile")]
    pub run_profile: String,
    #[serde(default)]
    pub leader_election: bool,
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("invalid listen address `{0}`: {1}")]
    Addr(String, String),
    #[error("controller_names must be non-empty")]
    EmptyControllers,
    #[error("TLS cert and key must both be set or both omitted")]
    TlsMismatch,
    #[error("production profile cannot run with plaintext xDS")]
    InsecureProd,
}

impl Config {
    /// Load from process environment (`UGWC_` prefix).
    pub fn from_env() -> Result<Self, ConfigError> {
        let cfg: Config = envy::prefixed("UGWC_")
            .from_env()
            .map_err(|e: envy::Error| ConfigError::Addr("UGWC_*".into(), e.to_string()))?;
        cfg.validate()?;
        Ok(cfg)
    }

    fn validate(&self) -> Result<(), ConfigError> {
        parse_socket_addr(&self.admin_listen_addr)?;
        parse_socket_addr(&self.xds_listen_addr)?;
        let names: Vec<_> = self
            .controller_names
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .collect();
        if names.is_empty() {
            return Err(ConfigError::EmptyControllers);
        }
        let profile = self.run_profile.to_ascii_lowercase();
        if profile == "prod" && self.xds_insecure {
            return Err(ConfigError::InsecureProd);
        }
        let has_cert = self.xds_tls_cert_path.is_some();
        let has_key = self.xds_tls_key_path.is_some();
        if has_cert != has_key {
            return Err(ConfigError::TlsMismatch);
        }
        Ok(())
    }

    #[must_use]
    pub fn controller_name_list(&self) -> Vec<String> {
        self.controller_names
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(String::from)
            .collect()
    }

    /// Redacted single-line summary safe for startup logs.
    #[must_use]
    pub fn log_summary(&self) -> String {
        format!(
            "admin={} xds={} kube={} ns={} insecure={} delta={} profile={} leader={} controllers={:?}",
            self.admin_listen_addr,
            self.xds_listen_addr,
            self.kube_enable,
            self.kube_namespace,
            self.xds_insecure,
            self.xds_delta,
            self.run_profile,
            self.leader_election,
            self.controller_name_list(),
        )
    }
}

fn parse_socket_addr(s: &str) -> Result<SocketAddr, ConfigError> {
    s.parse()
        .map_err(|e: std::net::AddrParseError| ConfigError::Addr(s.into(), e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_empty_controllers_via_env() {
        let c = Config {
            controller_names: "".into(),
            ..sample()
        };
        assert!(matches!(c.validate(), Err(ConfigError::EmptyControllers)));
    }

    fn sample() -> Config {
        Config {
            admin_listen_addr: default_admin(),
            xds_listen_addr: default_xds(),
            controller_names: default_controllers(),
            xds_insecure: true,
            xds_tls_cert_path: None,
            xds_tls_key_path: None,
            xds_tls_ca_path: None,
            log_json: false,
            kube_enable: false,
            kube_namespace: default_ns(),
            xds_delta: false,
            run_profile: default_profile(),
            leader_election: false,
        }
    }
}
