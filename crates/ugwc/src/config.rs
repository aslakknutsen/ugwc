//! Environment-backed configuration (`UGWC_…` prefix via `envy`).

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::implementation_profiles::{DataPlaneKind, ImplementationProfile};

fn default_admin() -> String {
    "127.0.0.1:8080".into()
}
fn default_xds() -> String {
    "127.0.0.1:18000".into()
}
fn default_controllers() -> String {
    "example.com/gateway-controller".into()
}
fn default_profile() -> String {
    "dev".into()
}
fn default_endpoint_debounce_ms() -> u64 {
    200
}
fn default_xds_wire() -> String {
    "gwxds".into()
}
fn default_reconcile_concurrency() -> usize {
    16
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
    #[serde(default)]
    pub xds_delta: bool,
    #[serde(default = "default_profile")]
    pub run_profile: String,
    #[serde(default)]
    pub leader_election: bool,
    /// Coalesce `EndpointSlice` churn before reconciling all Gateways cluster-wide.
    #[serde(default = "default_endpoint_debounce_ms")]
    pub endpoint_debounce_ms: u64,
    /// Concurrent gateway reconcile workers (dedup queue consumers).
    #[serde(default = "default_reconcile_concurrency")]
    pub reconcile_concurrency: usize,
    /// xDS wire profile for ADS responses: `gwxds` (Praxis) or `envoy` (RDS-only slice).
    #[serde(default = "default_xds_wire")]
    pub xds_wire: String,
    /// JSON file: `profiles` map from `controllerName` → dataplane + subscription defaults.
    #[serde(default)]
    pub implementation_profiles_path: Option<PathBuf>,
    /// Apply `PerGatewayPraxis` workloads when reconciling (also requires profile + bootstrap addr).
    #[serde(default)]
    pub dataplane_enable: bool,
    /// Bootstrap for Praxis `ISTIOD_ADDR`: `host:port` or full URI (`http://…` cleartext, `https://…` TLS).
    #[serde(default)]
    pub control_plane_xds_bootstrap_addr: Option<String>,
    /// Built-in profiles merged with [`implementation_profiles_path`](Self::implementation_profiles_path)
    /// (file entries override the same `controllerName` key). Not set directly from env.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub implementation_profiles: HashMap<String, ImplementationProfile>,
    /// OTLP metrics endpoint (requires `otlp-metrics` feature on the binary).
    pub otlp_endpoint: Option<String>,
    /// OTLP push interval in milliseconds (default 15000).
    #[serde(default = "default_otlp_push_interval_ms")]
    pub otlp_push_interval_ms: u64,
}

fn default_otlp_push_interval_ms() -> u64 {
    15_000
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
    #[error("implementation profiles: {0}")]
    Profiles(String),
    #[error("UGWC_DATAPLANE_ENABLE requires UGWC_CONTROL_PLANE_XDS_BOOTSTRAP_ADDR when a profile uses per_gateway_praxis")]
    BootstrapAddrRequired,
    #[error("per_gateway_praxis profile requires non-empty proxy_image for controller {0:?}")]
    ProxyImageRequired(String),
    #[error("UGWC_RECONCILE_CONCURRENCY must be >= 1")]
    ReconcileConcurrencyZero,
    #[error("UGWC_RECONCILE_CONCURRENCY must be <= 128")]
    ReconcileConcurrencyTooHigh,
}

impl Config {
    /// Load from process environment (`UGWC_` prefix).
    pub fn from_env() -> Result<Self, ConfigError> {
        let mut cfg: Config = envy::prefixed("UGWC_")
            .from_env()
            .map_err(|e: envy::Error| ConfigError::Addr("UGWC_*".into(), e.to_string()))?;

        let from_file = match &cfg.implementation_profiles_path {
            Some(p) => {
                crate::implementation_profiles::load_from_path(p).map_err(ConfigError::Profiles)?
            }
            None => HashMap::new(),
        };
        let mut profiles = crate::implementation_profiles::builtin_profiles();
        profiles.extend(from_file);
        cfg.implementation_profiles = profiles;

        cfg.validate_profiles()?;
        cfg.validate()?;
        Ok(cfg)
    }

    fn validate_profiles(&self) -> Result<(), ConfigError> {
        for (cn, p) in &self.implementation_profiles {
            if matches!(p.data_plane, DataPlaneKind::PerGatewayPraxis) {
                let img = p.proxy_image.as_deref().unwrap_or("").trim();
                if img.is_empty() {
                    return Err(ConfigError::ProxyImageRequired(cn.clone()));
                }
            }
        }

        if self.dataplane_enable {
            let needs_addr = self
                .implementation_profiles
                .values()
                .any(|p| matches!(p.data_plane, DataPlaneKind::PerGatewayPraxis));
            if needs_addr {
                let addr = self
                    .control_plane_xds_bootstrap_addr
                    .as_deref()
                    .unwrap_or("")
                    .trim();
                if addr.is_empty() {
                    return Err(ConfigError::BootstrapAddrRequired);
                }
            }
        }
        Ok(())
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
        let wire = self.xds_wire.to_ascii_lowercase();
        if wire != "gwxds" && wire != "envoy" {
            return Err(ConfigError::Addr(
                "UGWC_XDS_WIRE".into(),
                "must be `gwxds` or `envoy`".into(),
            ));
        }
        let has_cert = self.xds_tls_cert_path.is_some();
        let has_key = self.xds_tls_key_path.is_some();
        if has_cert != has_key {
            return Err(ConfigError::TlsMismatch);
        }
        if self.reconcile_concurrency == 0 {
            return Err(ConfigError::ReconcileConcurrencyZero);
        }
        if self.reconcile_concurrency > 128 {
            return Err(ConfigError::ReconcileConcurrencyTooHigh);
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
            "admin={} xds={} kube={} kube_watch=cluster insecure={} delta={} profile={} leader={} debounce_ms={} reconcile_workers={} xds_wire={} controllers={:?} dataplane={} profiles={}",
            self.admin_listen_addr,
            self.xds_listen_addr,
            self.kube_enable,
            self.xds_insecure,
            self.xds_delta,
            self.run_profile,
            self.leader_election,
            self.endpoint_debounce_ms,
            self.reconcile_concurrency,
            self.xds_wire,
            self.controller_name_list(),
            self.dataplane_enable,
            self.implementation_profiles.len(),
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

    #[test]
    fn rejects_zero_reconcile_concurrency() {
        let c = Config {
            reconcile_concurrency: 0,
            ..sample()
        };
        assert!(matches!(
            c.validate(),
            Err(ConfigError::ReconcileConcurrencyZero)
        ));
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
            xds_delta: false,
            run_profile: default_profile(),
            leader_election: false,
            endpoint_debounce_ms: default_endpoint_debounce_ms(),
            reconcile_concurrency: default_reconcile_concurrency(),
            xds_wire: default_xds_wire(),
            implementation_profiles_path: None,
            dataplane_enable: false,
            control_plane_xds_bootstrap_addr: None,
            implementation_profiles: HashMap::new(),
            otlp_endpoint: None,
            otlp_push_interval_ms: default_otlp_push_interval_ms(),
        }
    }
}
