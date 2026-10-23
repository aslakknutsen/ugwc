//! Publish encoded Praxis configuration as a ConfigMap plus a TLS Secret.
//!
//! The ConfigMap holds a single `praxis.yaml` key; the Secret holds the
//! certificate, key, and CA files referenced by that document. Both are
//! owner-referenced to the `Gateway` and server-side applied so repeated
//! reconciles converge.

use std::collections::BTreeMap;

use sha2::{Digest, Sha256};

use k8s_openapi::api::core::v1::{ConfigMap, Secret};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use k8s_openapi::ByteString;
use kube::api::{Api, Patch, PatchParams};
use kube::Client;

use super::{per_gateway_dataplane_name, ProvisionError};
use crate::resources::Gateway;

/// ConfigMap name for the per-gateway Praxis document (`ugwc-dp-<gateway>`).
#[must_use]
pub fn praxis_config_map_name(gateway_name: &str) -> String {
    per_gateway_dataplane_name(gateway_name)
}

/// TLS Secret name for the per-gateway Praxis data plane
/// (`ugwc-dp-<gateway>-tls`, truncated to 63 characters).
#[must_use]
pub fn praxis_tls_secret_name(gateway_name: &str) -> String {
    let stem: String = per_gateway_dataplane_name(gateway_name)
        .chars()
        .take(59)
        .collect();
    format!("{stem}-tls")
}

/// Stable revision for the static part of the Praxis document and its TLS files.
///
/// The pod template carries this value as an annotation so route/listener or
/// certificate changes roll the proxy immediately. Endpoints are stripped: they
/// churn as pods come and go, and a restart per endpoint change would be
/// unacceptable. Endpoint-only updates reach the running proxy through Praxis
/// file reload.
#[must_use]
pub fn config_revision(output: &ugwc_praxis::EncodeOutput) -> String {
    let mut value: serde_yaml::Value = match serde_yaml::from_str(&output.yaml) {
        Ok(value) => value,
        Err(_) => return String::new(),
    };
    strip_endpoints(&mut value);
    let canonical = serde_yaml::to_string(&value).unwrap_or_default();
    let mut digest = Sha256::new();
    digest.update(canonical.as_bytes());
    let mut files: Vec<_> = output.files.iter().collect();
    files.sort_by(|a, b| a.key.cmp(&b.key));
    for file in files {
        digest.update(file.key.as_bytes());
        digest.update(&file.contents);
    }
    hex::encode(&digest.finalize()[..8])
}

fn strip_endpoints(value: &mut serde_yaml::Value) {
    match value {
        serde_yaml::Value::Mapping(map) => {
            map.remove(serde_yaml::Value::String("endpoints".into()));
            for (_, child) in map.iter_mut() {
                strip_endpoints(child);
            }
        }
        serde_yaml::Value::Sequence(seq) => {
            for child in seq {
                strip_endpoints(child);
            }
        }
        _ => {}
    }
}

fn owner_reference(gateway: &Gateway) -> Result<OwnerReference, ProvisionError> {
    let name = gateway
        .metadata
        .name
        .as_deref()
        .ok_or_else(|| ProvisionError::Msg("gateway missing name".into()))?;
    let uid = gateway
        .metadata
        .uid
        .as_deref()
        .ok_or_else(|| ProvisionError::Msg("gateway missing uid for ownerReference".into()))?;
    Ok(OwnerReference {
        api_version: "gateway.networking.k8s.io/v1".into(),
        kind: "Gateway".into(),
        name: name.into(),
        uid: uid.into(),
        controller: Some(true),
        block_owner_deletion: Some(true),
    })
}

fn labels(gateway: &Gateway, namespace: &str, gateway_name: &str) -> BTreeMap<String, String> {
    let mut labels = BTreeMap::new();
    labels.insert(
        "app.kubernetes.io/name".into(),
        "ugwc-praxis-dataplane".into(),
    );
    labels.insert("app.kubernetes.io/managed-by".into(), "ugwc".into());
    labels.insert(
        "gateway.ugwc.io/gateway-namespace".into(),
        gateway
            .metadata
            .namespace
            .clone()
            .unwrap_or_else(|| namespace.into()),
    );
    labels.insert("gateway.ugwc.io/gateway-name".into(), gateway_name.into());
    labels.insert(
        "gateway.networking.k8s.io/gateway-name".into(),
        gateway_name.into(),
    );
    labels.insert("gateway.ugwc.io/managed".into(), "ugwc-per-gateway".into());
    labels
}

/// Kubernetes Secret data keys only allow `[-._a-zA-Z0-9]`.
#[must_use]
pub fn valid_secret_key(key: &str) -> bool {
    !key.is_empty()
        && key.len() <= 253
        && key
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
}

fn metadata(
    gateway: &Gateway,
    name: &str,
    namespace: &str,
    gateway_name: &str,
) -> Result<k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta, ProvisionError> {
    Ok(k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta {
        name: Some(name.into()),
        namespace: Some(namespace.into()),
        labels: Some(labels(gateway, namespace, gateway_name)),
        owner_references: Some(vec![owner_reference(gateway)?]),
        ..Default::default()
    })
}

/// Build the ConfigMap holding `praxis.yaml`.
pub fn build_config_map(
    gateway: &Gateway,
    namespace: &str,
    gateway_name: &str,
    yaml: &str,
) -> Result<ConfigMap, ProvisionError> {
    let name = praxis_config_map_name(gateway_name);
    Ok(ConfigMap {
        metadata: metadata(gateway, &name, namespace, gateway_name)?,
        data: Some(BTreeMap::from([(
            ugwc_praxis::CONFIG_FILE_NAME.to_string(),
            yaml.to_string(),
        )])),
        ..ConfigMap::default()
    })
}

/// Build the TLS Secret from encoder side files.
pub fn build_tls_secret(
    gateway: &Gateway,
    namespace: &str,
    gateway_name: &str,
    files: &[ugwc_praxis::ConfigFile],
) -> Result<Secret, ProvisionError> {
    let mut data = BTreeMap::new();
    for file in files {
        if !valid_secret_key(&file.key) {
            return Err(ProvisionError::Msg(format!(
                "TLS file key {:?} is not a valid Secret key",
                file.key
            )));
        }
        data.insert(file.key.clone(), ByteString(file.contents.clone()));
    }
    let name = praxis_tls_secret_name(gateway_name);
    Ok(Secret {
        metadata: metadata(gateway, &name, namespace, gateway_name)?,
        data: Some(data),
        type_: Some("Opaque".into()),
        ..Secret::default()
    })
}

/// Server-side apply the Secret and ConfigMap for `gateway`.
///
/// The Secret is applied first so a restarting replica never finds the
/// mounted TLS volume missing.
pub async fn apply_praxis_config(
    client: &Client,
    gateway: &Gateway,
    output: &ugwc_praxis::EncodeOutput,
) -> Result<(), ProvisionError> {
    let namespace = gateway
        .metadata
        .namespace
        .as_deref()
        .ok_or_else(|| ProvisionError::Msg("gateway missing namespace".into()))?;
    let gateway_name = gateway.metadata.name.as_deref().unwrap_or("");
    if gateway_name.is_empty() {
        return Err(ProvisionError::Msg("gateway missing name".into()));
    }

    let pp = PatchParams::apply("ugwc").force();

    let secret = build_tls_secret(gateway, namespace, gateway_name, &output.files)?;
    let secret_name = praxis_tls_secret_name(gateway_name);
    Api::<Secret>::namespaced(client.clone(), namespace)
        .patch(&secret_name, &pp, &Patch::Apply(&secret))
        .await?;

    let config_map = build_config_map(gateway, namespace, gateway_name, &output.yaml)?;
    let config_map_name = praxis_config_map_name(gateway_name);
    Api::<ConfigMap>::namespaced(client.clone(), namespace)
        .patch(&config_map_name, &pp, &Patch::Apply(&config_map))
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::api::core::v1::ObjectReference;

    fn gateway(name: &str, namespace: &str, uid: &str) -> Gateway {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "gateway.networking.k8s.io/v1",
            "kind": "Gateway",
            "metadata": {
                "name": name,
                "namespace": namespace,
                "uid": uid,
                "generation": 1,
            },
            "spec": {
                "gatewayClassName": "ugwc-praxis",
                "listeners": [{
                    "name": "http",
                    "port": 80,
                    "protocol": "HTTP",
                }],
            },
        }))
        .expect("gateway fixture")
    }

    #[test]
    fn names_are_stable_and_fit_dns_limits() {
        assert_eq!(praxis_config_map_name("gw1"), "ugwc-dp-gw1");
        assert_eq!(praxis_tls_secret_name("gw1"), "ugwc-dp-gw1-tls");
        let long = "a".repeat(80);
        assert!(praxis_config_map_name(&long).len() <= 63);
        assert!(praxis_tls_secret_name(&long).len() <= 63);
    }

    #[test]
    fn config_map_contains_yaml_key_and_owner_reference() {
        let gw = gateway("gw1", "ns1", "uid-1");
        let cm = build_config_map(&gw, "ns1", "gw1", "listeners: []\n").unwrap();
        assert_eq!(cm.metadata.name.as_deref(), Some("ugwc-dp-gw1"));
        assert_eq!(cm.metadata.namespace.as_deref(), Some("ns1"));
        assert_eq!(
            cm.data
                .as_ref()
                .and_then(|d| d.get("praxis.yaml"))
                .map(String::as_str),
            Some("listeners: []\n")
        );
        let owner = &cm.metadata.owner_references.as_ref().unwrap()[0];
        assert_eq!(owner.kind, "Gateway");
        assert_eq!(owner.uid, "uid-1");
        assert_eq!(owner.controller, Some(true));
    }

    #[test]
    fn tls_secret_rejects_invalid_keys() {
        let gw = gateway("gw1", "ns1", "uid-1");
        let bad = ugwc_praxis::ConfigFile {
            key: "listener-tls/gw.crt".into(),
            contents: b"x".to_vec(),
        };
        assert!(build_tls_secret(&gw, "ns1", "gw1", &[bad]).is_err());
    }

    #[test]
    fn tls_secret_encodes_files() {
        let gw = gateway("gw1", "ns1", "uid-1");
        let files = vec![ugwc_praxis::ConfigFile {
            key: "listener-gw.crt".into(),
            contents: b"cert".to_vec(),
        }];
        let secret = build_tls_secret(&gw, "ns1", "gw1", &files).unwrap();
        assert_eq!(secret.metadata.name.as_deref(), Some("ugwc-dp-gw1-tls"));
        assert_eq!(
            secret.data.unwrap().get("listener-gw.crt"),
            Some(&ByteString(b"cert".to_vec()))
        );
    }

    fn encoded(yaml: &str) -> ugwc_praxis::EncodeOutput {
        ugwc_praxis::EncodeOutput {
            config: ugwc_praxis::PraxisConfig {
                listeners: Vec::new(),
                filter_chains: Vec::new(),
                insecure_options: None,
                admin: None,
            },
            yaml: yaml.into(),
            files: Vec::new(),
            unsupported: Vec::new(),
        }
    }

    #[test]
    fn config_revision_ignores_endpoint_changes() {
        let a = encoded(
            "listeners: []\nfilter_chains:\n- name: x\n  filters:\n  - filter: load_balancer\n    clusters:\n    - name: c\n      endpoints: [\"10.0.0.1:80\"]\n",
        );
        let b = encoded(
            "listeners: []\nfilter_chains:\n- name: x\n  filters:\n  - filter: load_balancer\n    clusters:\n    - name: c\n      endpoints: [\"10.0.0.2:80\", \"10.0.0.3:80\"]\n",
        );
        assert_eq!(config_revision(&a), config_revision(&b));
    }

    #[test]
    fn config_revision_changes_with_routes() {
        let a = encoded("listeners: []\n");
        let b = encoded("listeners:\n- name: x\n  address: 0.0.0.0:80\n  filter_chains: [x]\n");
        assert_ne!(config_revision(&a), config_revision(&b));
    }

    #[test]
    fn config_revision_includes_tls_files() {
        let mut a = encoded("listeners: []\n");
        let mut b = encoded("listeners: []\n");
        a.files.push(ugwc_praxis::ConfigFile {
            key: "listener-gw.crt".into(),
            contents: b"cert-a".to_vec(),
        });
        b.files.push(ugwc_praxis::ConfigFile {
            key: "listener-gw.crt".into(),
            contents: b"cert-b".to_vec(),
        });
        assert_ne!(config_revision(&a), config_revision(&b));
    }

    #[test]
    fn gateway_object_ref_matches() {
        use kube::Resource as _;
        let gw = gateway("gw1", "ns1", "uid-1");
        let reference: ObjectReference = gw.object_ref(&());
        assert_eq!(reference.kind.as_deref(), Some("Gateway"));
        assert_eq!(reference.name.as_deref(), Some("gw1"));
    }
}
