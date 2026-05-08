//! Embedded Minijinja YAML for per-gateway Praxis; applied as [`kube::core::DynamicObject`] (SSA).

use kube::api::{Api, Patch, PatchParams};
use kube::core::{ApiResource, DynamicObject, GroupVersionKind, TypeMeta};
use kube::Client;
use kube::ResourceExt;
use minijinja::Environment;
use serde::Deserialize;
use serde::Serialize;

use super::ProvisionError;

const PER_GATEWAY_PRAXIS_TEMPLATE: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/templates/per_gateway_praxis.yaml.j2"
));

/// One container or service port row for the template.
#[derive(Clone, Serialize)]
pub struct NamePort {
    pub name: String,
    pub port: i32,
}

/// Values substituted into [`PER_GATEWAY_PRAXIS_TEMPLATE`].
#[derive(Serialize)]
pub struct PerGatewayTemplateContext<'a> {
    pub namespace: &'a str,
    pub gateway_name: &'a str,
    pub gateway_uid: &'a str,
    pub gateway_class: &'a str,
    pub deployment_name: &'a str,
    pub sa_name: &'a str,
    /// `serde_json::to_string` so YAML `image: {{ proxy_image_json }}` is valid.
    pub proxy_image_json: String,
    pub xds_bootstrap_addr_json: String,
    pub gateway_scope_json: String,
    pub xds_insecure_json: String,
    pub xds_wire_json: String,
    pub gateway_name_json: String,
    pub container_ports: &'a [NamePort],
    pub service_ports: &'a [NamePort],
}

pub fn render_per_gateway_praxis(
    ctx: &PerGatewayTemplateContext<'_>,
) -> Result<String, ProvisionError> {
    let mut env = Environment::new();
    env.add_template("per_gateway_praxis", PER_GATEWAY_PRAXIS_TEMPLATE)
        .map_err(|e| ProvisionError::Msg(format!("template compile: {e}")))?;
    let tmpl = env
        .get_template("per_gateway_praxis")
        .map_err(|e| ProvisionError::Msg(format!("template get: {e}")))?;
    let value = minijinja::Value::from_serialize(ctx);
    tmpl.render(value)
        .map_err(|e| ProvisionError::Msg(format!("template render: {e}")))
}

/// Parse multi-doc YAML into JSON values (sync; safe to call before `.await` in `Send` futures).
pub fn parse_manifest_documents(
    rendered_yaml: &str,
) -> Result<Vec<serde_json::Value>, ProvisionError> {
    let mut out = Vec::new();
    for doc in serde_yaml::Deserializer::from_str(rendered_yaml) {
        let yaml_val: serde_yaml::Value = serde_yaml::Value::deserialize(doc)
            .map_err(|e| ProvisionError::Msg(format!("yaml: {e}")))?;
        let json_val = serde_json::to_value(&yaml_val)
            .map_err(|e| ProvisionError::Msg(format!("yaml→json: {e}")))?;
        out.push(json_val);
    }
    Ok(out)
}

/// Server-side apply each pre-parsed manifest as a [`DynamicObject`].
pub async fn apply_manifests_dynamic(
    client: &Client,
    namespace: &str,
    documents: Vec<serde_json::Value>,
    pp: &PatchParams,
) -> Result<(), ProvisionError> {
    for json_val in documents {
        let api_version = json_val
            .get("apiVersion")
            .and_then(|v| v.as_str())
            .ok_or_else(|| ProvisionError::Msg("manifest missing apiVersion".into()))?;
        let kind = json_val
            .get("kind")
            .and_then(|v| v.as_str())
            .ok_or_else(|| ProvisionError::Msg("manifest missing kind".into()))?;

        let tm = TypeMeta {
            api_version: api_version.to_string(),
            kind: kind.to_string(),
        };
        let gvk = GroupVersionKind::try_from(&tm)
            .map_err(|e| ProvisionError::Msg(format!("apiVersion/kind: {e}")))?;
        let ar = ApiResource::from_gvk(&gvk);

        let obj: DynamicObject = serde_json::from_value(json_val)
            .map_err(|e| ProvisionError::Msg(format!("dynamic object: {e}")))?;
        let name = obj.name_any();
        if name.is_empty() {
            return Err(ProvisionError::Msg("manifest missing metadata.name".into()));
        }

        let api: Api<DynamicObject> = Api::namespaced_with(client.clone(), namespace, &ar);
        api.patch(&name, pp, &Patch::Apply(obj)).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn template_renders_without_error() {
        let ports = [
            NamePort {
                name: "status-port".into(),
                port: 15021,
            },
            NamePort {
                name: "http".into(),
                port: 8080,
            },
        ];
        let ctx = PerGatewayTemplateContext {
            namespace: "ns1",
            gateway_name: "gw1",
            gateway_uid: "uid-1",
            gateway_class: "ugwc-praxis",
            deployment_name: "ugwc-dp-gw1",
            sa_name: "ugwc-dp-gw1-sa",
            proxy_image_json: serde_json::to_string("quay.io/example/praxis:latest").unwrap(),
            xds_bootstrap_addr_json: serde_json::to_string("ugwc.ugwc-system.svc:18000").unwrap(),
            gateway_scope_json: serde_json::to_string("ns1/gw1").unwrap(),
            xds_insecure_json: serde_json::to_string("true").unwrap(),
            xds_wire_json: serde_json::to_string("gwxds").unwrap(),
            gateway_name_json: serde_json::to_string("gw1").unwrap(),
            container_ports: &ports,
            service_ports: &ports[1..],
        };
        let out = render_per_gateway_praxis(&ctx).expect("render");
        assert!(out.contains("kind: ServiceAccount"));
        assert!(out.contains("kind: Deployment"));
        assert!(out.contains("quay.io/example/praxis:latest"));
        assert!(out.contains("containerPort: 15021"));
        let docs = parse_manifest_documents(&out).expect("parse");
        assert_eq!(docs.len(), 5);
        assert_eq!(
            docs[0].get("kind").and_then(|k| k.as_str()),
            Some("ServiceAccount")
        );
    }
}
