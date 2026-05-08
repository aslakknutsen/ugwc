//! Static JSON map: `GatewayClass.spec.controllerName` → implementation profile.

use std::collections::HashMap;
use std::path::Path;

use serde::{Deserialize, Serialize};

/// How workloads are (or are not) created for gateways using this controller name.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DataPlaneKind {
    External,
    PerGatewayPraxis,
    /// Stub: grouping key not defined; no cluster mutations.
    SharedDeployment,
}

/// Default subscription hint for clients; xDS uses `node.id` (see design doc).
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SubscriptionScope {
    #[default]
    All,
    OneGateway,
    Some,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ImplementationProfile {
    pub data_plane: DataPlaneKind,
    #[serde(default)]
    pub subscription_default: SubscriptionScope,
    #[serde(default)]
    pub proxy_image: Option<String>,
}

impl ImplementationProfile {
    #[must_use]
    pub fn default_external() -> Self {
        Self {
            data_plane: DataPlaneKind::External,
            subscription_default: SubscriptionScope::All,
            proxy_image: None,
        }
    }
}

/// File shape for `UGWC_IMPLEMENTATION_PROFILES_PATH`.
#[derive(Debug, Deserialize)]
struct ProfilesFile {
    profiles: HashMap<String, ImplementationProfile>,
}

/// Load profiles from JSON (`profiles` object keyed by `controllerName`).
pub fn load_from_path(path: &Path) -> Result<HashMap<String, ImplementationProfile>, String> {
    let raw = std::fs::read_to_string(path).map_err(|e| format!("read {path:?}: {e}"))?;
    let file: ProfilesFile =
        serde_json::from_str(&raw).map_err(|e| format!("parse profiles JSON: {e}"))?;
    Ok(file.profiles)
}

#[must_use]
pub fn resolve_for_controller(
    profiles: &HashMap<String, ImplementationProfile>,
    controller_name: Option<&str>,
) -> ImplementationProfile {
    let Some(cn) = controller_name.filter(|s| !s.is_empty()) else {
        return ImplementationProfile::default_external();
    };
    profiles
        .get(cn)
        .cloned()
        .unwrap_or_else(ImplementationProfile::default_external)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn load_profiles_json_roundtrip() {
        let dir = std::env::temp_dir().join(format!(
            "ugwc-impl-prof-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("profiles.json");
        fs::write(
            &path,
            r#"{"profiles":{"example.com/foo":{"data_plane":"external","subscription_default":"all"}}}"#,
        )
        .unwrap();
        let m = load_from_path(&path).unwrap();
        let p = resolve_for_controller(&m, Some("example.com/foo"));
        assert_eq!(p.data_plane, DataPlaneKind::External);
        fs::remove_dir_all(&dir).ok();
    }
}
