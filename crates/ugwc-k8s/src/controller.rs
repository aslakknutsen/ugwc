//! kube-runtime watch over `Gateway` (Phase 2A minimal; Phase 3 expands graph).

use std::sync::Arc;

use futures::StreamExt;
use kube::runtime::watcher::{watcher, Config as WatcherConfig};
use kube::runtime::WatchStreamExt;
use kube::Api;
use kube::Client;
use kube::ResourceExt;
use tracing::{info, instrument, warn};

use crate::resources::Gateway;

pub type ReconcileFn = Arc<dyn Fn(Gateway) -> Result<(), String> + Send + Sync>;

/// Observe `Gateway` objects in `namespace` until cancelled.
#[instrument(skip(client, shutdown, reconcile))]
pub async fn run_gateway_controller(
    client: Client,
    namespace: String,
    shutdown: tokio_util::sync::CancellationToken,
    reconcile: ReconcileFn,
) {
    let api: Api<Gateway> = Api::namespaced(client, &namespace);
    let cfg = WatcherConfig::default().any_semantic();
    let mut stream = Box::pin(watcher(api, cfg).applied_objects());

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                info!("gateway watcher shutdown");
                break;
            }
            ev = stream.next() => {
                match ev {
                    Some(Ok(gw)) => {
                        let name = gw.name_any();
                        let ns = gw.namespace().unwrap_or_default();
                        if let Err(e) = reconcile(gw) {
                            warn!(error = %e, %ns, %name, "reconcile");
                        }
                    }
                    Some(Err(e)) => warn!(error = %e, "gateway watch stream"),
                    None => break,
                }
            }
        }
    }
}
