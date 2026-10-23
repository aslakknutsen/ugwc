//! Kubernetes `Lease` leader election for HA ugwc replicas.

use std::sync::Arc;
use std::time::Duration;

use k8s_openapi::api::coordination::v1::Lease;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{MicroTime, ObjectMeta};
use k8s_openapi::jiff::Timestamp;
use kube::api::{Api, Patch, PatchParams};
use kube::Client;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::leader::LeaderState;
use crate::metrics;

const LEASE_NAME: &str = "ugwc-leader";
const LEASE_DURATION_SECS: i32 = 15;
const RENEW_INTERVAL: Duration = Duration::from_secs(5);

fn lease_expired(renew_time: &MicroTime, lease_duration_secs: i32) -> bool {
    let span = k8s_openapi::jiff::Span::new().seconds(lease_duration_secs as i64);
    Timestamp::now() > renew_time.0 + span
}

async fn try_acquire_or_renew(api: &Api<Lease>, identity: &str) -> Result<bool, kube::Error> {
    let now = Timestamp::now();
    let micro = MicroTime(now);

    match api.get(LEASE_NAME).await {
        Ok(mut lease) => {
            let spec = lease.spec.get_or_insert_with(Default::default);
            let duration = spec.lease_duration_seconds.unwrap_or(LEASE_DURATION_SECS);
            let holder = spec.holder_identity.as_deref();
            let expired = spec
                .renew_time
                .as_ref()
                .map(|t| lease_expired(t, duration))
                .unwrap_or(true);
            let can_take = holder == Some(identity) || holder.is_none() || expired;
            if !can_take {
                return Ok(false);
            }
            spec.holder_identity = Some(identity.to_string());
            spec.lease_duration_seconds = Some(LEASE_DURATION_SECS);
            spec.acquire_time.get_or_insert(micro.clone());
            spec.renew_time = Some(micro);
            api.replace(LEASE_NAME, &Default::default(), &lease).await?;
            Ok(true)
        }
        Err(kube::Error::Api(err)) if err.code == 404 => {
            let lease = Lease {
                metadata: ObjectMeta {
                    name: Some(LEASE_NAME.into()),
                    ..Default::default()
                },
                spec: Some(k8s_openapi::api::coordination::v1::LeaseSpec {
                    holder_identity: Some(identity.to_string()),
                    lease_duration_seconds: Some(LEASE_DURATION_SECS),
                    acquire_time: Some(micro.clone()),
                    renew_time: Some(micro),
                    ..Default::default()
                }),
            };
            api.create(&Default::default(), &lease).await?;
            Ok(true)
        }
        Err(e) => Err(e),
    }
}

async fn release_lease(api: &Api<Lease>, identity: &str) {
    let Ok(mut lease) = api.get(LEASE_NAME).await else {
        return;
    };
    let Some(spec) = lease.spec.as_mut() else {
        return;
    };
    if spec.holder_identity.as_deref() != Some(identity) {
        return;
    }
    spec.holder_identity = None;
    spec.renew_time = None;
    if let Err(e) = api
        .patch(LEASE_NAME, &PatchParams::default(), &Patch::Merge(&lease))
        .await
    {
        warn!(error = %e, "release leader lease");
    } else {
        info!("released leader lease");
    }
}

/// Start lease renewal loop; returns shared [`LeaderState`] updated by the task.
pub fn spawn_lease_election(
    client: Client,
    namespace: String,
    identity: String,
    shutdown: CancellationToken,
) -> (LeaderState, Arc<LeaseElectionHandle>) {
    let state = LeaderState::new_follower();
    let handle = Arc::new(LeaseElectionHandle {
        client,
        namespace,
        identity,
        state: state.clone(),
        shutdown,
    });
    let task = handle.clone();
    ugwc_k8s::spawn_with_trace_context(async move {
        task.run().await;
    });
    (state, handle)
}

/// Handle used to release the lease during graceful shutdown.
pub struct LeaseElectionHandle {
    client: Client,
    namespace: String,
    identity: String,
    state: LeaderState,
    shutdown: CancellationToken,
}

impl LeaseElectionHandle {
    async fn run(self: Arc<Self>) {
        let api: Api<Lease> = Api::namespaced(self.client.clone(), &self.namespace);
        loop {
            tokio::select! {
                _ = self.shutdown.cancelled() => break,
                _ = tokio::time::sleep(RENEW_INTERVAL) => {}
            }
            if self.shutdown.is_cancelled() {
                break;
            }
            match try_acquire_or_renew(&api, &self.identity).await {
                Ok(true) => {
                    self.state.set_is_leader(true);
                    metrics::set_leader_active(true);
                    debug!("leader lease held");
                }
                Ok(false) => {
                    self.state.set_is_leader(false);
                    metrics::set_leader_active(false);
                }
                Err(e) => {
                    warn!(error = %e, "leader lease renew");
                    self.state.set_is_leader(false);
                    metrics::set_leader_active(false);
                }
            }
        }
        release_lease(&api, &self.identity).await;
        self.state.set_is_leader(false);
        metrics::set_leader_active(false);
    }

    pub async fn release(&self) {
        self.shutdown.cancel();
        let api: Api<Lease> = Api::namespaced(self.client.clone(), &self.namespace);
        release_lease(&api, &self.identity).await;
        self.state.set_is_leader(false);
        metrics::set_leader_active(false);
    }
}
