//! Shared kube-runtime watch streams: generation predicates, managedFields pruning, default backoff.

use std::pin::Pin;

use futures::{Stream, TryStreamExt};
use kube::api::{Api, PartialObjectMeta};
use kube::runtime::watcher::{self, Config as WatcherConfig};
use kube::runtime::{metadata_watcher, predicates, WatchStreamExt};
use kube::Resource;
use serde::de::DeserializeOwned;
use tokio_util::sync::CancellationToken;
use tracing::warn;

/// Default watcher config for Gateway API controllers.
#[must_use]
pub fn watcher_config() -> WatcherConfig {
    WatcherConfig::default().any_semantic()
}

/// Clear `managedFields` before objects are handled or cached.
pub fn strip_managed_fields<K: Resource>(obj: &mut K) {
    obj.meta_mut().managed_fields = None;
}

fn strip_meta_managed_fields<K>(obj: &mut PartialObjectMeta<K>) {
    obj.metadata.managed_fields = None;
}

/// Full-resource watch: prune → backoff → applied → generation filter.
pub fn full_applied_stream<K>(
    api: Api<K>,
    cfg: WatcherConfig,
) -> Pin<Box<dyn Stream<Item = Result<K, watcher::Error>> + Send>>
where
    K: Resource<DynamicType: Default + Eq + std::hash::Hash>
        + Clone
        + DeserializeOwned
        + std::fmt::Debug
        + Send
        + Sync
        + 'static,
{
    Box::pin(
        watcher::watcher(api, cfg)
            .modify(strip_managed_fields)
            .default_backoff()
            .applied_objects()
            .predicate_filter(predicates::generation, Default::default()),
    )
}

/// Metadata-only watch for child resources (routes, policies, etc.).
pub fn meta_applied_stream<K>(
    api: Api<K>,
    cfg: WatcherConfig,
) -> Pin<Box<dyn Stream<Item = Result<PartialObjectMeta<K>, watcher::Error>> + Send>>
where
    K: Resource<DynamicType: Default + Eq + std::hash::Hash>
        + Clone
        + DeserializeOwned
        + std::fmt::Debug
        + Send
        + Sync
        + 'static,
{
    Box::pin(
        metadata_watcher(api, cfg)
            .modify(strip_meta_managed_fields)
            .default_backoff()
            .applied_objects()
            .predicate_filter(predicates::generation, Default::default()),
    )
}

/// Drive a watch stream until `token` is cancelled or the sender closes.
pub async fn run_stream_loop<T, F, Fut>(
    token: CancellationToken,
    mut stream: Pin<Box<dyn Stream<Item = Result<T, watcher::Error>> + Send>>,
    log_label: &'static str,
    mut on_item: F,
) where
    T: Send,
    F: FnMut(T) -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    loop {
        tokio::select! {
            _ = token.cancelled() => break,
            ev = stream.try_next() => {
                match ev {
                    Ok(Some(item)) => {
                        if !on_item(item).await {
                            break;
                        }
                    }
                    Ok(None) => break,
                    Err(e) => warn!(error = %e, stream = log_label, "watch stream"),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use kube::runtime::predicates;

    use super::*;

    #[test]
    fn generation_predicate_ignores_duplicate() {
        use k8s_openapi::api::core::v1::Pod;
        use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;

        let mut pod = Pod {
            metadata: ObjectMeta {
                generation: Some(1),
                ..Default::default()
            },
            ..Default::default()
        };
        let h1 = predicates::generation(&pod);
        pod.metadata.generation = Some(1);
        assert_eq!(predicates::generation(&pod), h1);
        pod.metadata.generation = Some(2);
        assert_ne!(predicates::generation(&pod), h1);
    }
}
