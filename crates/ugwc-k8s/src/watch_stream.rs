//! Shared kube-runtime watch streams: generation predicates, managedFields pruning, default backoff.

use std::collections::HashMap;
use std::hash::Hash;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use futures::{Stream, TryStreamExt};
use kube::api::Api;
use kube::runtime::reflector::{self, Store};
use kube::runtime::watcher::{self, Config as WatcherConfig, Event};
use kube::runtime::{predicates, WatchStreamExt};
use kube::Resource;
use serde::de::DeserializeOwned;
use tokio_util::sync::CancellationToken;
use tracing::warn;

/// Default watcher config for Gateway API controllers.
/// Uses streaming lists (K8s >= 1.27) for efficient initial sync without full LIST.
#[must_use]
pub fn watcher_config() -> WatcherConfig {
    WatcherConfig::default().streaming_lists()
}

/// Clear `managedFields` before objects are handled or cached.
pub fn strip_managed_fields<K: Resource>(obj: &mut K) {
    obj.meta_mut().managed_fields = None;
}

/// Apply or delete notification from a Kubernetes watch.
#[derive(Clone, Debug)]
pub enum WatchItem<K> {
    Applied(K),
    /// Last known object body from `Event::Delete`.
    Deleted(K),
}

fn object_cache_key<K: Resource>(obj: &K) -> (String, String)
where
    K::DynamicType: Default,
{
    (
        obj.meta().namespace.clone().unwrap_or_default(),
        obj.meta().name.clone().unwrap_or_default(),
    )
}

fn map_event<K>(last_gen: &Arc<Mutex<HashMap<(String, String), u64>>>, ev: Event<K>) -> Option<WatchItem<K>>
where
    K: Resource<DynamicType: Default>,
{
    match ev {
        Event::Apply(obj) | Event::InitApply(obj) => {
            let key = object_cache_key(&obj);
            let hash = predicates::generation(&obj).unwrap_or(0);
            let mut last = last_gen.lock().expect("watch generation cache");
            if last.get(&key) == Some(&hash) {
                return None;
            }
            last.insert(key, hash);
            Some(WatchItem::Applied(obj))
        }
        Event::Delete(obj) => {
            last_gen
                .lock()
                .expect("watch generation cache")
                .remove(&object_cache_key(&obj));
            Some(WatchItem::Deleted(obj))
        }
        Event::Init | Event::InitDone => None,
    }
}

/// Poll inner watch events, map to [`WatchItem`]; loops over filtered-out events until one
/// maps to a value or the inner stream returns Pending/None.
fn demux_poll<T, Map>(
    mut inner: Pin<&mut (dyn Stream<Item = Result<Event<T>, watcher::Error>> + Send)>,
    last_gen: &Arc<Mutex<HashMap<(String, String), u64>>>,
    cx: &mut Context<'_>,
    map: Map,
) -> Poll<Option<Result<WatchItem<T>, watcher::Error>>>
where
    Map: Fn(&Arc<Mutex<HashMap<(String, String), u64>>>, Event<T>) -> Option<WatchItem<T>>,
{
    loop {
        match inner.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(ev))) => {
                if let Some(item) = map(last_gen, ev) {
                    return Poll::Ready(Some(Ok(item)));
                }
            }
            Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => return Poll::Ready(None),
            Poll::Pending => return Poll::Pending,
        }
    }
}

struct WatchDemuxStream<T> {
    inner: Pin<Box<dyn Stream<Item = Result<Event<T>, watcher::Error>> + Send>>,
    last_gen: Arc<Mutex<HashMap<(String, String), u64>>>,
    map_fn: fn(&Arc<Mutex<HashMap<(String, String), u64>>>, Event<T>) -> Option<WatchItem<T>>,
}

impl<K> Stream for WatchDemuxStream<K>
where
    K: Resource<DynamicType: Default> + Send + 'static,
{
    type Item = Result<WatchItem<K>, watcher::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.as_mut().get_mut();
        demux_poll(this.inner.as_mut(), &this.last_gen, cx, this.map_fn)
    }
}

fn reflector_watch_stream_with_map<K>(
    api: Api<K>,
    cfg: WatcherConfig,
    writer: reflector::store::Writer<K>,
    map_fn: fn(&Arc<Mutex<HashMap<(String, String), u64>>>, Event<K>) -> Option<WatchItem<K>>,
) -> (
    Store<K>,
    Pin<Box<dyn Stream<Item = Result<WatchItem<K>, watcher::Error>> + Send>>,
)
where
    K: Resource<DynamicType: Default + Eq + Hash + Clone>
        + Clone
        + DeserializeOwned
        + std::fmt::Debug
        + Send
        + Sync
        + 'static,
{
    let store = writer.as_reader();
    let last_gen = Arc::new(Mutex::new(HashMap::new()));
    let inner: Pin<Box<dyn Stream<Item = Result<Event<K>, watcher::Error>> + Send>> = Box::pin(
        reflector::reflector(
            writer,
            watcher::watcher(api, cfg).modify(strip_managed_fields),
        )
        .default_backoff(),
    );
    let stream = Box::pin(WatchDemuxStream {
        inner,
        last_gen,
        map_fn,
    });
    (store, stream)
}

/// Full-resource watch with reflector: prune → reflector(store) → backoff → generation dedup.
///
/// The reflector populates the store with every event; the generation dedup downstream
/// controls whether the handler fires, not whether the store updates.
pub fn reflector_full_watch_stream<K>(
    api: Api<K>,
    cfg: WatcherConfig,
    writer: reflector::store::Writer<K>,
) -> (
    Store<K>,
    Pin<Box<dyn Stream<Item = Result<WatchItem<K>, watcher::Error>> + Send>>,
)
where
    K: Resource<DynamicType: Default + Eq + Hash + Clone>
        + Clone
        + DeserializeOwned
        + std::fmt::Debug
        + Send
        + Sync
        + 'static,
{
    let store = writer.as_reader();
    let last_gen = Arc::new(Mutex::new(HashMap::new()));
    let inner: Pin<Box<dyn Stream<Item = Result<Event<K>, watcher::Error>> + Send>> = Box::pin(
        reflector::reflector(
            writer,
            watcher::watcher(api, cfg).modify(strip_managed_fields),
        )
        .default_backoff(),
    );
    let stream = Box::pin(WatchDemuxStream {
        inner,
        last_gen,
        map_fn: map_event::<K>,
    });
    (store, stream)
}

fn hash_data_map(data: &std::collections::BTreeMap<String, String>) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    let mut keys: Vec<_> = data.keys().collect();
    keys.sort();
    for k in keys {
        k.hash(&mut hasher);
        data[k].hash(&mut hasher);
    }
    hasher.finish()
}

fn configmap_data_fingerprint(cm: &k8s_openapi::api::core::v1::ConfigMap) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    cm.metadata.generation.unwrap_or(0).hash(&mut hasher);
    if let Some(data) = &cm.data {
        hash_data_map(data).hash(&mut hasher);
    }
    hasher.finish()
}

fn secret_data_fingerprint(sec: &k8s_openapi::api::core::v1::Secret) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    sec.metadata.generation.unwrap_or(0).hash(&mut hasher);
    if let Some(data) = &sec.data {
        let mut keys: Vec<_> = data.keys().collect();
        keys.sort();
        for k in keys {
            k.hash(&mut hasher);
            data[k].0.hash(&mut hasher);
        }
    }
    hasher.finish()
}

fn map_event_with_hash<K>(
    last_gen: &Arc<Mutex<HashMap<(String, String), u64>>>,
    ev: Event<K>,
    hash_fn: fn(&K) -> u64,
) -> Option<WatchItem<K>>
where
    K: Resource<DynamicType: Default>,
{
    match ev {
        Event::Apply(obj) | Event::InitApply(obj) => {
            let key = object_cache_key(&obj);
            let hash = hash_fn(&obj);
            let mut last = last_gen.lock().expect("watch generation cache");
            if last.get(&key) == Some(&hash) {
                return None;
            }
            last.insert(key, hash);
            Some(WatchItem::Applied(obj))
        }
        Event::Delete(obj) => {
            last_gen
                .lock()
                .expect("watch generation cache")
                .remove(&object_cache_key(&obj));
            Some(WatchItem::Deleted(obj))
        }
        Event::Init | Event::InitDone => None,
    }
}

fn map_configmap_event(
    last_gen: &Arc<Mutex<HashMap<(String, String), u64>>>,
    ev: Event<k8s_openapi::api::core::v1::ConfigMap>,
) -> Option<WatchItem<k8s_openapi::api::core::v1::ConfigMap>> {
    map_event_with_hash(last_gen, ev, configmap_data_fingerprint)
}

fn map_secret_event(
    last_gen: &Arc<Mutex<HashMap<(String, String), u64>>>,
    ev: Event<k8s_openapi::api::core::v1::Secret>,
) -> Option<WatchItem<k8s_openapi::api::core::v1::Secret>> {
    map_event_with_hash(last_gen, ev, secret_data_fingerprint)
}

/// ConfigMap watch with data-content fingerprint dedup (CA hot-reload).
pub fn reflector_configmap_watch_stream(
    api: Api<k8s_openapi::api::core::v1::ConfigMap>,
    cfg: WatcherConfig,
    writer: reflector::store::Writer<k8s_openapi::api::core::v1::ConfigMap>,
) -> (
    Store<k8s_openapi::api::core::v1::ConfigMap>,
    Pin<Box<dyn Stream<Item = Result<WatchItem<k8s_openapi::api::core::v1::ConfigMap>, watcher::Error>> + Send>>,
) {
    reflector_watch_stream_with_map(api, cfg, writer, map_configmap_event)
}

/// Secret watch with data-content fingerprint dedup (BackendTLS CA material).
pub fn reflector_secret_watch_stream(
    api: Api<k8s_openapi::api::core::v1::Secret>,
    cfg: WatcherConfig,
    writer: reflector::store::Writer<k8s_openapi::api::core::v1::Secret>,
) -> (
    Store<k8s_openapi::api::core::v1::Secret>,
    Pin<Box<dyn Stream<Item = Result<WatchItem<k8s_openapi::api::core::v1::Secret>, watcher::Error>> + Send>>,
) {
    reflector_watch_stream_with_map(api, cfg, writer, map_secret_event)
}

/// Drive a delete-aware watch stream until `token` is cancelled or the handler returns false.
pub async fn run_watch_loop<K, F, Fut>(
    token: CancellationToken,
    mut stream: Pin<Box<dyn Stream<Item = Result<WatchItem<K>, watcher::Error>> + Send>>,
    log_label: &'static str,
    mut on_item: F,
) where
    K: Send,
    F: FnMut(WatchItem<K>) -> Fut,
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
    use k8s_openapi::api::core::v1::Pod;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use kube::runtime::predicates;

    use super::*;

    fn test_pod(name: &str, generation: i64) -> Pod {
        Pod {
            metadata: ObjectMeta {
                name: Some(name.into()),
                namespace: Some("default".into()),
                generation: Some(generation),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    #[test]
    fn map_event_demux_apply_and_delete() {
        let last_gen = Arc::new(Mutex::new(HashMap::new()));
        let pod = test_pod("a", 1);
        assert!(matches!(
            map_event(&last_gen, Event::Apply(pod.clone())),
            Some(WatchItem::Applied(_))
        ));
        assert!(matches!(
            map_event(&last_gen, Event::Delete(pod)),
            Some(WatchItem::Deleted(_))
        ));
    }

    #[test]
    fn map_event_generation_skips_duplicate_applied_not_delete() {
        let last_gen = Arc::new(Mutex::new(HashMap::new()));
        let pod = test_pod("b", 2);
        assert!(map_event(&last_gen, Event::Apply(pod.clone())).is_some());
        assert!(map_event(&last_gen, Event::Apply(pod.clone())).is_none());
        assert!(map_event(&last_gen, Event::Delete(pod)).is_some());
    }

    #[test]
    fn generation_predicate_ignores_duplicate() {
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

    #[test]
    fn configmap_data_fingerprint_changes_when_ca_crt_changes() {
        use k8s_openapi::api::core::v1::ConfigMap;
        use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;

        let mut cm = ConfigMap {
            metadata: ObjectMeta {
                name: Some("ca".into()),
                namespace: Some("ns".into()),
                generation: Some(1),
                ..Default::default()
            },
            data: Some(std::collections::BTreeMap::from([(
                "ca.crt".into(),
                "valid".into(),
            )])),
            ..Default::default()
        };
        let h1 = configmap_data_fingerprint(&cm);
        cm.data.as_mut().unwrap().insert("ca.crt".into(), String::new());
        let h2 = configmap_data_fingerprint(&cm);
        assert_ne!(h1, h2);

        let last = Arc::new(Mutex::new(HashMap::new()));
        let mut valid_cm = cm.clone();
        valid_cm.data.as_mut().unwrap().insert("ca.crt".into(), "valid".into());
        assert!(map_configmap_event(&last, Event::Apply(valid_cm)).is_some());
        assert!(map_configmap_event(&last, Event::Apply(cm.clone())).is_some());
        assert!(map_configmap_event(&last, Event::Apply(cm)).is_none());
    }
}
