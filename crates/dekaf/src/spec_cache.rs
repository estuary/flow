use crate::topology::fetch_dekaf_task_auth;
use futures::future::{FutureExt, Shared};
use proto_flow::flow::MaterializationSpec;
use std::sync::Arc;
use std::time::Duration;
use std::{collections::HashMap, future::Future, pin::Pin};

type FetchOutput = Arc<Result<MaterializationSpec, anyhow::Error>>;
type BoxedFetchFuture = Pin<Box<dyn Future<Output = FetchOutput> + Send + 'static>>;

struct CacheEntry {
    insert_time: time::OffsetDateTime,
    shared_spec: Shared<BoxedFetchFuture>,
}

pub struct SpecCache {
    store: std::sync::Mutex<HashMap<String, CacheEntry>>,
    ttl: Duration,
    client: flow_client::Client,
    data_plane_fqdn: String,
    data_plane_signer: jsonwebtoken::EncodingKey,
}

impl SpecCache {
    pub fn new(
        ttl: Duration,
        client: flow_client::Client,
        data_plane_fqdn: String,
        data_plane_signer: jsonwebtoken::EncodingKey,
    ) -> Self {
        SpecCache {
            store: std::sync::Mutex::new(HashMap::new()),
            ttl,
            client,
            data_plane_fqdn,
            data_plane_signer: data_plane_signer,
        }
    }

    #[tracing::instrument(skip(self))]
    pub fn get(self: &std::sync::Arc<Self>, task_name: &str) -> Shared<BoxedFetchFuture> {
        let mut store_guard = self.store.lock().unwrap();

        if let Some(CacheEntry {
            insert_time,
            shared_spec,
        }) = store_guard.get(task_name)
        {
            if time::OffsetDateTime::now_utc() - *insert_time < self.ttl {
                tracing::debug!(task_name, "Cache hit");
                return shared_spec.clone(); // Return cloned future on fresh hit
            } else {
                tracing::debug!(task_name, "Cache stale");
                store_guard.remove(task_name);
            }
        } else {
            tracing::debug!(task_name, "Cache miss");
        }

        let fetch_task_name = task_name.to_string();
        let cache = self.clone();

        let shared_fetch_future = async move {
            fetch_dekaf_task_auth(
                &cache.client,
                &fetch_task_name,
                &cache.data_plane_fqdn,
                &cache.data_plane_signer,
            )
            .map(|res| Arc::new(res.map(|(_, _, _, _, spec)| spec)))
            .await
        }
        .boxed()
        .shared();

        store_guard.insert(
            task_name.to_string(),
            CacheEntry {
                insert_time: time::OffsetDateTime::now_utc(),
                shared_spec: shared_fetch_future.clone(),
            },
        );

        shared_fetch_future
    }

    #[tracing::instrument(skip(self))]
    pub fn prune_expired(self: &std::sync::Arc<Self>) {
        let mut store_guard = self.store.lock().unwrap();
        let now = time::OffsetDateTime::now_utc();
        let current_len = store_guard.len();

        store_guard.retain(|_, entry| now - entry.insert_time < self.ttl);

        tracing::info!(
            pruned = (current_len - store_guard.len()),
            remaining = store_guard.len(),
            "Pruned expired cache entries"
        );
    }
}
