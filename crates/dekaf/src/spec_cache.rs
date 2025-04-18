use crate::topology::fetch_dekaf_task_auth;
use futures::future::{FutureExt, Shared};
use proto_flow::flow::MaterializationSpec;
use std::sync::Arc;
use std::time::Duration;
use std::{collections::HashMap, future::Future, pin::Pin};
use tokio::sync::Mutex;

// Result<Arc,Arc> because anyhow::Error is not Clone
type FetchOutput = Arc<Result<MaterializationSpec, anyhow::Error>>;
type BoxedFetchFuture = Pin<Box<dyn Future<Output = FetchOutput> + Send + 'static>>;

struct CacheEntry {
    insert_time: time::OffsetDateTime,
    shared_spec: Shared<BoxedFetchFuture>,
}

#[derive(Clone)]
pub struct SpecCache {
    store: Arc<Mutex<HashMap<String, CacheEntry>>>,
    ttl: Duration,
    client: flow_client::Client,
    data_plane_fqdn: String,
    data_plane_signer: Arc<jsonwebtoken::EncodingKey>,
}

impl SpecCache {
    pub fn new(
        ttl: Duration,
        client: flow_client::Client,
        data_plane_fqdn: String,
        data_plane_signer: jsonwebtoken::EncodingKey,
    ) -> Self {
        SpecCache {
            store: Arc::new(Mutex::new(HashMap::new())),
            ttl,
            client,
            data_plane_fqdn,
            data_plane_signer: Arc::new(data_plane_signer),
        }
    }

    #[tracing::instrument(skip(self))]
    pub async fn get(&self, task_name: &str) -> FetchOutput {
        let shared_fut = async {
            let mut store_guard = self.store.lock().await;

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
            let client_clone = self.client.clone();
            let fqdn_clone = self.data_plane_fqdn.clone();
            let signer_clone = Arc::clone(&self.data_plane_signer);

            // Capture owned clones in the async block
            let fetch_future = async move {
                fetch_dekaf_task_auth(&client_clone, &fetch_task_name, &fqdn_clone, &signer_clone)
                    .map(|res| Arc::new(res.map(|(_, _, _, _, spec)| spec)))
                    .await
            };

            let boxed_fetch_future: BoxedFetchFuture = Box::pin(fetch_future);
            let shared_fetch_future = boxed_fetch_future.shared();

            store_guard.insert(
                task_name.to_string(),
                CacheEntry {
                    insert_time: time::OffsetDateTime::now_utc(),
                    shared_spec: shared_fetch_future.clone(),
                },
            );

            shared_fetch_future
        }
        .await;

        shared_fut.await
    }

    #[tracing::instrument(skip(self))]
    pub async fn prune_expired(&self) {
        let mut store_guard = self.store.lock().await;
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
