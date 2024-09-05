use super::App;
use anyhow::Context;
use std::sync::Arc;

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Request {
    // # JWT token to be authorized and signed.
    token: String,
}

#[derive(Default, Debug, serde::Serialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Response {
    // # JWT token which has been authorized for use.
    token: String,
    // # Address of Gazette brokers for the issued token.
    broker_address: String,
    // # Number of milliseconds to wait before retrying the request.
    // Non-zero if and only if token is not set.
    retry_millis: u64,
}

#[axum::debug_handler]
pub async fn authorize_task(
    axum::extract::State(app): axum::extract::State<Arc<App>>,
    axum::Json(request): axum::Json<Request>,
) -> axum::response::Response {
    super::wrap(async move { do_authorize_task(&app, &request).await }).await
}

#[tracing::instrument(skip_all, err(level = tracing::Level::WARN))]
async fn do_authorize_task(app: &App, Request { token }: &Request) -> anyhow::Result<Response> {
    let jsonwebtoken::TokenData { header, mut claims }: jsonwebtoken::TokenData<
        proto_gazette::Claims,
    > = {
        // In this pass we do not validate the signature,
        // because we don't yet know which data-plane the JWT is signed by.
        let empty_key = jsonwebtoken::DecodingKey::from_secret(&[]);
        let mut validation = jsonwebtoken::Validation::default();
        validation.insecure_disable_signature_validation();
        jsonwebtoken::decode(token, &empty_key, &validation)
    }?;
    tracing::debug!(?claims, ?header, "decoded authorization request");

    let shard_id = claims.sub.as_str();
    if shard_id.is_empty() {
        anyhow::bail!("missing required shard ID (`sub` claim)");
    }

    let shard_data_plane_fqdn = claims.iss.as_str();
    if shard_data_plane_fqdn.is_empty() {
        anyhow::bail!("missing required shard data-plane FQDN (`iss` claim)");
    }

    let journal_name_or_prefix = labels::expect_one(claims.sel.include(), "name")?.to_owned();

    // Require the request was signed with the AUTHORIZE capability,
    // and then strip this capability before issuing a response token.
    if claims.cap & proto_flow::capability::AUTHORIZE == 0 {
        anyhow::bail!("missing required AUTHORIZE capability: {}", claims.cap);
    }
    claims.cap &= !proto_flow::capability::AUTHORIZE;

    // Validate and match the requested capabilities to a corresponding role.
    let required_role = match claims.cap {
        proto_gazette::capability::LIST | proto_gazette::capability::READ => {
            models::Capability::Read
        }
        proto_gazette::capability::APPLY | proto_gazette::capability::APPEND => {
            models::Capability::Write
        }
        cap => {
            anyhow::bail!("capability {cap} cannot be authorized by this service");
        }
    };

    // Resolve the authorization snapshot against which this request is evaluated.
    let snapshot = app.snapshot.read().unwrap();

    let taken_unix = snapshot
        .taken
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    // If the snapshot is too old then the client MUST retry.
    if claims.iat > taken_unix + MAX_SNAPSHOT_INTERVAL.as_secs() {
        begin_refresh(snapshot, &app.snapshot);

        return Ok(Response {
            retry_millis: jitter(),
            ..Default::default()
        });
    }

    match evaluate_authorization(
        &snapshot,
        shard_id,
        shard_data_plane_fqdn,
        token,
        &journal_name_or_prefix,
        required_role,
    ) {
        Ok((encoding_key, data_plane_fqdn, broker_address)) => {
            claims.iss = data_plane_fqdn;
            claims.exp = claims.iat + 3_600; // One hour.

            let token = jsonwebtoken::encode(&header, &claims, &encoding_key)
                .context("failed to encode authorized JWT")?;

            Ok(Response {
                broker_address,
                token,
                ..Default::default()
            })
        }
        Err(err) if taken_unix > claims.iat => {
            // The snapshot was taken AFTER the authorization request was minted,
            // which means the request cannot have prior knowledge of upcoming
            // state re-configurations, and this is a terminal error.
            Err(err)
        }
        Err(_) => {
            let retry_millis = if let Some(remaining) =
                MIN_SNAPSHOT_INTERVAL.checked_sub(snapshot.taken.elapsed().unwrap_or_default())
            {
                // Our current snapshot isn't old enough.
                remaining.as_millis() as u64
            } else {
                begin_refresh(snapshot, &app.snapshot);
                0
            } + jitter();

            Ok(Response {
                retry_millis,
                ..Default::default()
            })
        }
    }
}

fn evaluate_authorization(
    Snapshot {
        taken: _,
        collections,
        data_planes,
        role_grants,
        tasks,
        refresh_tx: _,
    }: &Snapshot,
    shard_id: &str,
    shard_data_plane_fqdn: &str,
    token: &str,
    journal_name_or_prefix: &str,
    required_role: models::Capability,
) -> anyhow::Result<(jsonwebtoken::EncodingKey, String, String)> {
    // Map `claims.sub`, a Shard ID, into its task.
    let task = tasks
        .binary_search_by(|task| {
            if shard_id.starts_with(&task.shard_template_id) {
                std::cmp::Ordering::Equal
            } else {
                task.shard_template_id.as_str().cmp(shard_id)
            }
        })
        .ok()
        .map(|index| &tasks[index]);

    // Map `claims.iss`, a data-plane FQDN, into its task-matched data-plane.
    let task_data_plane = task.and_then(|task| {
        data_planes
            .get_by_key(&task.data_plane_id)
            .filter(|data_plane| data_plane.data_plane_fqdn == shard_data_plane_fqdn)
    });

    let (Some(task), Some(task_data_plane)) = (task, task_data_plane) else {
        anyhow::bail!(
            "task shard {shard_id} within data-plane {shard_data_plane_fqdn} is not known"
        )
    };

    // Attempt to find an HMAC key of this data-plane which validates against the request token.
    let validation = jsonwebtoken::Validation::default();
    let mut verified = false;

    for hmac_key in &task_data_plane.hmac_keys {
        let key = jsonwebtoken::DecodingKey::from_base64_secret(hmac_key)
            .context("invalid data-plane hmac key")?;

        if jsonwebtoken::decode::<proto_gazette::Claims>(token, &key, &validation).is_ok() {
            verified = true;
            break;
        }
    }
    if !verified {
        anyhow::bail!("no data-plane keys validated against the token signature");
    }

    // Map a required `name` journal label selector into its collection.
    let Some(collection) = collections
        .binary_search_by(|collection| {
            if journal_name_or_prefix.starts_with(&collection.journal_template_name) {
                std::cmp::Ordering::Equal
            } else {
                collection
                    .journal_template_name
                    .as_str()
                    .cmp(journal_name_or_prefix)
            }
        })
        .ok()
        .map(|index| &collections[index])
    else {
        anyhow::bail!("journal name or prefix {journal_name_or_prefix} is not known");
    };

    let Some(collection_data_plane) = data_planes.get_by_key(&collection.data_plane_id) else {
        anyhow::bail!(
            "collection data-plane {} not found",
            collection.data_plane_id
        );
    };

    let ops_kind = match task.spec_type {
        models::CatalogType::Capture => "capture",
        models::CatalogType::Collection => "derivation",
        models::CatalogType::Materialization => "materialization",
        models::CatalogType::Test => "test",
    };

    // As a special case outside of the RBAC system, allow a task to write
    // to its designated partition within its ops collections.
    if required_role == models::Capability::Write
        && (collection.collection_name == task_data_plane.ops_logs_name
            || collection.collection_name == task_data_plane.ops_stats_name)
        && journal_name_or_prefix.ends_with(&format!(
            "/kind={ops_kind}/name={}/pivot=00",
            labels::percent_encoding(&task.task_name).to_string(),
        ))
    {
        // Authorized write into designated ops partition.
    } else if tables::RoleGrant::is_authorized(
        role_grants,
        &task.task_name,
        &collection.collection_name,
        required_role,
    ) {
        // Authorized access through RBAC.
    } else {
        let ops_suffix = format!(
            "/kind={ops_kind}/name={}/pivot=00",
            labels::percent_encoding(&task.task_name).to_string(),
        );
        tracing::warn!(
            %task.spec_type,
            %shard_id,
            %journal_name_or_prefix,
            ?required_role,
            ops_logs=%task_data_plane.ops_logs_name,
            ops_stats=%task_data_plane.ops_stats_name,
            %ops_suffix,
            "task authorization rejection context"
        );
        anyhow::bail!(
            "task shard {shard_id} is not authorized to {journal_name_or_prefix} for {required_role:?}"
        );
    }

    let Some(encoding_key) = collection_data_plane.hmac_keys.first() else {
        anyhow::bail!(
            "collection data-plane {} has no configured HMAC keys",
            collection_data_plane.data_plane_name
        );
    };
    let encoding_key = jsonwebtoken::EncodingKey::from_base64_secret(&encoding_key)?;

    Ok((
        encoding_key,
        collection_data_plane.data_plane_fqdn.clone(),
        collection_data_plane.broker_address.clone(),
    ))
}

// Snapshot is a point-in-time view of control-plane state
// that influences authorization decisions.
pub struct Snapshot {
    // Time immediately before the snapshot was taken.
    taken: std::time::SystemTime,
    // Platform collections, indexed on `journal_template_name`.
    collections: Vec<SnapshotCollection>,
    // Platform data-planes.
    data_planes: tables::DataPlanes,
    // Platform role grants.
    role_grants: tables::RoleGrants,
    // Platform tasks, indexed on `shard_template_id`.
    tasks: Vec<SnapshotTask>,
    // `refresh` is take()-en when the current snapshot should be refreshed.
    refresh_tx: Option<futures::channel::oneshot::Sender<()>>,
}

// SnapshotCollection is the state of a live collection which influences authorization.
// It's indexed on `journal_template_name`.
struct SnapshotCollection {
    journal_template_name: String,
    collection_name: models::Collection,
    data_plane_id: models::Id,
}
// SnapshotTask is the state of a live task which influences authorization.
// It's indexed on `shard_template_id`.
struct SnapshotTask {
    shard_template_id: String,
    task_name: models::Name,
    spec_type: models::CatalogType,
    data_plane_id: models::Id,
}

pub fn seed_snapshot() -> (Snapshot, futures::channel::oneshot::Receiver<()>) {
    let (next_tx, next_rx) = futures::channel::oneshot::channel();

    (
        Snapshot {
            taken: std::time::SystemTime::UNIX_EPOCH,
            collections: Vec::new(),
            data_planes: tables::DataPlanes::default(),
            role_grants: tables::RoleGrants::default(),
            tasks: Vec::new(),
            refresh_tx: Some(next_tx),
        },
        next_rx,
    )
}

pub async fn snapshot_loop(app: Arc<App>, mut refresh_rx: futures::channel::oneshot::Receiver<()>) {
    while let Ok(()) = refresh_rx.await {
        let (next_tx, next_rx) = futures::channel::oneshot::channel();
        refresh_rx = next_rx;

        match try_fetch_snapshot(&app.pg_pool).await {
            Ok(mut snapshot) => {
                snapshot.refresh_tx = Some(next_tx);
                *app.snapshot.write().unwrap() = snapshot;
            }
            Err(err) => {
                tracing::error!(?err, "failed to fetch snapshot (will retry)");
                () = tokio::time::sleep(MIN_SNAPSHOT_INTERVAL).await;
                _ = next_tx.send(()); // Wake ourselves to retry.
            }
        };
    }
}

async fn try_fetch_snapshot(pg_pool: &sqlx::PgPool) -> anyhow::Result<Snapshot> {
    tracing::info!("started to fetch authorization snapshot");
    let taken = std::time::SystemTime::now();

    let mut collections = sqlx::query_as!(
        SnapshotCollection,
        r#"
            select
                journal_template_name as "journal_template_name!",
                catalog_name as "collection_name: models::Collection",
                data_plane_id as "data_plane_id: models::Id"
            from live_specs
            where journal_template_name is not null
            "#,
    )
    .fetch_all(pg_pool)
    .await
    .context("failed to fetch view of live collections")?;

    let data_planes = sqlx::query_as!(
        tables::DataPlane,
        r#"
            select
                id as "control_id: models::Id",
                data_plane_name,
                data_plane_fqdn,
                false as "is_default!: bool",
                hmac_keys,
                broker_address,
                reactor_address,
                ops_logs_name as "ops_logs_name: models::Collection",
                ops_stats_name as "ops_stats_name: models::Collection"
            from data_planes
            "#,
    )
    .fetch_all(pg_pool)
    .await
    .context("failed to fetch data_planes")?;

    let role_grants = sqlx::query_as!(
        tables::RoleGrant,
        r#"
            select
                subject_role as "subject_role: models::Prefix",
                object_role as "object_role: models::Prefix",
                capability as "capability: models::Capability"
            from role_grants
            "#,
    )
    .fetch_all(pg_pool)
    .await
    .context("failed to fetch role_grants")?;

    let mut tasks = sqlx::query_as!(
        SnapshotTask,
        r#"
            select
                shard_template_id as "shard_template_id!",
                catalog_name as "task_name: models::Name",
                spec_type as "spec_type!: models::CatalogType",
                data_plane_id as "data_plane_id: models::Id"
            from live_specs
            where shard_template_id is not null
            "#,
    )
    .fetch_all(pg_pool)
    .await
    .context("failed to fetch view of live tasks")?;

    let data_planes = tables::DataPlanes::from_iter(data_planes);
    let role_grants = tables::RoleGrants::from_iter(role_grants);

    // Shard ID and journal name templates are prefixes which are always
    // extended with a slash-separated suffix. Avoid inadvertent matches
    // over path component prefixes.
    for task in &mut tasks {
        task.shard_template_id.push('/');
    }
    for collection in &mut collections {
        collection.journal_template_name.push('/');
    }

    tasks.sort_by(|t1, t2| t1.shard_template_id.cmp(&t2.shard_template_id));
    collections.sort_by(|c1, c2| c1.journal_template_name.cmp(&c2.journal_template_name));

    tracing::info!(
        collections = collections.len(),
        data_planes = data_planes.len(),
        role_grants = role_grants.len(),
        tasks = tasks.len(),
        "fetched authorization snapshot",
    );

    Ok(Snapshot {
        taken,
        collections,
        data_planes,
        role_grants,
        tasks,
        refresh_tx: None,
    })
}

fn begin_refresh<'m>(
    guard: std::sync::RwLockReadGuard<'_, Snapshot>,
    mu: &'m std::sync::RwLock<Snapshot>,
) {
    // We must release our read-lock before we can acquire a write lock.
    std::mem::drop(guard);

    if let Some(tx) = mu.write().unwrap().refresh_tx.take() {
        () = tx.send(()).unwrap(); // Begin a refresh.
    }
}

fn jitter() -> u64 {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    rng.gen_range(0..=2_000)
}

const MIN_SNAPSHOT_INTERVAL: std::time::Duration = std::time::Duration::from_secs(10);
const MAX_SNAPSHOT_INTERVAL: std::time::Duration = std::time::Duration::from_secs(300); // 5 minutes.
