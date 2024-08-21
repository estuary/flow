use super::App;
use anyhow::Context;
use std::sync::Arc;
use validator::Validate;

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum Category {
    Managed,
    Manual(Manual),
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema, Validate)]
#[serde(rename_all = "camelCase")]
pub struct Manual {
    #[validate(url)]
    broker_address: String,
    #[validate(url)]
    reactor_address: String,

    hmac_keys: Vec<String>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema, Validate)]
#[serde(rename_all = "camelCase")]
pub struct Request {
    #[validate]
    name: models::PartitionField,

    #[validate]
    #[serde(default)]
    private: Option<models::Prefix>,

    #[validate]
    category: Category,
}

#[derive(Debug, serde::Serialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Response {}

async fn do_create_data_plane(
    App {
        pg_pool,
        system_user_id,
        publisher,
        id_generator,
    }: &App,
    Request {
        name,
        private,
        category,
    }: Request,
) -> anyhow::Result<Response> {
    let (data_plane_fqdn, base_name) = match &private {
        None => (
            format!("{name}.dp.estuary-data.com"),
            format!("public/{name}"),
        ),
        Some(prefix) => {
            let base_name = format!("private/{prefix}{name}");
            (
                format!(
                    "{:x}.dp.estuary-data.com",
                    xxhash_rust::xxh3::xxh3_64(base_name.as_bytes()),
                ),
                base_name,
            )
        }
    };
    std::mem::drop(name); // Use `base_name` only.

    let data_plane_name = format!("ops/dp/{base_name}");
    let ops_l1_inferred_name = format!("ops/rollups/L1/{base_name}/inferred-schemas");
    let ops_l1_stats_name = format!("ops/rollups/L1/{base_name}/catalog-stats");
    let ops_l2_inferred_transform = format!("{data_plane_fqdn}");
    let ops_l2_stats_transform = format!("{data_plane_fqdn}");
    let ops_logs_name = format!("ops/tasks/{base_name}/logs");
    let ops_stats_name = format!("ops/tasks/{base_name}/stats");

    let (broker_address, reactor_address, hmac_keys) = match category {
        Category::Managed => (
            format!("https://gazette.{data_plane_fqdn}"),
            format!("https://reactor.{data_plane_fqdn}"),
            Vec::new(),
        ),
        Category::Manual(Manual {
            broker_address,
            reactor_address,
            hmac_keys,
        }) => {
            for key in &hmac_keys {
                let _ = base64::decode(key).context("HMAC keys must be base64")?;
            }
            (broker_address, reactor_address, hmac_keys)
        }
    };

    // Grant a private tenant access to their data-plane and task logs & stats.
    // These grants are always safe to create for every tenant, but we only
    // bother to do it for tenants which are actively creating private data-planes.
    if let Some(prefix) = &private {
        sqlx::query!(
            r#"
            insert into role_grants (subject_role, object_role, capability, detail) values
                ($1::text, 'ops/dp/private/' || $1, 'read', 'private data-plane'),
                ($1::text, 'ops/tasks/private/' || $1, 'read', 'private data-plane')
            on conflict do nothing
            "#,
            &prefix as &str,
        )
        .execute(pg_pool)
        .await?;
    }

    let insert = sqlx::query!(
        r#"
        insert into data_planes (
            data_plane_name,
            data_plane_fqdn,
            ops_logs_name,
            ops_stats_name,
            ops_l1_inferred_name,
            ops_l1_stats_name,
            ops_l2_inferred_transform,
            ops_l2_stats_transform,
            broker_address,
            reactor_address,
            hmac_keys
        ) values (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
        )
        on conflict (data_plane_name) do update set
            broker_address = $9,
            reactor_address = $10,
            -- Don't replace non-empty hmac_keys with empty ones.
            hmac_keys = case when array_length($11, 1) > 0 then $11
                        else data_planes.hmac_keys end
        returning logs_token
        ;
        "#,
        &data_plane_name as &String,
        data_plane_fqdn,
        &ops_logs_name as &String,
        &ops_stats_name as &String,
        &ops_l1_inferred_name as &String,
        &ops_l1_stats_name as &String,
        &ops_l2_inferred_transform,
        &ops_l2_stats_transform,
        broker_address,
        reactor_address,
        hmac_keys.as_slice(),
    )
    .fetch_one(pg_pool)
    .await?;

    let draft_str = include_str!("../../../../ops-catalog/data-plane-template.bundle.json")
        .replace("BASE_NAME", &base_name);
    let draft: tables::DraftCatalog = serde_json::from_str::<models::Catalog>(&draft_str)
        .unwrap()
        .into();

    let pub_id = id_generator.lock().unwrap().next();
    let built = publisher
        .build(
            *system_user_id,
            pub_id,
            Some(format!("system publication for data-plane {base_name}")),
            draft,
            insert.logs_token,
            &data_plane_name,
        )
        .await?;

    if built.has_errors() {
        for err in built.output.errors() {
            tracing::error!(scope=%err.scope, err=format!("{:#}", err.error), "data-plane-template build error")
        }
        anyhow::bail!("data-plane-template build failed");
    }

    _ = publisher
        .commit(built)
        .await
        .context("committing publication")?
        .error_for_status()?;

    tracing::info!(
        data_plane_fqdn,
        data_plane_name,
        ops_l1_inferred_name,
        ops_l1_stats_name,
        ops_logs_name,
        ops_stats_name,
        broker_address,
        reactor_address,
        "data-plane created"
    );

    Ok(Response {})
}

//#[tracing::instrument(skip(app))]
#[axum::debug_handler]
pub async fn create_data_plane(
    axum::extract::State(app): axum::extract::State<Arc<App>>,
    super::Request(request): super::Request<Request>,
    // TypedHeader(auth): TypedHeader<headers::Authorization<headers::authorization::Bearer>>,
) -> axum::response::Response {
    super::wrap(async move { do_create_data_plane(&app, request).await }).await
}

impl Validate for Category {
    fn validate(&self) -> Result<(), validator::ValidationErrors> {
        if let Self::Manual(manual) = &self {
            manual.validate()
        } else {
            Ok(())
        }
    }
}

/*
#[derive(Debug, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum Provider {
    AWS,
    Vultr,
    GCP,
    Local,
}
*/

/*
// Fetch names and transforms of L1 => L2 reporting roll-ups.
let bindings = sqlx::query!(
    r#"
    select
        ops_l1_inferred_name  as "ops_l1_inferred_name: models::Collection",
        ops_l2_inferred_transform,
        ops_l1_stats_name     as "ops_l1_stats_name:    models::Collection",
        ops_l2_stats_transform
    from data_planes
    order by data_plane_name asc;
    "#,
)
.fetch_all(pg_pool)
.await?;

let l2_inferred_bindings = &mut ops_draft
    .collections
    .get_mut_by_key(&models::Collection::new(
        "ops.us-central1.v1/inferred-schemas/L2",
    ))
    .expect("L2 inferred-schemas derivation must be included in bundle")
    .model
    .as_mut()
    .unwrap()
    .derive
    .as_mut()
    .unwrap()
    .transforms;

l2_inferred_bindings.clear();
for b in &bindings {
    l2_inferred_bindings.push(models::TransformDef {
        backfill: 0,
        disable: false,
        lambda: models::RawValue::from_value(&serde_json::json!(
            "select json($flow_document);"
        )),
        name: models::Transform::new(&b.ops_l2_inferred_transform),
        priority: 0,
        read_delay: None,
        shuffle: models::Shuffle::Key(models::CompositeKey::new([models::JsonPointer::new(
            "/collection_name",
        )])),
        source: models::Source::Collection(b.ops_l1_inferred_name.clone()),
    });
}

let l2_stats_bindings = &mut ops_draft
    .collections
    .get_mut_by_key(&models::Collection::new(
        "ops.us-central1.v1/catalog-stats-L2",
    ))
    .expect("L2 catalog-stats derivation must be included in bundle")
    .model
    .as_mut()
    .unwrap()
    .derive
    .as_mut()
    .unwrap()
    .transforms;

l2_stats_bindings.clear();
for b in &bindings {
    l2_stats_bindings.push(models::TransformDef {
        backfill: 0,
        disable: false,
        lambda: models::RawValue::from_value(&serde_json::json!(
            "select json($flow_document);"
        )),
        name: models::Transform::new(&b.ops_l2_stats_transform),
        priority: 0,
        read_delay: None,
        shuffle: models::Shuffle::Key(models::CompositeKey::new([models::JsonPointer::new(
            "/catalogName",
        )])),
        source: models::Source::Collection(b.ops_l1_stats_name.clone()),
    });
}
*/
