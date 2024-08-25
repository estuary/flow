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
    /// Address of brokers in this data-plane.
    #[validate(url)]
    broker_address: String,
    /// Address of reactors in this data-plane.
    #[validate(url)]
    reactor_address: String,
    /// HMAC keys of the data-plane.
    hmac_keys: Vec<String>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema, Validate)]
#[serde(rename_all = "camelCase")]
pub struct Request {
    /// Base name of this data-plane, such as "gcp-us-central1-c1".
    #[validate]
    name: models::PartitionField,

    /// Private tenant to which this data-plane is provisioned,
    /// or if None the data-plane is public.
    #[validate]
    #[serde(default)]
    private: Option<models::Prefix>,

    #[validate]
    category: Category,
}

#[derive(Debug, serde::Serialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Response {}

#[tracing::instrument(
    skip(pg_pool, publisher, id_generator),
    ret,
    err(level = tracing::Level::WARN),
)]
async fn do_create_data_plane(
    App {
        pg_pool,
        publisher,
        id_generator,
        ..
    }: &App,
    super::Claims { sub: user_id, .. }: super::Claims,
    Request {
        name,
        private,
        category,
    }: Request,
) -> anyhow::Result<Response> {
    if let None = sqlx::query!(
        "select role_prefix from internal.user_roles($1, 'admin') where role_prefix = 'ops/'",
        user_id,
    )
    .fetch_optional(pg_pool)
    .await?
    {
        anyhow::bail!("authenticated user is not an admin of the 'ops/' tenant");
    }

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

    // Install ops logs and stats collections, as well as L1 roll-ups.
    // These may fail to activate if the data-plane is still being provisioned.
    let draft_str = include_str!("../../../../ops-catalog/data-plane-template.bundle.json")
        .replace("BASE_NAME", &base_name);
    let draft: tables::DraftCatalog = serde_json::from_str::<models::Catalog>(&draft_str)
        .unwrap()
        .into();

    let pub_id = id_generator.lock().unwrap().next();
    let built = publisher
        .build(
            user_id,
            pub_id,
            Some(format!("publication for data-plane {base_name}")),
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

#[axum::debug_handler]
pub async fn create_data_plane(
    axum::extract::State(app): axum::extract::State<Arc<App>>,
    axum::Extension(claims): axum::Extension<super::Claims>,
    super::Request(request): super::Request<Request>,
) -> axum::response::Response {
    super::wrap(async move { do_create_data_plane(&app, claims, request).await }).await
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