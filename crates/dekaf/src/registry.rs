use super::App;
use anyhow::Context;
use axum::{
    headers,
    http::StatusCode,
    response::{IntoResponse, Response},
    TypedHeader,
};
use std::sync::Arc;

// Build an axum::Router which implements a subset of the Confluent Schema Registry API,
// sufficient for decoding Avro-encoded topic data.
pub fn build_router(app: Arc<App>) -> axum::Router<()> {
    use axum::routing::get;

    let schema_router = axum::Router::new()
        .route("/subjects", get(all_subjects))
        .route(
            "/subjects/:subject/versions/latest",
            get(get_subject_latest),
        )
        .route("/schemas/ids/:id", get(get_schema_by_id))
        .layer(tower_http::trace::TraceLayer::new_for_http())
        .with_state(app);

    schema_router
}

// List all collections as "subjects", which are generally Kafka topics in the ecosystem.
#[tracing::instrument(skip_all)]
async fn all_subjects(
    axum::extract::State(app): axum::extract::State<Arc<App>>,
    TypedHeader(auth): TypedHeader<headers::Authorization<headers::authorization::Basic>>,
) -> Response {
    wrap(async move {
        let client = apply_auth(&app, auth)?;

        super::fetch_all_collection_names(&client)
            .await
            .context("failed to list collections from the control plane")
    })
    .await
}

// Fetch the "latest" schema for a subject (collection).
#[tracing::instrument(skip(app, auth))]
async fn get_subject_latest(
    axum::extract::State(app): axum::extract::State<Arc<App>>,
    TypedHeader(auth): TypedHeader<headers::Authorization<headers::authorization::Basic>>,
    axum::extract::Path(subject): axum::extract::Path<String>,
) -> Response {
    wrap(async move {
        let client = apply_auth(&app, auth)?;

        let (is_key, collection) = if subject.ends_with("-value") {
            (false, &subject[..subject.len() - 6])
        } else if subject.ends_with("-key") {
            (true, &subject[..subject.len() - 4])
        } else {
            anyhow::bail!("expected subject to end with -key or -value")
        };

        let collection = super::Collection::new(&client, collection)
            .await
            .context("failed to fetch collection metadata")?
            .with_context(|| format!("collection {collection} does not exist"))?;

        let (key_id, value_id) = collection
            .registered_schema_ids(&client)
            .await
            .context("failed to resolve registered Avro schemas")?;

        let (id, schema) = if is_key {
            (key_id, &collection.key_schema)
        } else {
            (value_id, &collection.value_schema)
        };

        Ok(serde_json::json!({
            "id": id,
            "schema": schema.canonical_form(),
            "schemaType": "AVRO",
            "subject": subject,
            "version": 1,
        }))
    })
    .await
}

// Fetch the schema with the given ID.
// Schemas are content-addressed and immutable, so an ID uniquely identifies a Avro schema.
#[tracing::instrument(skip(app, auth))]
async fn get_schema_by_id(
    axum::extract::State(app): axum::extract::State<Arc<App>>,
    TypedHeader(auth): TypedHeader<headers::Authorization<headers::authorization::Basic>>,
    axum::extract::Path(id): axum::extract::Path<u32>,
) -> Response {
    wrap(async move {
        let client = apply_auth(&app, auth)?;

        #[derive(serde::Deserialize)]
        struct Row {
            avro_schema: serde_json::Value,
        }

        let now = time::OffsetDateTime::now_utc();
        let now = now
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap();

        let mut rows: Vec<Row> = client
            .from("registered_avro_schemas")
            .eq("registry_id", format!("{id}"))
            .update(serde_json::json!({"updated_at": now}).to_string())
            .select("avro_schema")
            .execute()
            .await
            .and_then(|r| r.error_for_status())
            .context("querying for an already-registered schema")?
            .json()
            .await?;

        let Some(Row { avro_schema }) = rows.pop() else {
            anyhow::bail!("could not find schema with registry id {id}");
        };

        Ok(serde_json::json!({
            "schema": avro_schema.to_string(),
        }))
    })
    .await
}

fn apply_auth(
    app: &App,
    auth: headers::Authorization<headers::authorization::Basic>,
) -> anyhow::Result<postgrest::Postgrest> {
    // The "username" will eventually hold session configuration state.
    // Reserve the ability to do this by ensuring it currently equals '{}'.
    if auth.username() != "{}" {
        anyhow::bail!(crate::RESERVED_USERNAME_ERR);
    }

    let client = app
        .anon_client
        .clone()
        .insert_header("Authorization", format!("Bearer {}", auth.password()));

    Ok(client)
}

async fn wrap<F, T>(fut: F) -> Response
where
    T: serde::Serialize,
    F: std::future::Future<Output = anyhow::Result<T>>,
{
    match fut.await {
        Ok(inner) => (StatusCode::OK, axum::Json::from(inner)).into_response(),
        Err(err) => {
            let err = format!("{err:#?}");
            tracing::warn!(err, "request failed");
            (StatusCode::BAD_REQUEST, err).into_response()
        }
    }
}
