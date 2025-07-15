use super::App;
use crate::{
    from_downstream_topic_name, to_downstream_topic_name, DekafError, SessionAuthentication,
};
use anyhow::Context;
use axum::extract::Request;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum_extra::headers;
use itertools::Itertools;
use kafka_protocol::{messages::TopicName, protocol::StrBytes};
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
        .layer(axum::middleware::from_fn_with_state(
            app.clone(),
            authenticate_and_redirect,
        ))
        .layer(tower_http::trace::TraceLayer::new_for_http())
        .with_state(app);

    schema_router
}

// List all collections as "subjects", which are generally Kafka topics in the ecosystem.
#[tracing::instrument(skip_all)]
async fn all_subjects(
    axum::extract::Extension(mut auth): axum::extract::Extension<SessionAuthentication>,
) -> Response {
    wrap(async move {
        let strict_topic_names = match &auth {
            SessionAuthentication::User(auth) => auth.config.strict_topic_names,
            SessionAuthentication::Task(auth) => auth.config.strict_topic_names,
            SessionAuthentication::Redirect { config, .. } => config.strict_topic_names,
        };

        auth.fetch_all_collection_names()
            .await
            .context("failed to list collections from the control plane")
            .map(|collections| {
                collections
                    .into_iter()
                    .map(|name| {
                        if strict_topic_names {
                            to_downstream_topic_name(TopicName::from(StrBytes::from_string(name)))
                                .to_string()
                        } else {
                            name
                        }
                    })
                    .flat_map(|collection| {
                        vec![format!("{collection}-key"), format!("{collection}-value")]
                    })
                    .collect_vec()
            })
    })
    .await
}

// Fetch the "latest" schema for a subject (collection).
#[tracing::instrument(skip(auth))]
async fn get_subject_latest(
    axum::extract::Extension(mut auth): axum::extract::Extension<SessionAuthentication>,
    axum::extract::Path(subject): axum::extract::Path<String>,
) -> Response {
    wrap(async move {
        let (is_key, collection) = if subject.ends_with("-value") {
            (false, &subject[..subject.len() - 6])
        } else if subject.ends_with("-key") {
            (true, &subject[..subject.len() - 4])
        } else {
            anyhow::bail!("expected subject to end with -key or -value")
        };

        let client = &auth.flow_client().await?.pg_client();

        let collection = super::Collection::new(
            &auth,
            client,
            &from_downstream_topic_name(TopicName::from(StrBytes::from_string(
                collection.to_string(),
            ))),
        )
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
#[tracing::instrument(skip(auth))]
async fn get_schema_by_id(
    axum::extract::Extension(mut auth): axum::extract::Extension<SessionAuthentication>,
    axum::extract::Path(id): axum::extract::Path<u32>,
) -> Response {
    wrap(async move {
        let client = &auth.flow_client().await?.pg_client();

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

async fn wrap<F, T>(fut: F) -> Response
where
    T: serde::Serialize,
    F: std::future::Future<Output = anyhow::Result<T>>,
{
    match fut.await {
        Ok(inner) => (axum::http::StatusCode::OK, axum::Json::from(inner)).into_response(),
        Err(err) => {
            let err = format!("{err:#?}");
            tracing::warn!(err, "request failed");
            (axum::http::StatusCode::BAD_REQUEST, err).into_response()
        }
    }
}

async fn authenticate_and_redirect(
    axum::extract::State(app): axum::extract::State<Arc<App>>,
    axum_extra::TypedHeader(auth): axum_extra::TypedHeader<
        headers::Authorization<headers::authorization::Basic>,
    >,
    uri: axum::http::Uri,
    mut req: Request,
    next: Next,
) -> Response {
    match app.authenticate(auth.username(), auth.password()).await {
        Ok(SessionAuthentication::Redirect {
            target_dataplane_fqdn,
            ..
        }) => {
            let redirect_url = format!(
                "https://{}{}",
                target_dataplane_fqdn,
                uri.path_and_query().map(|pq| pq.as_str()).unwrap_or("/")
            );
            (
                axum::http::StatusCode::TEMPORARY_REDIRECT,
                [("Location", redirect_url)],
            )
                .into_response()
        }
        Ok(auth) => {
            // Insert the authentication into request extensions so handlers can access it
            req.extensions_mut().insert(auth);
            next.run(req).await
        }
        Err(DekafError::Authentication(auth_err)) => {
            let err = format!("{auth_err:#?}");
            tracing::warn!(err, "authentication failed");
            (axum::http::StatusCode::UNAUTHORIZED, err).into_response()
        }
        Err(err) => {
            let err = format!("{err:#?}");
            tracing::error!(err, "unexpected error during authentication");
            (axum::http::StatusCode::INTERNAL_SERVER_ERROR, err).into_response()
        }
    }
}
