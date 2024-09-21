use super::{App, Snapshot};
use anyhow::Context;
use std::sync::Arc;

type Request = models::authorizations::UserCollectionAuthorizationRequest;
type Response = models::authorizations::UserCollectionAuthorization;

#[tracing::instrument(
    skip(snapshot),
    err(level = tracing::Level::WARN),
)]
async fn do_authorize_user_collection(
    App { snapshot, .. }: &App,
    super::ControlClaims {
        sub: user_id,
        email,
        ..
    }: super::ControlClaims,
    Request {
        collection: collection_name,
        started_unix,
    }: Request,
) -> anyhow::Result<Response> {
    let (has_started, started_unix) = if started_unix == 0 {
        (false, jsonwebtoken::get_current_timestamp())
    } else {
        (true, started_unix)
    };

    loop {
        match Snapshot::evaluate(snapshot, started_unix, |snapshot: &Snapshot| {
            evaluate_authorization(snapshot, user_id, email.as_ref(), &collection_name)
        }) {
            Ok(response) => return Ok(response),
            Err(Ok(retry_millis)) if has_started => {
                return Ok(Response {
                    retry_millis,
                    ..Default::default()
                })
            }
            Err(Ok(retry_millis)) => {
                () = tokio::time::sleep(std::time::Duration::from_millis(retry_millis)).await;
            }
            Err(Err(err)) => return Err(err),
        }
    }
}

fn evaluate_authorization(
    snapshot: &Snapshot,
    user_id: uuid::Uuid,
    user_email: Option<&String>,
    collection_name: &models::Collection,
) -> anyhow::Result<Response> {
    if !tables::UserGrant::is_authorized(
        &snapshot.role_grants,
        &snapshot.user_grants,
        user_id,
        collection_name,
        models::Capability::Read,
    ) {
        anyhow::bail!(
            "{} is not authorized to {collection_name}",
            user_email.map(String::as_str).unwrap_or("user")
        );
    }

    let Some(collection) = snapshot.collection_by_catalog_name(collection_name) else {
        anyhow::bail!("collection {collection_name} is not known")
    };
    let Some(data_plane) = snapshot.data_planes.get_by_key(&collection.data_plane_id) else {
        anyhow::bail!("couldn't resolve collection {collection_name} data-plane")
    };
    let Some(encoding_key) = data_plane.hmac_keys.first() else {
        anyhow::bail!(
            "collection data-plane {} has no configured HMAC keys",
            data_plane.data_plane_name
        );
    };
    let encoding_key = jsonwebtoken::EncodingKey::from_base64_secret(&encoding_key)?;

    let iat = jsonwebtoken::get_current_timestamp();
    let exp = iat + super::exp_seconds();
    let header = jsonwebtoken::Header::default();

    let claims = super::DataClaims {
        inner: proto_gazette::Claims {
            cap: proto_gazette::capability::LIST | proto_gazette::capability::READ,
            exp,
            iat,
            iss: data_plane.data_plane_fqdn.clone(),
            sub: user_id.to_string(),
            sel: proto_gazette::broker::LabelSelector {
                include: Some(labels::build_set([
                    ("name:prefix", collection.journal_template_name.as_str()),
                    (labels::COLLECTION, collection_name.as_str()),
                ])),
                exclude: None,
            },
        },
        // TODO(johnny): Temporary support for data-plane-gateway.
        prefixes: vec![
            collection_name.to_string(),
            collection.journal_template_name.clone(),
        ],
    };
    let token = jsonwebtoken::encode(&header, &claims, &encoding_key)
        .context("failed to encode authorized JWT")?;

    Ok(Response {
        broker_address: super::maybe_rewrite_address(true, &data_plane.broker_address),
        broker_token: token,
        journal_name_prefix: collection.journal_template_name.clone(),
        retry_millis: 0,
    })
}

#[axum::debug_handler]
pub async fn authorize_user_collection(
    axum::extract::State(app): axum::extract::State<Arc<App>>,
    axum::Extension(claims): axum::Extension<super::ControlClaims>,
    super::Request(request): super::Request<Request>,
) -> axum::response::Response {
    super::wrap(async move { do_authorize_user_collection(&app, claims, request).await }).await
}
