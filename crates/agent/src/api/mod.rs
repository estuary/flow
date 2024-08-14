use axum::{http::StatusCode, response::IntoResponse};
use std::sync::{Arc, Mutex};

#[derive(Debug, thiserror::Error)]
pub enum Rejection {
    #[error(transparent)]
    ValidationError(#[from] validator::ValidationErrors),
    #[error(transparent)]
    JsonError(#[from] axum::extract::rejection::JsonRejection),
}

pub struct App {
    pub pg_pool: sqlx::PgPool,
    pub system_user_id: uuid::Uuid,
    pub publisher: crate::publications::Publisher,
    pub id_generator: Mutex<models::IdGenerator>,
}

mod authorize;
mod data_plane;

#[derive(Debug, Clone, Copy, Default)]
pub struct Request<T>(pub T);

#[axum::async_trait]
impl<T, S> axum::extract::FromRequest<S> for Request<T>
where
    T: serde::de::DeserializeOwned + validator::Validate,
    S: Send + Sync,
    axum::extract::Json<T>:
        axum::extract::FromRequest<S, Rejection = axum::extract::rejection::JsonRejection>,
{
    type Rejection = Rejection;

    async fn from_request(req: axum::extract::Request, state: &S) -> Result<Self, Self::Rejection> {
        let axum::extract::Json(value) = axum::extract::Json::<T>::from_request(req, state).await?;
        value.validate()?;
        Ok(Request(value))
    }
}

// Build an axum::Router for the agent API.
pub fn build_router(app: Arc<App>) -> axum::Router<()> {
    use axum::routing::{post, put};

    let schema_router = axum::Router::new()
        .route("/authorize", post(authorize::authorize))
        .route("/admin/data-plane", put(data_plane::upsert_data_plane))
        .layer(tower_http::trace::TraceLayer::new_for_http())
        .with_state(app);

    schema_router
}

impl axum::response::IntoResponse for Rejection {
    fn into_response(self) -> axum::response::Response {
        match self {
            Rejection::ValidationError(inner) => {
                let message = format!("Input validation error: [{inner}]").replace('\n', ", ");
                (StatusCode::BAD_REQUEST, message).into_response()
            }
            Rejection::JsonError(inner) => inner.into_response(),
        }
    }
}

async fn wrap<F, T>(fut: F) -> axum::response::Response
where
    T: serde::Serialize,
    F: std::future::Future<Output = anyhow::Result<T>>,
{
    match fut.await {
        Ok(inner) => (StatusCode::OK, axum::Json::from(inner)).into_response(),
        Err(err) => {
            let err = format!("{err:#}");
            tracing::warn!(err, "request failed");
            (StatusCode::BAD_REQUEST, err).into_response()
        }
    }
}
