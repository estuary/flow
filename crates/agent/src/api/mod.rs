use axum::{http::StatusCode, response::IntoResponse};
use std::sync::{Arc, Mutex};

mod authorize;
mod create_data_plane;
mod update_l2_reporting;

/// Request wraps a JSON-deserialized request type T which
/// also implements the validator::Validate trait.
#[derive(Debug, Clone, Copy, Default)]
pub struct Request<T>(pub T);

/// Claims are the JWT claims attached to control-plane access tokens.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct Claims {
    // Note that many more fields, such as additional user metadata,
    // are available if we choose to parse them.
    pub sub: uuid::Uuid,
    pub email: String,
    pub iat: usize,
    pub exp: usize,
}

/// Rejection is an error type of reasons why an API request may fail.
#[derive(Debug, thiserror::Error)]
pub enum Rejection {
    #[error(transparent)]
    ValidationError(#[from] validator::ValidationErrors),
    #[error(transparent)]
    JsonError(#[from] axum::extract::rejection::JsonRejection),
}

struct App {
    id_generator: Mutex<models::IdGenerator>,
    jwt_secret: jsonwebtoken::DecodingKey,
    jwt_validation: jsonwebtoken::Validation,
    pg_pool: sqlx::PgPool,
    publisher: crate::publications::Publisher,
}

/// Build the agent's API router.
pub fn build_router(
    id_generator: models::IdGenerator,
    jwt_secret: Vec<u8>,
    pg_pool: sqlx::PgPool,
    publisher: crate::publications::Publisher,
) -> axum::Router<()> {
    let jwt_secret = jsonwebtoken::DecodingKey::from_secret(&jwt_secret);

    let mut jwt_validation = jsonwebtoken::Validation::default();
    jwt_validation.set_audience(&["authenticated"]);

    let app = Arc::new(App {
        id_generator: Mutex::new(id_generator),
        jwt_secret,
        jwt_validation,
        pg_pool,
        publisher,
    });

    use axum::routing::post;

    let schema_router = axum::Router::new()
        .route("/authorize/task", post(authorize::authorize_task))
        .route(
            "/admin/create-data-plane",
            post(create_data_plane::create_data_plane)
                .route_layer(axum::middleware::from_fn_with_state(app.clone(), authorize)),
        )
        .route(
            "/admin/update-l2-reporting",
            post(update_l2_reporting::update_l2_reporting)
                .route_layer(axum::middleware::from_fn_with_state(app.clone(), authorize)),
        )
        .layer(tower_http::trace::TraceLayer::new_for_http())
        .with_state(app);

    schema_router
}

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

// TODO(johnny): Helper for more ergonomic errors.
// I'm near-certain there's a cleaner way to do this, but haven't found it yet.
async fn wrap<F, T>(fut: F) -> axum::response::Response
where
    T: serde::Serialize,
    F: std::future::Future<Output = anyhow::Result<T>>,
{
    match fut.await {
        Ok(inner) => (StatusCode::OK, axum::Json::from(inner)).into_response(),
        Err(err) => {
            let err = format!("{err:#}");
            (StatusCode::BAD_REQUEST, err).into_response()
        }
    }
}

// Middleware which validates JWT tokens before proceeding, and attaches verified Claims.
async fn authorize(
    axum::extract::State(app): axum::extract::State<Arc<App>>,
    axum_extra::TypedHeader(bearer): axum_extra::TypedHeader<
        axum_extra::headers::Authorization<axum_extra::headers::authorization::Bearer>,
    >,
    mut req: axum::http::Request<axum::body::Body>,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let token = match jsonwebtoken::decode::<Claims>(
        bearer.token(),
        &app.jwt_secret,
        &app.jwt_validation,
    ) {
        Ok(claims) => claims,
        Err(err) => {
            return (
                StatusCode::UNAUTHORIZED,
                format!("failed to parse authorization token: {err}"),
            )
                .into_response();
        }
    };

    req.extensions_mut().insert(token.claims);
    next.run(req).await
}
