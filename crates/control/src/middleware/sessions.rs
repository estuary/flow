use std::str::FromStr;

use async_trait::async_trait;
use axum::extract::{Extension, FromRequest, RequestParts, TypedHeader};
use axum::headers::authorization::Basic;
use axum::headers::Authorization;
use axum::http::header::HeaderMap;
use axum::http::{self, HeaderValue};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::Json;
use hyper::{Request, StatusCode};
use strum::IntoEnumIterator;

use crate::config::settings;
use crate::context::AppContext;
use crate::controllers::json_api::{Links, PayloadError, ProblemDetails};
use crate::controllers::sessions::routes as sessions_routes;
use crate::models::accounts::Account;
use crate::models::id::Id;
use crate::models::sessions::IdentityProvider;
use crate::repo::accounts as accounts_repo;
use crate::repo::credentials as credentials_repo;
use crate::services::sessions::{SignedToken, Token};

#[derive(Clone, Debug)]
pub struct CurrentAccount(pub Account);

/// Fetches the `CurrentAccount` from the request extensions so that it can be
/// used as a handler argument.
#[async_trait]
impl<B> FromRequest<B> for CurrentAccount
where
    B: Send,
{
    type Rejection = AuthRedirect;

    async fn from_request(req: &mut RequestParts<B>) -> Result<Self, Self::Rejection> {
        let Extension(current_account) = Extension::<CurrentAccount>::from_request(req)
            .await
            .map_err(AuthRedirect::halt)?;

        Ok(current_account)
    }
}

/// Middleware function to enforce a valid authentication token in the headers.
/// Loads the associated account and stashes it in the request's extensions for
/// use by individual handlers.
pub async fn validate_authentication_token<B: Send>(
    req: Request<B>,
    next: Next<B>,
) -> impl IntoResponse {
    let mut req_parts = RequestParts::new(req);

    let Extension(ctx) = Extension::<AppContext>::from_request(&mut req_parts)
        .await
        .expect(
        "Middleware applied in the wrong order. Authentication requires the application context.",
    );

    let current_account = if let Ok(Extension(account)) =
        Extension::<CurrentAccount>::from_request(&mut req_parts).await
    {
        // Tests will attach a CurrentAccount directly to the request, something
        // not possible from a normal http request. This allows us to skip the login
        // setup for every single endpoint test while authorizing as the correct
        // account.
        account
    } else {
        // Extract the Basic Auth headers, verify the session token, then pull up the linked Account.
        let basic_auth = TypedHeader::<Authorization<Basic>>::from_request(&mut req_parts)
            .await
            .map_err(AuthRedirect::halt)?;

        let account_id = Id::from_str(basic_auth.username()).map_err(AuthRedirect::halt)?;
        let signed_token =
            SignedToken::decode(&basic_auth.password()).map_err(AuthRedirect::halt)?;
        let token = ctx
            .session_verifier()
            .verify_token(&signed_token)
            .map_err(AuthRedirect::halt)?;

        authorize_current_account(&ctx, account_id, &token)
            .await
            .map_err(AuthRedirect::halt)?
    };

    let mut req = req_parts.try_into_request().map_err(AuthRedirect::halt)?;
    req.extensions_mut().insert(current_account);

    Ok::<_, AuthRedirect>(next.run(req).await)
}

async fn authorize_current_account(
    ctx: &AppContext,
    account_id: Id<Account>,
    token: &Token,
) -> Result<CurrentAccount, anyhow::Error> {
    let credential = credentials_repo::fetch_by_account_and_session_token(
        ctx.db(),
        account_id,
        token.credential_token(),
    )
    .await?;

    let account = accounts_repo::fetch_one(ctx.db(), credential.account_id).await?;
    tracing::info!(
        credential_id = ?credential.id,
        account_name = ?account.name,
        "Authentication successful"
    );

    Ok(CurrentAccount(account))
}

pub struct AuthRedirect;

impl AuthRedirect {
    pub fn halt<E: std::fmt::Debug>(e: E) -> AuthRedirect {
        tracing::warn!(error = ?e, "AuthRedirect triggered");
        AuthRedirect
    }
}

impl IntoResponse for AuthRedirect {
    fn into_response(self) -> Response {
        let mut headers = HeaderMap::new();
        headers.append(http::header::WWW_AUTHENTICATE, www_authenticate_header());

        let mut payload = PayloadError::new(ProblemDetails {
            title: "Unauthorized".to_owned(),
            detail: Some("Authentication is required".to_owned()),
        });
        payload.links = IdentityProvider::iter().fold(Links::default(), |links, idp| {
            links.put(idp.to_string(), sessions_routes::create(idp))
        });

        (StatusCode::UNAUTHORIZED, headers, Json(payload)).into_response()
    }
}

fn www_authenticate_header() -> HeaderValue {
    let value = format!(r#"Basic realm="{}""#, settings().application.base_url());
    HeaderValue::try_from(value).expect("The base url to only include visible ascii characters")
}
