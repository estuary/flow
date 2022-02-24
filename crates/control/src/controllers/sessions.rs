use axum::extract::{Extension, Path};
use axum::response::IntoResponse;
use axum::Json;
use chrono::{Duration, Utc};
use hyper::StatusCode;
use sqlx::PgPool;

use crate::context::AppContext;
use crate::error::AppError;
use crate::models::accounts::{Account, NewAccount};
use crate::models::credentials::{Credential, NewCredential};
use crate::models::sessions::{IdentityProvider, NewSession, Session};
use crate::repo::accounts as accounts_repo;
use crate::repo::credentials as credentials_repo;
use crate::services::sessions::Token;

pub mod routes;
mod view;

pub async fn create(
    Extension(ctx): Extension<AppContext>,
    Path(idp): Path<IdentityProvider>,
    Json(input): Json<NewSession>,
) -> Result<impl IntoResponse, AppError> {
    match idp {
        IdentityProvider::Local => local_login(ctx, input).await,
        // Axum will reject unknown IdentityProviders before this function is even called.
    }
}

async fn local_login(ctx: AppContext, input: NewSession) -> Result<impl IntoResponse, AppError> {
    // Rather than a real OpenID Connect "auth_token", the local login strategy
    // interprets "auth_token" as the name of the account to login with.
    // "local" login is not meant for production, so there is no need for a
    // password or other mechanism of verifying identity.
    let account_name = input.auth_token;

    // TODO: We are going to split the account registration functionality out of
    // this endpoint eventually, but this is currently an expedient way to do
    // local testing for now.
    let account = match accounts_repo::find_by_name(ctx.db(), &account_name).await? {
        Some(account) => account,
        None => create_local_account(ctx.db(), &account_name).await?,
    };

    let credential = match credentials_repo::find_by_account(ctx.db(), account.id).await? {
        Some(credential) => credential,
        None => create_local_credential(ctx.db(), &account).await?,
    };

    let token = Token::new(credential.session_token);
    let signed_token = ctx.session_verifier().sign_token(&token)?;

    let session = Session {
        account_id: account.id,
        token: signed_token.encode()?,
        expires_at: token.expires_at().clone(),
    };

    Ok((StatusCode::CREATED, view::create(session)))
}

async fn create_local_account(db: &PgPool, account_name: &str) -> Result<Account, sqlx::Error> {
    let new_account = NewAccount {
        display_name: account_name.to_owned(),
        email: format!("{account_name}@example.com"),
        name: account_name.to_owned(),
    };

    accounts_repo::insert(db, new_account).await
}

async fn create_local_credential(
    db: &PgPool,
    account: &Account,
) -> Result<Credential, sqlx::Error> {
    let new_credential = NewCredential {
        account_id: account.id,
        expires_at: Utc::now() + Duration::weeks(52),
        issuer: "local".to_owned(),
        last_authorized_at: Utc::now(),
        subject: account.name.clone(),
    };

    credentials_repo::insert(db, new_credential).await
}
