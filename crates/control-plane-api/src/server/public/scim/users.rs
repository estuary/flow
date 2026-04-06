//! SCIM 2.0 Users endpoints.
//!
//! Users are scoped to the SCIM token's tenant by requiring a matching SSO
//! identity: only users who have an `auth.identities` row with `provider_id`
//! matching the tenant's `sso_provider_id` are visible. These accounts are
//! owned by the tenant's IdP.

use super::ScimContext;
use axum::Json;

/// POST /Users — provision a new user via GoTrue admin API.
pub async fn create_user(
    ctx: ScimContext,
    Json(body): Json<CreateUserBody>,
) -> Result<(axum::http::StatusCode, Json<serde_json::Value>), ScimError> {
    let email = &body.user_name;

    // Call GoTrue admin API to create the user account.
    let gotrue_response = ctx
        .app
        .http_client
        .post(format!("{}/admin/users", ctx.app.gotrue_url))
        .header("apikey", &ctx.app.gotrue_service_role_key)
        .bearer_auth(&ctx.app.gotrue_service_role_key)
        .json(&serde_json::json!({
            "email": email,
            "email_confirm": true,
        }))
        .send()
        .await
        .map_err(|e| ScimError::Internal(format!("GoTrue request failed: {e}")))?;

    if !gotrue_response.status().is_success() {
        let status = gotrue_response.status();
        let body = gotrue_response
            .text()
            .await
            .unwrap_or_else(|_| "unknown".to_string());

        // If the user already exists, look them up and return 409 Conflict per SCIM spec.
        // We intentionally don't attach an SSO identity here — the existing user may
        // be a social-auth account, and GoTrue doesn't support multiple provider
        // identities on one user. The on_sso_identity_insert trigger will handle
        // merging when the user eventually logs in via SSO.
        if status == reqwest::StatusCode::UNPROCESSABLE_ENTITY
            && body.contains("already been registered")
        {
            let existing = sqlx::query!(
                r#"
                SELECT
                    u.id AS "id!: uuid::Uuid",
                    u.email AS "email!: String",
                    u.raw_user_meta_data->>'full_name' AS "display_name: String"
                FROM auth.users u
                WHERE u.email = $1
                "#,
                email.as_str(),
            )
            .fetch_optional(&ctx.pg_pool)
            .await
            .map_err(ScimError::internal)?
            .ok_or_else(|| {
                ScimError::Internal("user exists in GoTrue but not found by email".to_string())
            })?;

            return Ok((
                axum::http::StatusCode::CONFLICT,
                Json(user_resource(
                    &existing.id,
                    &existing.email,
                    existing.display_name.as_deref(),
                    true,
                )),
            ));
        }

        return Err(ScimError::Internal(format!(
            "GoTrue returned {status}: {body}"
        )));
    }

    let gotrue_user: serde_json::Value = gotrue_response
        .json()
        .await
        .map_err(|e| ScimError::Internal(format!("GoTrue response parse failed: {e}")))?;

    let user_id = gotrue_user["id"]
        .as_str()
        .and_then(|s| s.parse::<uuid::Uuid>().ok())
        .ok_or_else(|| ScimError::Internal("GoTrue response missing user id".to_string()))?;

    // Convert the GoTrue-created email user into an SSO user:
    //  1. Delete the auto-created `email` provider identity (GoTrue always creates one).
    //  2. Insert an SSO identity so subsequent SCIM queries (which JOIN on
    //     auth.identities) can see this user, and so GoTrue matches this identity
    //     when the user logs in via SAML — reusing the account instead of creating
    //     a duplicate.
    //  3. Mark is_sso_user = true.
    // The provider_id is the email, which must match the IdP's SAML NameID.
    let sso_provider = format!("sso:{}", ctx.sso_provider_id);

    sqlx::query!(
        r#"
        WITH remove_email_identity AS (
            DELETE FROM auth.identities
            WHERE user_id = $1 AND provider = 'email'
        ),
        mark_sso AS (
            UPDATE auth.users SET is_sso_user = true WHERE id = $1
        )
        INSERT INTO auth.identities (id, user_id, provider, provider_id, identity_data, last_sign_in_at, created_at, updated_at)
        VALUES (gen_random_uuid(), $1, $2, $3, '{}'::jsonb, now(), now(), now())
        "#,
        user_id,
        sso_provider,
        email.as_str(),
    )
    .execute(&ctx.pg_pool)
    .await
    .map_err(|e| ScimError::Internal(format!("failed to create SSO identity: {e}")))?;

    tracing::info!(
        %user_id,
        %email,
        tenant = %ctx.tenant,
        "SCIM provisioned new SSO user"
    );

    Ok((
        axum::http::StatusCode::CREATED,
        Json(user_resource(
            &user_id,
            email,
            body.display_name.as_deref(),
            true,
        )),
    ))
}

/// GET /Users — list users, optionally filtered by `userName eq "..."`.
pub async fn list_users(
    ctx: ScimContext,
    axum::extract::Query(params): axum::extract::Query<ListUsersParams>,
) -> Result<Json<serde_json::Value>, ScimError> {
    let email_filter = parse_optional_username_filter(&params.filter)?;

    let users = sqlx::query!(
        r#"
        SELECT
            u.id AS "id!: uuid::Uuid",
            u.email AS "email!: String",
            u.raw_user_meta_data->>'full_name' AS "display_name: String"
        FROM auth.users u
        JOIN auth.identities i
            ON i.user_id = u.id
            AND i.provider = 'sso:' || $1::uuid::text
        WHERE ($2::text IS NULL OR u.email = $2)
        "#,
        ctx.sso_provider_id,
        email_filter as Option<&str>,
    )
    .fetch_all(&ctx.pg_pool)
    .await
    .map_err(ScimError::internal)?;

    let resources: Vec<serde_json::Value> = users
        .iter()
        .map(|u| user_resource(&u.id, &u.email, u.display_name.as_deref(), true))
        .collect();

    Ok(Json(serde_json::json!({
        "schemas": ["urn:ietf:params:scim:api:messages:2.0:ListResponse"],
        "totalResults": resources.len(),
        "Resources": resources,
    })))
}

/// GET /Users/{id} — lookup a single user by UUID.
pub async fn get_user(
    ctx: ScimContext,
    axum::extract::Path(user_id): axum::extract::Path<uuid::Uuid>,
) -> Result<Json<serde_json::Value>, ScimError> {
    let user = sqlx::query!(
        r#"
        SELECT
            u.id AS "id!: uuid::Uuid",
            u.email AS "email!: String",
            u.raw_user_meta_data->>'full_name' AS "display_name: String"
        FROM auth.users u
        JOIN auth.identities i
            ON i.user_id = u.id
            AND i.provider = 'sso:' || $1::uuid::text
        WHERE u.id = $2
        "#,
        ctx.sso_provider_id,
        user_id,
    )
    .fetch_optional(&ctx.pg_pool)
    .await
    .map_err(ScimError::internal)?
    .ok_or(ScimError::NotFound)?;

    Ok(Json(user_resource(
        &user.id,
        &user.email,
        user.display_name.as_deref(),
        true,
    )))
}

/// PATCH /Users/{id} — deprovisioning via `active: false`.
pub async fn patch_user(
    ctx: ScimContext,
    axum::extract::Path(user_id): axum::extract::Path<uuid::Uuid>,
    Json(patch): Json<PatchOp>,
) -> Result<Json<serde_json::Value>, ScimError> {
    // Validate the patch operations: we only support setting active to false.
    let mut deactivate = false;
    for op in &patch.operations {
        match (op.op.as_str(), op.path.as_deref(), &op.value) {
            ("replace", Some("active"), serde_json::Value::Bool(false)) => {
                deactivate = true;
            }
            ("replace", Some("active"), serde_json::Value::String(s))
                if s == "false" || s == "False" =>
            {
                deactivate = true;
            }
            _ => {
                return Err(ScimError::BadRequest(format!(
                    "unsupported SCIM operation: op={}, path={:?}",
                    op.op, op.path
                )));
            }
        }
    }

    if !deactivate {
        return Err(ScimError::BadRequest(
            "patch must set active to false".to_string(),
        ));
    }

    // Verify the user has an SSO identity matching this tenant's provider.
    let user = sqlx::query!(
        r#"
        SELECT
            u.id AS "id!: uuid::Uuid",
            u.email AS "email!: String",
            u.raw_user_meta_data->>'full_name' AS "display_name: String"
        FROM auth.users u
        JOIN auth.identities i
            ON i.user_id = u.id
            AND i.provider = 'sso:' || $1::uuid::text
        WHERE u.id = $2
        "#,
        ctx.sso_provider_id,
        user_id,
    )
    .fetch_optional(&ctx.pg_pool)
    .await
    .map_err(ScimError::internal)?
    .ok_or(ScimError::NotFound)?;

    // Deprovision in a transaction: revoke all grants, tokens, and sessions.
    let mut txn = ctx.pg_pool.begin().await.map_err(ScimError::internal)?;

    // Delete all user grants (user accounts are owned by the tenant).
    let grants_deleted = sqlx::query!("DELETE FROM user_grants WHERE user_id = $1", user_id,)
        .execute(&mut *txn)
        .await
        .map_err(ScimError::internal)?
        .rows_affected();

    // Revoke Estuary refresh tokens (flowctl / API tokens).
    sqlx::query!("DELETE FROM refresh_tokens WHERE user_id = $1", user_id,)
        .execute(&mut *txn)
        .await
        .map_err(ScimError::internal)?;

    // Revoke GoTrue sessions (forces immediate re-auth).
    sqlx::query!("DELETE FROM auth.sessions WHERE user_id = $1", user_id,)
        .execute(&mut *txn)
        .await
        .map_err(ScimError::internal)?;

    txn.commit().await.map_err(ScimError::internal)?;

    tracing::info!(
        %user_id,
        email = %user.email,
        tenant = %ctx.tenant,
        %grants_deleted,
        "SCIM deprovisioned user"
    );

    Ok(Json(user_resource(
        &user.id,
        &user.email,
        user.display_name.as_deref(),
        false,
    )))
}

// --- Types ---

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateUserBody {
    pub user_name: String,
    pub display_name: Option<String>,
    #[allow(dead_code)]
    #[serde(default)]
    pub schemas: Vec<String>,
}

#[derive(serde::Deserialize)]
pub struct ListUsersParams {
    #[serde(default)]
    filter: String,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PatchOp {
    #[allow(dead_code)]
    #[serde(default)]
    pub schemas: Vec<String>,
    #[serde(alias = "Operations")]
    pub operations: Vec<Operation>,
}

#[derive(serde::Deserialize)]
pub struct Operation {
    pub op: String,
    pub path: Option<String>,
    #[serde(default)]
    pub value: serde_json::Value,
}

// --- Helpers ---

/// Build a SCIM User resource JSON object.
fn user_resource(
    id: &uuid::Uuid,
    email: &str,
    display_name: Option<&str>,
    active: bool,
) -> serde_json::Value {
    serde_json::json!({
        "schemas": ["urn:ietf:params:scim:schemas:core:2.0:User"],
        "id": id.to_string(),
        "userName": email,
        "displayName": display_name.unwrap_or(""),
        "active": active,
        "meta": {
            "resourceType": "User",
        },
    })
}

/// Parse an optional SCIM filter like `userName eq "user@example.com"`.
/// Returns `None` if the filter is empty (list all users).
/// Only `userName eq "..."` is supported when a filter is provided.
fn parse_optional_username_filter(filter: &str) -> Result<Option<&str>, ScimError> {
    let filter = filter.trim();
    if filter.is_empty() {
        return Ok(None);
    }

    // Split into parts: ["userName", "eq", "\"user@example.com\""]
    let parts: Vec<&str> = filter.splitn(3, ' ').collect();
    if parts.len() != 3 {
        return Err(ScimError::BadRequest(format!(
            "invalid filter syntax: {filter}"
        )));
    }

    if !parts[0].eq_ignore_ascii_case("userName") || !parts[1].eq_ignore_ascii_case("eq") {
        return Err(ScimError::BadRequest(format!(
            "only 'userName eq' filter is supported, got: {filter}"
        )));
    }

    // Strip surrounding quotes.
    let value = parts[2]
        .strip_prefix('"')
        .and_then(|s| s.strip_suffix('"'))
        .ok_or_else(|| ScimError::BadRequest(format!("filter value must be quoted: {filter}")))?;

    Ok(Some(value))
}

// --- Errors ---

#[derive(Debug)]
pub enum ScimError {
    NotFound,
    BadRequest(String),
    Internal(String),
}

impl ScimError {
    pub(crate) fn internal(e: impl std::fmt::Display) -> Self {
        ScimError::Internal(e.to_string())
    }
}

impl axum::response::IntoResponse for ScimError {
    fn into_response(self) -> axum::response::Response {
        use axum::http::StatusCode;

        let (status, detail) = match &self {
            ScimError::NotFound => (StatusCode::NOT_FOUND, "resource not found".to_string()),
            ScimError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg.clone()),
            ScimError::Internal(msg) => {
                tracing::error!(error = %msg, "SCIM internal error");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal server error".to_string(),
                )
            }
        };

        let body = serde_json::json!({
            "schemas": ["urn:ietf:params:scim:api:messages:2.0:Error"],
            "status": status.as_str(),
            "detail": detail,
        });

        (status, Json(body)).into_response()
    }
}
