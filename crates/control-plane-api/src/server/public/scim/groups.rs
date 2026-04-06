//! SCIM 2.0 Groups endpoints.
//!
//! Groups are stateless — the group display name encodes the catalog prefix and
//! capability (e.g. `acmeCo/widgets:admin`). The SCIM group ID is the
//! base64url encoding of the display name, so no storage is needed.
//!
//! Group membership maps directly to `user_grants`: adding a member creates a
//! grant, removing a member deletes it.

use super::ScimContext;
use axum::Json;
use base64::Engine;
use super::users::ScimError;

// --- Group name ↔ prefix + capability ---

/// Parse a group display name like `acmeCo/:admin` into a catalog prefix
/// and capability. The prefix must end with `/`.
fn parse_group_name(display_name: &str) -> Result<(String, models::Capability), ScimError> {
    let (prefix, cap_str) = display_name.rsplit_once(':').ok_or_else(|| {
        ScimError::BadRequest(format!(
            "group displayName must be 'prefix/:capability', got: {display_name}"
        ))
    })?;

    if !prefix.ends_with('/') {
        return Err(ScimError::BadRequest(format!(
            "group prefix must end with '/', got: {prefix}"
        )));
    }

    let capability = match cap_str {
        "read" => models::Capability::Read,
        "write" => models::Capability::Write,
        "admin" => models::Capability::Admin,
        other => {
            return Err(ScimError::BadRequest(format!(
                "unknown capability '{other}', expected read/write/admin"
            )));
        }
    };

    Ok((prefix.to_string(), capability))
}

/// Validate that a prefix is under the SCIM token's tenant.
fn validate_prefix_for_tenant(prefix: &str, tenant: &str) -> Result<(), ScimError> {
    if !prefix.starts_with(tenant) {
        return Err(ScimError::BadRequest(format!(
            "prefix '{prefix}' is not under tenant '{tenant}'"
        )));
    }
    Ok(())
}

// --- Group ID encoding ---

const BASE64_ENGINE: base64::engine::GeneralPurpose = base64::engine::general_purpose::URL_SAFE_NO_PAD;

fn encode_group_id(display_name: &str) -> String {
    BASE64_ENGINE.encode(display_name.as_bytes())
}

fn decode_group_id(id: &str) -> Result<String, ScimError> {
    let bytes = BASE64_ENGINE.decode(id).map_err(|_| {
        ScimError::BadRequest(format!("invalid group id (not valid base64url): {id}"))
    })?;
    String::from_utf8(bytes).map_err(|_| {
        ScimError::BadRequest(format!("invalid group id (not valid utf-8): {id}"))
    })
}

// --- Handlers ---

/// POST /Groups — create a group (validates the display name, returns the resource).
pub async fn create_group(
    ctx: ScimContext,
    Json(body): Json<GroupBody>,
) -> Result<(axum::http::StatusCode, Json<serde_json::Value>), ScimError> {
    let (prefix, capability) = parse_group_name(&body.display_name)?;
    validate_prefix_for_tenant(&prefix, &ctx.tenant)?;

    // Create grants for any members included in the request.
    if !body.members.is_empty() {
        let mut txn = ctx.pg_pool.begin().await.map_err(ScimError::internal)?;
        for member in &body.members {
            let user_id = parse_member_id(&member.value)?;
            crate::directives::grant::upsert_user_grant(
                user_id,
                &prefix,
                capability,
                Some(format!("SCIM group {}", body.display_name)),
                &mut txn,
            )
            .await
            .map_err(ScimError::internal)?;
        }
        txn.commit().await.map_err(ScimError::internal)?;
    }

    tracing::info!(
        tenant = %ctx.tenant,
        display_name = %body.display_name,
        members = body.members.len(),
        "SCIM created group"
    );

    let resource = group_resource(&body.display_name, &fetch_members(&ctx, &prefix, capability).await?);
    Ok((axum::http::StatusCode::CREATED, Json(resource)))
}

/// GET /Groups — list all groups for this tenant, optionally filtered by displayName.
///
/// Since groups are stateless (derived from user_grants), this returns one group
/// per distinct (object_role, capability) pair that has at least one grant.
pub async fn list_groups(
    ctx: ScimContext,
    axum::extract::Query(params): axum::extract::Query<ListGroupsParams>,
) -> Result<Json<serde_json::Value>, ScimError> {
    let display_name_filter = parse_optional_display_name_filter(&params.filter)?;

    let rows = sqlx::query!(
        r#"
        SELECT DISTINCT
            object_role AS "object_role!: String",
            capability AS "capability!: models::Capability"
        FROM user_grants
        WHERE object_role LIKE $1 || '%'
        ORDER BY object_role, capability
        "#,
        ctx.tenant,
    )
    .fetch_all(&ctx.pg_pool)
    .await
    .map_err(ScimError::internal)?;

    let mut resources = Vec::new();
    for row in &rows {
            let display_name = format!("{}:{}", row.object_role, capability_str(row.capability));

        if let Some(filter) = &display_name_filter {
            if display_name != *filter {
                continue;
            }
        }

        let members = fetch_members(&ctx, &row.object_role, row.capability).await?;
        resources.push(group_resource(&display_name, &members));
    }

    Ok(Json(serde_json::json!({
        "schemas": ["urn:ietf:params:scim:api:messages:2.0:ListResponse"],
        "totalResults": resources.len(),
        "Resources": resources,
    })))
}

/// GET /Groups/{id} — get a single group by its base64url-encoded ID.
pub async fn get_group(
    ctx: ScimContext,
    axum::extract::Path(group_id): axum::extract::Path<String>,
) -> Result<Json<serde_json::Value>, ScimError> {
    let display_name = decode_group_id(&group_id)?;
    let (prefix, capability) = parse_group_name(&display_name)?;
    validate_prefix_for_tenant(&prefix, &ctx.tenant)?;

    let members = fetch_members(&ctx, &prefix, capability).await?;
    Ok(Json(group_resource(&display_name, &members)))
}

/// PATCH /Groups/{id} — add or remove members.
pub async fn patch_group(
    ctx: ScimContext,
    axum::extract::Path(group_id): axum::extract::Path<String>,
    Json(patch): Json<GroupPatchOp>,
) -> Result<Json<serde_json::Value>, ScimError> {
    let display_name = decode_group_id(&group_id)?;
    let (prefix, capability) = parse_group_name(&display_name)?;
    validate_prefix_for_tenant(&prefix, &ctx.tenant)?;

    let mut txn = ctx.pg_pool.begin().await.map_err(ScimError::internal)?;

    for op in &patch.operations {
        match op.op.to_lowercase().as_str() {
            "add" => {
                let members = extract_members_from_value(&op.value)?;
                for member in members {
                    let user_id = parse_member_id(&member.value)?;
                    crate::directives::grant::upsert_user_grant(
                        user_id,
                        &prefix,
                        capability,
                        Some(format!("SCIM group {display_name}")),
                        &mut txn,
                    )
                    .await
                    .map_err(ScimError::internal)?;
                }
            }
            "remove" => {
                // SCIM PATCH remove with path "members[value eq \"<id>\"]"
                let user_id = parse_member_filter(op.path.as_deref())?;
                sqlx::query!(
                    r#"
                    DELETE FROM user_grants
                    WHERE user_id = $1
                        AND object_role = $2
                        AND capability = $3
                    "#,
                    user_id,
                    prefix.as_str(),
                    capability as models::Capability,
                )
                .execute(&mut *txn)
                .await
                .map_err(ScimError::internal)?;
            }
            "replace" => {
                // Full member replacement via replace on "members".
                let members = extract_members_from_value(&op.value)?;

                // Delete all existing grants for this prefix+capability.
                sqlx::query!(
                    r#"
                    DELETE FROM user_grants
                    WHERE object_role = $1
                        AND capability = $2
                    "#,
                    prefix.as_str(),
                    capability as models::Capability,
                )
                .execute(&mut *txn)
                .await
                .map_err(ScimError::internal)?;

                // Re-create grants for the new member list.
                for member in members {
                    let user_id = parse_member_id(&member.value)?;
                    crate::directives::grant::upsert_user_grant(
                        user_id,
                        &prefix,
                        capability,
                        Some(format!("SCIM group {display_name}")),
                        &mut txn,
                    )
                    .await
                    .map_err(ScimError::internal)?;
                }
            }
            other => {
                return Err(ScimError::BadRequest(format!(
                    "unsupported SCIM group operation: {other}"
                )));
            }
        }
    }

    txn.commit().await.map_err(ScimError::internal)?;

    tracing::info!(
        tenant = %ctx.tenant,
        %display_name,
        operations = patch.operations.len(),
        "SCIM patched group"
    );

    let members = fetch_members(&ctx, &prefix, capability).await?;
    Ok(Json(group_resource(&display_name, &members)))
}

/// PUT /Groups/{id} — full replacement of group membership.
pub async fn replace_group(
    ctx: ScimContext,
    axum::extract::Path(group_id): axum::extract::Path<String>,
    Json(body): Json<GroupBody>,
) -> Result<Json<serde_json::Value>, ScimError> {
    let display_name = decode_group_id(&group_id)?;

    if body.display_name != display_name {
        return Err(ScimError::BadRequest(format!(
            "displayName in body '{}' does not match group id '{}' (decoded: '{}')",
            body.display_name, group_id, display_name,
        )));
    }

    let (prefix, capability) = parse_group_name(&display_name)?;
    validate_prefix_for_tenant(&prefix, &ctx.tenant)?;

    let mut txn = ctx.pg_pool.begin().await.map_err(ScimError::internal)?;

    // Delete all existing grants for this prefix+capability.
    sqlx::query!(
        r#"
        DELETE FROM user_grants
        WHERE object_role = $1 AND capability = $2
        "#,
        prefix.as_str(),
        capability as models::Capability,
    )
    .execute(&mut *txn)
    .await
    .map_err(ScimError::internal)?;

    // Re-create from the PUT body.
    for member in &body.members {
        let user_id = parse_member_id(&member.value)?;
        crate::directives::grant::upsert_user_grant(
            user_id,
            &prefix,
            capability,
            Some(format!("SCIM group {display_name}")),
            &mut txn,
        )
        .await
        .map_err(ScimError::internal)?;
    }

    txn.commit().await.map_err(ScimError::internal)?;

    tracing::info!(
        tenant = %ctx.tenant,
        %display_name,
        members = body.members.len(),
        "SCIM replaced group membership"
    );

    let members = fetch_members(&ctx, &prefix, capability).await?;
    Ok(Json(group_resource(&display_name, &members)))
}

/// DELETE /Groups/{id} — remove all grants for this prefix+capability.
pub async fn delete_group(
    ctx: ScimContext,
    axum::extract::Path(group_id): axum::extract::Path<String>,
) -> Result<axum::http::StatusCode, ScimError> {
    let display_name = decode_group_id(&group_id)?;
    let (prefix, capability) = parse_group_name(&display_name)?;
    validate_prefix_for_tenant(&prefix, &ctx.tenant)?;

    let deleted = sqlx::query!(
        r#"
        DELETE FROM user_grants
        WHERE object_role = $1 AND capability = $2
        "#,
        prefix.as_str(),
        capability as models::Capability,
    )
    .execute(&ctx.pg_pool)
    .await
    .map_err(ScimError::internal)?
    .rows_affected();

    tracing::info!(
        tenant = %ctx.tenant,
        %display_name,
        %deleted,
        "SCIM deleted group"
    );

    Ok(axum::http::StatusCode::NO_CONTENT)
}

// --- Types ---

#[derive(serde::Deserialize)]
pub struct ListGroupsParams {
    #[serde(default)]
    filter: String,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupBody {
    pub display_name: String,
    #[serde(default)]
    pub members: Vec<MemberRef>,
    #[allow(dead_code)]
    #[serde(default)]
    pub schemas: Vec<String>,
}

#[derive(serde::Deserialize, Clone)]
pub struct MemberRef {
    pub value: String,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupPatchOp {
    #[allow(dead_code)]
    #[serde(default)]
    pub schemas: Vec<String>,
    #[serde(alias = "Operations")]
    pub operations: Vec<GroupOperation>,
}

#[derive(serde::Deserialize)]
pub struct GroupOperation {
    pub op: String,
    #[serde(default)]
    pub path: Option<String>,
    #[serde(default)]
    pub value: serde_json::Value,
}

// --- Helpers ---

/// Parse an optional SCIM filter like `displayName eq "gregCo/us/prod:read"`.
fn parse_optional_display_name_filter(filter: &str) -> Result<Option<String>, ScimError> {
    let filter = filter.trim();
    if filter.is_empty() {
        return Ok(None);
    }

    let parts: Vec<&str> = filter.splitn(3, ' ').collect();
    if parts.len() != 3 {
        return Err(ScimError::BadRequest(format!(
            "invalid filter syntax: {filter}"
        )));
    }

    if !parts[0].eq_ignore_ascii_case("displayName") || !parts[1].eq_ignore_ascii_case("eq") {
        return Err(ScimError::BadRequest(format!(
            "only 'displayName eq' filter is supported for groups, got: {filter}"
        )));
    }

    let value = parts[2]
        .strip_prefix('"')
        .and_then(|s| s.strip_suffix('"'))
        .ok_or_else(|| ScimError::BadRequest(format!("filter value must be quoted: {filter}")))?;

    Ok(Some(value.to_string()))
}

fn capability_str(cap: models::Capability) -> &'static str {
    match cap {
        models::Capability::Read => "read",
        models::Capability::Write => "write",
        models::Capability::Admin => "admin",
    }
}

fn parse_member_id(value: &str) -> Result<uuid::Uuid, ScimError> {
    value.parse::<uuid::Uuid>().map_err(|_| {
        ScimError::BadRequest(format!("member value must be a UUID, got: {value}"))
    })
}

/// Parse SCIM member filter path like `members[value eq "<uuid>"]`.
fn parse_member_filter(path: Option<&str>) -> Result<uuid::Uuid, ScimError> {
    let path = path.ok_or_else(|| {
        ScimError::BadRequest("remove operation requires a path".to_string())
    })?;

    // Expected format: members[value eq "<uuid>"]
    let inner = path
        .strip_prefix("members[")
        .and_then(|s| s.strip_suffix(']'))
        .ok_or_else(|| {
            ScimError::BadRequest(format!("unsupported path format: {path}"))
        })?;

    let parts: Vec<&str> = inner.splitn(3, ' ').collect();
    if parts.len() != 3 || parts[0] != "value" || !parts[1].eq_ignore_ascii_case("eq") {
        return Err(ScimError::BadRequest(format!(
            "unsupported filter in path: {path}"
        )));
    }

    let id_str = parts[2]
        .strip_prefix('"')
        .and_then(|s| s.strip_suffix('"'))
        .unwrap_or(parts[2]);

    parse_member_id(id_str)
}

/// Extract member references from a SCIM operation value.
/// The value may be a single member object or an array of members.
fn extract_members_from_value(value: &serde_json::Value) -> Result<Vec<MemberRef>, ScimError> {
    match value {
        serde_json::Value::Array(arr) => {
            let members: Result<Vec<MemberRef>, _> = arr
                .iter()
                .map(|v| serde_json::from_value(v.clone()))
                .collect();
            members.map_err(|e| ScimError::BadRequest(format!("invalid member value: {e}")))
        }
        serde_json::Value::Object(_) => {
            let member: MemberRef = serde_json::from_value(value.clone())
                .map_err(|e| ScimError::BadRequest(format!("invalid member value: {e}")))?;
            Ok(vec![member])
        }
        _ => Err(ScimError::BadRequest(
            "operation value must be a member object or array".to_string(),
        )),
    }
}

/// Fetch current group members (users with matching grant).
async fn fetch_members(
    ctx: &ScimContext,
    prefix: &str,
    capability: models::Capability,
) -> Result<Vec<serde_json::Value>, ScimError> {
    let rows = sqlx::query!(
        r#"
        SELECT
            ug.user_id AS "user_id!: uuid::Uuid",
            u.email AS "email: String"
        FROM user_grants ug
        LEFT JOIN auth.users u ON u.id = ug.user_id
        WHERE ug.object_role = $1
            AND ug.capability = $2
        "#,
        prefix,
        capability as models::Capability,
    )
    .fetch_all(&ctx.pg_pool)
    .await
    .map_err(ScimError::internal)?;

    Ok(rows
        .iter()
        .map(|r| {
            serde_json::json!({
                "value": r.user_id.to_string(),
                "display": r.email.as_deref().unwrap_or(""),
            })
        })
        .collect())
}

/// Build a SCIM Group resource JSON object.
fn group_resource(display_name: &str, members: &[serde_json::Value]) -> serde_json::Value {
    serde_json::json!({
        "schemas": ["urn:ietf:params:scim:schemas:core:2.0:Group"],
        "id": encode_group_id(display_name),
        "displayName": display_name,
        "members": members,
        "meta": {
            "resourceType": "Group",
        },
    })
}
