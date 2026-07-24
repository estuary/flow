use chrono::{DateTime, Datelike, Timelike, Utc};

/// An authorized adjustment to a tenant's invoice for a particular month,
/// such as a make-good credit or a negotiated service fee. Adjustments are
/// created by Estuary support staff and appear as invoice line items.
#[derive(Debug, Clone, async_graphql::SimpleObject)]
pub struct BillingAdjustment {
    /// Unique id of this adjustment.
    pub id: models::Id,
    /// Time at which this adjustment was created.
    pub created_at: DateTime<Utc>,
    /// Time at which this adjustment was last updated.
    pub updated_at: DateTime<Utc>,
    /// Tenant whose invoice is adjusted.
    pub tenant: String,
    /// Month of the invoice being adjusted, as the first instant of that
    /// month in UTC.
    pub billed_month: DateTime<Utc>,
    /// Adjustment amount in USD cents. Positive values increase the invoice
    /// total, and negative values are credits which decrease it.
    pub usd_cents: i32,
    /// Email of the Estuary employee who authorized this adjustment.
    pub authorizer: String,
    /// Human-readable reason for this adjustment.
    pub detail: Option<String>,
}

/// Errors unless `billed_month` is the first instant of a month in UTC,
/// mirroring the CHECK constraint on internal.billing_adjustments.
pub fn require_month_boundary(billed_month: DateTime<Utc>) -> async_graphql::Result<()> {
    if billed_month.day() != 1
        || billed_month.hour() != 0
        || billed_month.minute() != 0
        || billed_month.second() != 0
        || billed_month.nanosecond() != 0
    {
        return Err(async_graphql::Error::new(
            "billedMonth must be the first instant of a month in UTC, like 2026-07-01T00:00:00Z",
        ));
    }
    Ok(())
}

pub async fn insert_adjustment(
    pool: &sqlx::PgPool,
    tenant: &str,
    billed_month: DateTime<Utc>,
    usd_cents: i32,
    authorizer: &str,
    detail: &str,
) -> sqlx::Result<BillingAdjustment> {
    sqlx::query_as!(
        BillingAdjustment,
        r#"
        INSERT INTO internal.billing_adjustments (id, tenant, billed_month, usd_cents, authorizer, detail)
        VALUES (internal.id_generator(), $1::catalog_tenant, $2, $3, $4, $5)
        RETURNING
            id as "id!: models::Id",
            created_at as "created_at!: DateTime<Utc>",
            updated_at as "updated_at!: DateTime<Utc>",
            tenant as "tenant!: String",
            billed_month as "billed_month!: DateTime<Utc>",
            usd_cents as "usd_cents!",
            authorizer as "authorizer!",
            detail as "detail: String"
        "#,
        tenant as &str,
        billed_month,
        usd_cents,
        authorizer,
        detail,
    )
    .fetch_one(pool)
    .await
}

/// Forward pagination: fetch adjustments older than `cursor` (or the newest
/// adjustments when `cursor` is `None`). Returned rows are ordered newest-first.
pub async fn fetch_adjustments_forward(
    pool: &sqlx::PgPool,
    tenant: &str,
    cursor: Option<DateTime<Utc>>,
    limit: Option<usize>,
) -> sqlx::Result<(Vec<BillingAdjustment>, bool)> {
    let query_limit = limit.map(|l| l as i64 + 1).unwrap_or(i64::MAX);
    let mut rows = sqlx::query_as!(
        BillingAdjustment,
        r#"
        SELECT
            id as "id!: models::Id",
            created_at as "created_at!: DateTime<Utc>",
            updated_at as "updated_at!: DateTime<Utc>",
            tenant as "tenant!: String",
            billed_month as "billed_month!: DateTime<Utc>",
            usd_cents as "usd_cents!",
            authorizer as "authorizer!",
            detail as "detail: String"
        FROM internal.billing_adjustments
        WHERE tenant = $1::catalog_tenant
          AND ($2::timestamptz IS NULL OR created_at < $2)
        ORDER BY created_at DESC
        LIMIT $3
        "#,
        tenant as &str,
        cursor,
        query_limit,
    )
    .fetch_all(pool)
    .await?;

    let has_more = limit.is_some_and(|l| rows.len() > l);
    if let Some(l) = limit {
        rows.truncate(l);
    }
    Ok((rows, has_more))
}

/// Backward pagination: fetch adjustments newer than `cursor`. Returned rows
/// are ordered newest-first (the query selects oldest-first to honor `limit`,
/// then the result is reversed).
pub async fn fetch_adjustments_backward(
    pool: &sqlx::PgPool,
    tenant: &str,
    cursor: Option<DateTime<Utc>>,
    limit: Option<usize>,
) -> sqlx::Result<(Vec<BillingAdjustment>, bool)> {
    let query_limit = limit.map(|l| l as i64 + 1).unwrap_or(i64::MAX);
    let mut rows = sqlx::query_as!(
        BillingAdjustment,
        r#"
        SELECT
            id as "id!: models::Id",
            created_at as "created_at!: DateTime<Utc>",
            updated_at as "updated_at!: DateTime<Utc>",
            tenant as "tenant!: String",
            billed_month as "billed_month!: DateTime<Utc>",
            usd_cents as "usd_cents!",
            authorizer as "authorizer!",
            detail as "detail: String"
        FROM internal.billing_adjustments
        WHERE tenant = $1::catalog_tenant
          AND ($2::timestamptz IS NULL OR created_at > $2)
        ORDER BY created_at ASC
        LIMIT $3
        "#,
        tenant as &str,
        cursor,
        query_limit,
    )
    .fetch_all(pool)
    .await?;

    let has_more = limit.is_some_and(|l| rows.len() > l);
    if let Some(l) = limit {
        rows.truncate(l);
    }
    rows.reverse();
    Ok((rows, has_more))
}

#[cfg(test)]
mod tests {
    use super::super::test_util::*;
    use crate::test_server;
    use serde_json::json;

    /// Provisions a user who is a member of estuary_support/, and re-creates
    /// the support role grant on `tenant` which provision_test_tenant removes.
    async fn provision_support_user(pool: &sqlx::PgPool, email: &str, tenant: &str) -> uuid::Uuid {
        let user_id = uuid::Uuid::new_v4();
        sqlx::query("insert into auth.users (id, email) values ($1, $2)")
            .bind(user_id)
            .bind(email)
            .execute(pool)
            .await
            .expect("insert support user");
        sqlx::query(
            "insert into user_grants (user_id, object_role, capability) values ($1, 'estuary_support/', 'admin')",
        )
        .bind(user_id)
        .execute(pool)
        .await
        .expect("grant estuary_support/ membership");
        sqlx::query(
            "insert into role_grants (subject_role, object_role, capability) values ('estuary_support/', $1, 'admin')",
        )
        .bind(format!("{tenant}/"))
        .execute(pool)
        .await
        .expect("grant estuary_support/ admin on tenant");
        user_id
    }

    const CREATE_MUTATION: &str = r#"
        mutation CreateAdjustment(
            $tenant: String!
            $billedMonth: DateTime!
            $usdCents: Int!
            $detail: String!
        ) {
            createBillingAdjustment(
                tenant: $tenant
                billedMonth: $billedMonth
                usdCents: $usdCents
                detail: $detail
            ) {
                adjustment {
                    tenant
                    billedMonth
                    usdCents
                    authorizer
                    detail
                }
            }
        }
    "#;

    const ADJUSTMENTS_QUERY: &str = r#"
        query Adjustments($tenant: String!, $first: Int) {
            tenant(name: $tenant) {
                billing {
                    adjustments(first: $first) {
                        pageInfo { hasNextPage hasPreviousPage }
                        edges { node { tenant billedMonth usdCents authorizer detail } }
                    }
                }
            }
        }
    "#;

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../../../fixtures", scripts("data_planes", "alice"))
    )]
    async fn graphql_billing_adjustments(pool: sqlx::PgPool) {
        let _guard = test_server::init();
        let tenant = "adjustco";
        let admin_user_id = provision_test_tenant(&pool, tenant).await;
        let support_user_id = provision_support_user(&pool, "support@estuary.test", tenant).await;

        let (server, admin_token) =
            start_server_and_token(&pool, admin_user_id, tenant, mock_provider()).await;
        let support_token = server.make_access_token(support_user_id, Some("support@estuary.test"));

        // A tenant admin holds EditBilling on their own tenant, but must NOT
        // be able to create adjustments (credit themselves).
        let denied: serde_json::Value = server
            .graphql(
                &json!({
                    "query": CREATE_MUTATION,
                    "variables": {
                        "tenant": "adjustco/",
                        "billedMonth": "2026-07-01T00:00:00Z",
                        "usdCents": -5000,
                        "detail": "self-credit attempt",
                    }
                }),
                Some(&admin_token),
            )
            .await;
        insta::assert_json_snapshot!("create_adjustment_denied_tenant_admin", denied);

        // A support user may create an adjustment, and `authorizer` is
        // derived from their token's email claim.
        let created: serde_json::Value = server
            .graphql(
                &json!({
                    "query": CREATE_MUTATION,
                    "variables": {
                        "tenant": "adjustco/",
                        "billedMonth": "2026-07-01T00:00:00Z",
                        "usdCents": -5000,
                        "detail": "make-good for incident",
                    }
                }),
                Some(&support_token),
            )
            .await;
        insta::assert_json_snapshot!("create_adjustment_by_support", created);

        // billedMonth which is not a month boundary is rejected.
        let bad_month: serde_json::Value = server
            .graphql(
                &json!({
                    "query": CREATE_MUTATION,
                    "variables": {
                        "tenant": "adjustco/",
                        "billedMonth": "2026-07-15T00:00:00Z",
                        "usdCents": -5000,
                        "detail": "mid-month",
                    }
                }),
                Some(&support_token),
            )
            .await;
        insta::assert_json_snapshot!("create_adjustment_bad_month", bad_month);

        // Zero-value adjustments are rejected.
        let zero: serde_json::Value = server
            .graphql(
                &json!({
                    "query": CREATE_MUTATION,
                    "variables": {
                        "tenant": "adjustco/",
                        "billedMonth": "2026-07-01T00:00:00Z",
                        "usdCents": 0,
                        "detail": "zero",
                    }
                }),
                Some(&support_token),
            )
            .await;
        insta::assert_json_snapshot!("create_adjustment_zero_cents", zero);

        // Adjustments for a nonexistent tenant are rejected.
        let missing: serde_json::Value = server
            .graphql(
                &json!({
                    "query": CREATE_MUTATION,
                    "variables": {
                        "tenant": "nonexistent/",
                        "billedMonth": "2026-07-01T00:00:00Z",
                        "usdCents": -5000,
                        "detail": "no such tenant",
                    }
                }),
                Some(&support_token),
            )
            .await;
        insta::assert_json_snapshot!("create_adjustment_missing_tenant", missing);

        // The tenant admin may view their own adjustments: they already
        // appear as invoice line items.
        let listed: serde_json::Value = server
            .graphql(
                &json!({
                    "query": ADJUSTMENTS_QUERY,
                    "variables": { "tenant": "adjustco/", "first": 10 }
                }),
                Some(&admin_token),
            )
            .await;
        insta::assert_json_snapshot!("adjustments_query_tenant_admin", listed);

        // Another tenant's admin cannot view adjustco's adjustments.
        let other_tenant = "adjustother";
        let other_user_id = provision_test_tenant(&pool, other_tenant).await;
        let other_token =
            server.make_access_token(other_user_id, Some(&format!("{other_tenant}@example.test")));
        let cross: serde_json::Value = server
            .graphql(
                &json!({
                    "query": ADJUSTMENTS_QUERY,
                    "variables": { "tenant": "adjustco/", "first": 10 }
                }),
                Some(&other_token),
            )
            .await;
        insta::assert_json_snapshot!("adjustments_query_cross_tenant_denied", cross);
    }

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../../../fixtures", scripts("data_planes", "alice"))
    )]
    async fn graphql_billing_adjustments_pagination(pool: sqlx::PgPool) {
        let _guard = test_server::init();
        let tenant = "adjustpage";
        let admin_user_id = provision_test_tenant(&pool, tenant).await;
        let support_user_id = provision_support_user(&pool, "support@estuary.test", tenant).await;

        // Insert three adjustments with distinct, increasing created_at values,
        // so "credit 2" is the newest.
        for (n, month) in ["2026-05-01", "2026-06-01", "2026-07-01"]
            .iter()
            .enumerate()
        {
            sqlx::query(
                r#"
                insert into internal.billing_adjustments
                    (id, tenant, billed_month, usd_cents, authorizer, detail, created_at)
                values
                    (internal.id_generator(), $1::catalog_tenant, $2::timestamptz, $3,
                     'support@estuary.test', $4, now() + make_interval(secs => $5))
                "#,
            )
            .bind(format!("{tenant}/"))
            .bind(format!("{month}T00:00:00Z"))
            .bind((n as i32 + 1) * -100)
            .bind(format!("credit {n}"))
            .bind(n as f64)
            .execute(&pool)
            .await
            .expect("insert adjustment fixture");
        }
        let _ = support_user_id;

        let (server, admin_token) =
            start_server_and_token(&pool, admin_user_id, tenant, mock_provider()).await;

        // Page forward, newest-first, two at a time.
        let page: serde_json::Value = server
            .graphql(
                &json!({
                    "query": r#"
                        query($tenant: String!, $first: Int, $after: String) {
                            tenant(name: $tenant) {
                                billing {
                                    adjustments(first: $first, after: $after) {
                                        pageInfo { hasNextPage endCursor }
                                        edges { node { detail usdCents } }
                                    }
                                }
                            }
                        }
                    "#,
                    "variables": { "tenant": "adjustpage/", "first": 2 }
                }),
                Some(&admin_token),
            )
            .await;

        let edges = page["data"]["tenant"]["billing"]["adjustments"]["edges"]
            .as_array()
            .expect("edges array");
        assert_eq!(edges.len(), 2, "first page should have 2 edges: {page:?}");
        assert_eq!(
            edges[0]["node"]["detail"],
            json!("credit 2"),
            "newest first"
        );
        assert_eq!(
            page["data"]["tenant"]["billing"]["adjustments"]["pageInfo"]["hasNextPage"],
            json!(true)
        );

        let end_cursor = page["data"]["tenant"]["billing"]["adjustments"]["pageInfo"]["endCursor"]
            .as_str()
            .expect("end cursor")
            .to_string();

        let page2: serde_json::Value = server
            .graphql(
                &json!({
                    "query": r#"
                        query($tenant: String!, $first: Int, $after: String) {
                            tenant(name: $tenant) {
                                billing {
                                    adjustments(first: $first, after: $after) {
                                        pageInfo { hasNextPage }
                                        edges { node { detail } }
                                    }
                                }
                            }
                        }
                    "#,
                    "variables": { "tenant": "adjustpage/", "first": 2, "after": end_cursor }
                }),
                Some(&admin_token),
            )
            .await;
        let edges2 = page2["data"]["tenant"]["billing"]["adjustments"]["edges"]
            .as_array()
            .expect("edges array");
        assert_eq!(edges2.len(), 1, "second page should have 1 edge: {page2:?}");
        assert_eq!(edges2[0]["node"]["detail"], json!("credit 0"));
        assert_eq!(
            page2["data"]["tenant"]["billing"]["adjustments"]["pageInfo"]["hasNextPage"],
            json!(false)
        );
    }
}
