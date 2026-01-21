use chrono::{DateTime, Utc};
use models::status::AlertType;

#[derive(Debug, Clone, async_graphql::SimpleObject)]
#[graphql(complex)]
pub struct AlertSubscription {
    pub catalog_prefix: models::Prefix,
    /// Note that email is exposed only via the function, since it will
    /// eventually become optional when we introduce other delivery mechanisms.
    #[graphql(skip)]
    pub email: String,
    pub alert_types: Vec<AlertType>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub detail: Option<String>,
}

#[async_graphql::ComplexObject]
impl AlertSubscription {
    /// The email recipient for notifications. This may be null for
    /// subscriptions that use a different destination type.
    pub async fn email(&self) -> Option<String> {
        Some(self.email.clone())
    }

    /// Destination represents the notification destination (i.e. the receiver)
    /// as a URI, which can represent any type of transport. For now, only email
    /// is supported and every destination URL will have the `mailto:` URI
    /// scheme. Future notification mechanisms may use different URI schemes.
    pub async fn destination(&self) -> async_graphql::Result<url::Url> {
        url::Url::parse(&format!("mailto:{}", self.email))
            .map_err(|e| async_graphql::Error::new_with_source(e))
    }
}

pub async fn fetch_alert_subscriptions_prefixed_by(
    prefix: &str,
    db: &mut sqlx::PgConnection,
) -> sqlx::Result<Vec<AlertSubscription>> {
    sqlx::query_as!(
        AlertSubscription,
        r#"select
        catalog_prefix as "catalog_prefix: models::Prefix",
        email as "email!: String",
        include_alert_types as "alert_types: Vec<AlertType>",
        created_at,
        updated_at,
        detail
        from alert_subscriptions
        where catalog_prefix ^@ $1
        order by catalog_prefix asc, created_at asc
        "#,
        prefix
    )
    .fetch_all(db)
    .await
}

pub async fn fetch_alert_subsription_for_update(
    catalog_prefix: &str,
    email: &str,
    db: &mut sqlx::PgConnection,
) -> sqlx::Result<Option<AlertSubscription>> {
    sqlx::query_as!(
        AlertSubscription,
        r#"select
        catalog_prefix as "catalog_prefix: models::Prefix",
        email as "email!: String",
        include_alert_types as "alert_types: Vec<AlertType>",
        created_at,
        updated_at,
        detail
        from alert_subscriptions
        where
          catalog_prefix = $1::catalog_prefix
          and email = $2
        for update of alert_subscriptions
        "#,
        catalog_prefix as &str,
        email,
    )
    .fetch_optional(db)
    .await
}

pub async fn create_alert_subscription(
    catalog_prefix: &str,
    email: &str,
    alert_types: &[AlertType],
    detail: Option<&str>,
    db: &mut sqlx::PgConnection,
) -> sqlx::Result<AlertSubscription> {
    sqlx::query_as!(
        AlertSubscription,
        r#"insert into alert_subscriptions
          (catalog_prefix, email, include_alert_types, detail, created_at, updated_at)
        values ($1::catalog_prefix, $2, $3::alert_type[], $4, now(), now())
        returning
          catalog_prefix as "catalog_prefix: models::Prefix",
          email as "email!: String",
          include_alert_types as "alert_types: Vec<AlertType>",
          created_at,
          updated_at,
          detail
        "#,
        catalog_prefix as &str,
        email,
        alert_types as &[AlertType],
        detail,
    )
    .fetch_one(db)
    .await
}

pub async fn update_alert_subscription(
    catalog_prefix: &str,
    email: &str,
    alert_types: &[AlertType],
    detail: Option<&str>,
    db: &mut sqlx::PgConnection,
) -> sqlx::Result<AlertSubscription> {
    sqlx::query_as!(
        AlertSubscription,
        r#"
        update alert_subscriptions set
            include_alert_types = $3,
            detail = $4,
            updated_at = now()
        where catalog_prefix = $1::catalog_prefix
        and email = $2
        returning
            catalog_prefix as "catalog_prefix: models::Prefix",
            email as "email!: String",
            include_alert_types as "alert_types: Vec<AlertType>",
            created_at,
            updated_at,
            detail
        "#,
        catalog_prefix as &str,
        email,
        alert_types as &[AlertType],
        detail,
    )
    .fetch_one(db)
    .await
}

pub async fn delete_alert_subscription(
    prefix: &str,
    email: &str,
    pool: &sqlx::PgPool,
) -> sqlx::Result<Option<AlertSubscription>> {
    sqlx::query_as!(
        AlertSubscription,
        r#"delete from alert_subscriptions
        where catalog_prefix = $1::catalog_prefix
        and email = $2
        returning
            catalog_prefix as "catalog_prefix: models::Prefix",
            email as "email!: String",
            include_alert_types as "alert_types: Vec<AlertType>",
            created_at,
            updated_at,
            detail
        "#,
        prefix as &str,
        email,
    )
    .fetch_optional(pool)
    .await
}
