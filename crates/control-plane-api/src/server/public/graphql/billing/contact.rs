use anyhow::Context;
use async_graphql::{InputObject, SimpleObject};

#[derive(Debug, Clone, SimpleObject)]
pub struct BillingContact {
    pub emails: Vec<String>,
    pub name: Option<String>,
    pub address: Option<BillingAddress>,
}

#[derive(Debug, Clone, SimpleObject, serde::Serialize, serde::Deserialize)]
pub struct BillingAddress {
    pub line1: Option<String>,
    pub line2: Option<String>,
    pub city: Option<String>,
    pub state: Option<String>,
    pub postal_code: Option<String>,
    pub country: Option<String>,
}

impl From<BillingAddress> for stripe::Address {
    fn from(a: BillingAddress) -> Self {
        Self {
            line1: a.line1,
            line2: a.line2,
            city: a.city,
            state: a.state,
            postal_code: a.postal_code,
            country: a.country,
        }
    }
}

#[derive(Debug, Clone, InputObject)]
pub struct BillingAddressInput {
    pub line1: Option<String>,
    pub line2: Option<String>,
    pub city: Option<String>,
    pub state: Option<String>,
    pub postal_code: Option<String>,
    pub country: Option<String>,
}

impl From<BillingAddressInput> for BillingAddress {
    fn from(input: BillingAddressInput) -> Self {
        Self {
            line1: input.line1,
            line2: input.line2,
            city: input.city,
            state: input.state,
            postal_code: input.postal_code,
            country: input.country,
        }
    }
}

pub async fn fetch_billing_contact(
    pool: &sqlx::PgPool,
    tenant: &str,
) -> anyhow::Result<BillingContact> {
    let row = sqlx::query!(
        r#"
        SELECT billing_emails, billing_name, billing_address
        FROM tenants
        WHERE tenant = $1
        "#,
        tenant,
    )
    .fetch_one(pool)
    .await?;

    let address: Option<BillingAddress> = row
        .billing_address
        .map(serde_json::from_value)
        .transpose()
        .context("deserializing stored billing_address")?;

    Ok(BillingContact {
        emails: row.billing_emails,
        name: row.billing_name,
        address,
    })
}

pub async fn update_billing_contact(
    pool: &sqlx::PgPool,
    tenant: &str,
    emails: &[String],
    name: &str,
    address: &BillingAddress,
) -> anyhow::Result<BillingContact> {
    let address_json = serde_json::to_value(address)?;

    let row = sqlx::query!(
        r#"
        UPDATE tenants
        SET
            billing_emails = $2,
            billing_name = $3,
            billing_address = $4,
            updated_at = now()
        WHERE tenant = $1
        RETURNING billing_emails, billing_name, billing_address
        "#,
        tenant,
        emails,
        name,
        address_json,
    )
    .fetch_one(pool)
    .await?;

    let address: Option<BillingAddress> = row
        .billing_address
        .map(serde_json::from_value)
        .transpose()
        .context("deserializing stored billing_address")?;

    Ok(BillingContact {
        emails: row.billing_emails,
        name: row.billing_name,
        address,
    })
}
