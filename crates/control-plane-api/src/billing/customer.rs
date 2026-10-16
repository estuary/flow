//! Resolution of a tenant's Stripe customer.
//!
//! Customer identity comes only from `tenants.stripe_customer_id`. A NULL ID
//! means no customer is recorded; callers do not search Stripe metadata.

use super::BillingProvider;
use anyhow::Context;

/// The tenant's Stripe customer, or `None` if it has none. A recorded customer
/// that was deleted in Stripe is also `None`.
pub async fn find_customer(
    pool: &sqlx::PgPool,
    provider: &dyn BillingProvider,
    tenant: &str,
) -> anyhow::Result<Option<stripe::Customer>> {
    let Some(customer) = lookup(pool, provider, tenant).await? else {
        return Ok(None);
    };
    if customer.deleted {
        tracing::warn!(tenant, customer_id = %customer.id, "recorded Stripe customer is deleted");
        Ok(None)
    } else {
        Ok(Some(customer))
    }
}

pub async fn require_customer(
    pool: &sqlx::PgPool,
    provider: &dyn BillingProvider,
    tenant: &str,
) -> anyhow::Result<stripe::Customer> {
    find_customer(pool, provider, tenant)
        .await?
        .ok_or_else(|| anyhow::anyhow!("no Stripe customer exists for tenant '{tenant}'"))
}

/// The tenant's Stripe customer, creating and recording one if it has none.
/// A recorded customer that was deleted in Stripe is an error rather than
/// being replaced, as that's a decision for an operator.
pub async fn find_or_create_customer(
    pool: &sqlx::PgPool,
    provider: &dyn BillingProvider,
    tenant: &str,
    email: &str,
    full_name: Option<&str>,
    billing_name: Option<&str>,
    address: Option<stripe::Address>,
) -> anyhow::Result<stripe::Customer> {
    match lookup(pool, provider, tenant).await? {
        Some(customer) if customer.deleted => anyhow::bail!(
            "Stripe customer {} of tenant '{tenant}' is deleted; \
             clear tenants.stripe_customer_id to create a new customer",
            customer.id
        ),
        Some(customer) => return Ok(customer),
        None => {
            let customer = provider
                .create_customer(tenant, email, full_name, billing_name, address)
                .await?;
            billing_types::record_customer_id(pool, tenant, &customer.id)
                .await
                .with_context(|| format!("recording created Stripe customer {}", customer.id))?;
            Ok(customer)
        }
    }
}

async fn lookup(
    pool: &sqlx::PgPool,
    provider: &dyn BillingProvider,
    tenant: &str,
) -> anyhow::Result<Option<stripe::Customer>> {
    let Some(id) = sqlx::query_scalar!(
        r#"select stripe_customer_id from public.tenants where tenant = $1"#,
        tenant,
    )
    .fetch_optional(pool)
    .await?
    .flatten() else {
        return Ok(None);
    };
    let customer_id: stripe::CustomerId = id
        .parse()
        .with_context(|| format!("invalid recorded Stripe customer ID {id}"))?;
    let customer = provider
        .retrieve_customer(&customer_id)
        .await
        .with_context(|| format!("retrieving Stripe customer {customer_id}"))?;
    Ok(Some(customer))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::billing::InMemoryBillingProvider;

    const TENANT: &str = "acmeCo/";

    async fn insert_tenant(pool: &sqlx::PgPool, customer_id: Option<&str>) {
        sqlx::query(
            "insert into tenants (id, tenant, stripe_customer_id) values (internal.id_generator(), $1, $2)",
        )
            .bind(TENANT)
            .bind(customer_id)
            .execute(pool)
            .await
            .unwrap();
    }

    async fn recorded(pool: &sqlx::PgPool) -> Option<String> {
        sqlx::query_scalar("select stripe_customer_id from tenants where tenant = $1")
            .bind(TENANT)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    #[sqlx::test(migrations = "../../supabase/migrations")]
    async fn missing_id_does_not_search_or_adopt(pool: sqlx::PgPool) {
        insert_tenant(&pool, None).await;
        let provider = InMemoryBillingProvider::new();
        provider.add_customer(TENANT, "cus_unrecorded", None);

        assert!(lookup(&pool, &provider, TENANT).await.unwrap().is_none());
        assert!(
            find_customer(&pool, &provider, TENANT)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(recorded(&pool).await, None);
    }

    #[sqlx::test(migrations = "../../supabase/migrations")]
    async fn recorded_customer_is_retrieved_and_deleted_is_not_replaced(pool: sqlx::PgPool) {
        insert_tenant(&pool, Some("cus_recorded")).await;
        let provider = InMemoryBillingProvider::new();
        provider.add_customer("otherCo/", "cus_recorded", None);
        provider.add_customer(TENANT, "cus_metadata_match", None);

        let found = find_customer(&pool, &provider, TENANT)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(found.id.as_str(), "cus_recorded");

        provider.delete_customer("cus_recorded");
        assert!(
            find_customer(&pool, &provider, TENANT)
                .await
                .unwrap()
                .is_none()
        );
        let err = find_or_create_customer(&pool, &provider, TENANT, "a@b.test", None, None, None)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("is deleted"), "{err:#}");
        assert_eq!(recorded(&pool).await.as_deref(), Some("cus_recorded"));
    }

    #[sqlx::test(migrations = "../../supabase/migrations")]
    async fn created_customer_is_recorded(pool: sqlx::PgPool) {
        insert_tenant(&pool, None).await;
        let provider = InMemoryBillingProvider::new();

        let created =
            find_or_create_customer(&pool, &provider, TENANT, "a@b.test", None, None, None)
                .await
                .unwrap();
        assert_eq!(recorded(&pool).await.as_deref(), Some(created.id.as_str()));

        // A conflicting customer is never recorded over the existing one.
        let err = billing_types::record_customer_id(&pool, TENANT, &"cus_other".parse().unwrap())
            .await
            .unwrap_err();
        assert!(err.to_string().contains("is billed to"), "{err:#}");
        assert_eq!(recorded(&pool).await.as_deref(), Some(created.id.as_str()));
    }
}
