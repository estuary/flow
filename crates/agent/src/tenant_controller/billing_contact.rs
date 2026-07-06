use std::sync::Arc;

use anyhow::Context;
use control_plane_api::billing::{BillingProvider, CUSTOMER_NAME_METADATA_KEY};

use crate::{storage::tenants::TenantStore, tenant_controller::quotas::update_quotas};

use super::Tenant;

#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
pub struct BillingContactStatus {
    #[serde(default, skip_serializing_if = "is_zero")]
    pub failures: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_retry: Option<chrono::DateTime<chrono::Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    /// This indicates if we have successfully updated the quotas based on
    /// the status of a field within stripe.
    #[serde(default)]
    pub updated_quotas: bool,
}

pub(crate) fn is_zero(i: &u32) -> bool {
    *i == 0
}

pub(crate) fn retry_backoff(failures: u32) -> std::time::Duration {
    match failures {
        0 => std::time::Duration::ZERO,
        1 => std::time::Duration::from_secs(60),
        2 => std::time::Duration::from_secs(300),
        _ => std::time::Duration::from_secs(900),
    }
}

// TODO: Refactor this into a more public space for re-use.
pub enum Outcome {
    Idle,
    WaitForRetry(std::time::Duration),
}

pub async fn reconcile(
    status: &mut BillingContactStatus,
    tenant_row: &Tenant,
    billing_provider: &Option<Arc<dyn BillingProvider>>,
    tenant_provider: &Arc<dyn TenantStore>,
) -> anyhow::Result<Outcome> {
    if let Some(next_retry) = status.next_retry {
        let now = chrono::Utc::now();
        if next_retry > now {
            let wait = (next_retry - now)
                .to_std()
                .unwrap_or(std::time::Duration::from_secs(60));
            return Ok(Outcome::WaitForRetry(wait));
        }
    }

    match do_reconcile(tenant_row, billing_provider, status, tenant_provider).await {
        Ok(()) => {
            status.failures = 0;
            status.next_retry = None;
            status.last_error = None;
            Ok(Outcome::Idle)
        }
        Err(err) => {
            status.failures += 1;
            let backoff = retry_backoff(status.failures);
            status.next_retry = Some(chrono::Utc::now() + chrono::Duration::from_std(backoff)?);
            status.last_error = Some(format!("{err:#}"));
            tracing::warn!(
                tenant = %tenant_row.tenant,
                failures = status.failures,
                ?backoff,
                "billing contact reconciliation failed: {err:#}",
            );
            Ok(Outcome::WaitForRetry(backoff))
        }
    }
}

async fn do_reconcile(
    tenant_row: &Tenant,
    billing_provider: &Option<Arc<dyn BillingProvider>>,
    status: &mut BillingContactStatus,
    tenant_provider: &Arc<dyn TenantStore>,
) -> anyhow::Result<()> {
    let Some(provider) = billing_provider else {
        return Ok(());
    };

    let customer = provider
        .find_customer(&tenant_row.tenant)
        .await
        .context("looking up Stripe customer")?;
    let Some(customer) = customer else {
        return Ok(());
    };

    update_contact_info(tenant_row, provider, &customer).await?;
    // Not sure how we want to handle this in the event that we fail to update the
    // contact info before we update the billing info.
    update_quotas(status, tenant_row, billing_provider, tenant_provider).await?;

    Ok(())
}

async fn update_contact_info(
    tenant_row: &Tenant,
    billing_provider: &Arc<dyn BillingProvider>,
    customer: &stripe::Customer,
) -> anyhow::Result<()> {
    // Desired state comes from the DB. A NULL column means "no desired value",
    // so it never counts as a mismatch and is never pushed to Stripe.
    let desired_email = tenant_row.billing_email.as_deref();
    let desired_name = tenant_row.billing_name.as_deref();
    let desired_address: Option<stripe::Address> = tenant_row
        .billing_address
        .as_ref()
        .map(|v| serde_json::from_value(v.clone()))
        .transpose()
        .context("deserializing stored billing_address")?;

    let current_email = customer.email.as_deref();
    // The billing name lives in customer metadata, not `Customer.name` (the
    // tenant slug). See `StripeBillingProvider::update_customer_billing_profile`.
    let current_name = customer
        .metadata
        .as_ref()
        .and_then(|m| m.get(CUSTOMER_NAME_METADATA_KEY))
        .map(String::as_str);

    let email_mismatch = desired_email.is_some() && desired_email != current_email;
    let name_mismatch = desired_name.is_some() && desired_name != current_name;
    let address_mismatch = match (&desired_address, customer.address.as_ref()) {
        (None, _) => false,
        (Some(_), None) => true,
        (Some(desired), Some(current)) => !addresses_match(desired, current),
    };

    if !email_mismatch && !name_mismatch && !address_mismatch {
        return Ok(());
    }

    billing_provider
        .update_customer_billing_profile(&customer.id, desired_email, desired_name, desired_address)
        .await
        .context("updating Stripe customer billing profile")?;

    tracing::info!(
        tenant = %tenant_row.tenant,
        email_changed = email_mismatch,
        name_changed = name_mismatch,
        address_changed = address_mismatch,
        "reconciled billing contact with Stripe",
    );

    Ok(())
}

fn addresses_match(a: &stripe::Address, b: &stripe::Address) -> bool {
    a.line1 == b.line1
        && a.line2 == b.line2
        && a.city == b.city
        && a.state == b.state
        && a.postal_code == b.postal_code
        && a.country == b.country
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use control_plane_api::billing::{
        BillingProvider, CUSTOMER_NAME_METADATA_KEY, InMemoryBillingProvider,
    };

    use crate::storage::tenants::{PaymentProvider, PgTenantStore, TenantStore};

    use super::super::Tenant;
    use super::{BillingContactStatus, Outcome, reconcile};

    const TENANT: &str = "acmeCo/";
    const CUSTOMER_ID: &str = "cus_test";

    fn tenant_row(
        email: Option<&str>,
        name: Option<&str>,
        address: Option<serde_json::Value>,
        payment_provider: Option<PaymentProvider>,
    ) -> Tenant {
        Tenant {
            tenant: TENANT.to_string(),
            billing_email: email.map(str::to_string),
            billing_name: name.map(str::to_string),
            billing_address: address,
            payment_provider,
        }
    }

    fn address(city: &str) -> stripe::Address {
        stripe::Address {
            line1: Some("1 Main St".to_string()),
            city: Some(city.to_string()),
            postal_code: Some("10001".to_string()),
            country: Some("US".to_string()),
            ..Default::default()
        }
    }

    // Store an address the way `graphql::billing::contact::BillingAddress`
    // serializes it into the `tenants.billing_address` JSONB column, so the test
    // exercises the same stored-shape -> `stripe::Address` round-trip that
    // `do_reconcile` performs.
    fn stored_address(city: &str) -> serde_json::Value {
        serde_json::json!({
            "line1": "1 Main St",
            "line2": null,
            "city": city,
            "state": null,
            "postal_code": "10001",
            "country": "US",
        })
    }

    // Seed a customer for `TENANT` with the given pre-existing Stripe state.
    async fn customer_with(
        email: Option<&str>,
        name: Option<&str>,
        address: Option<stripe::Address>,
    ) -> Arc<InMemoryBillingProvider> {
        let provider = InMemoryBillingProvider::new();
        provider.add_customer(TENANT, CUSTOMER_ID, None);
        provider
            .update_customer_billing_profile(&CUSTOMER_ID.parse().unwrap(), email, name, address)
            .await
            .unwrap();
        Arc::new(provider)
    }

    async fn run(
        provider: &Arc<InMemoryBillingProvider>,
        status: &mut BillingContactStatus,
        row: &Tenant,
        pool: sqlx::PgPool,
    ) -> Outcome {
        let provider: Option<Arc<dyn BillingProvider>> = Some(provider.clone());
        let tenant_store: Arc<dyn TenantStore> = Arc::new(PgTenantStore::new(pool.clone()));
        reconcile(status, row, &provider, &tenant_store)
            .await
            .unwrap()
    }

    async fn current_customer(provider: &InMemoryBillingProvider) -> stripe::Customer {
        provider.find_customer(TENANT).await.unwrap().unwrap()
    }

    fn metadata_name(customer: &stripe::Customer) -> Option<&str> {
        customer
            .metadata
            .as_ref()
            .and_then(|m| m.get(CUSTOMER_NAME_METADATA_KEY))
            .map(String::as_str)
    }

    // A NULL billing column means "no desired value": it never counts as a
    // mismatch and is never pushed to Stripe, and a change to one field leaves the
    // others untouched rather than clearing them.
    #[sqlx::test(migrations = "../../supabase/migrations")]
    async fn null_db_fields_do_not_clear_stripe(pool: sqlx::PgPool) {
        let provider = customer_with(
            Some("existing@example.com"),
            Some("Existing Name"),
            Some(address("New York")),
        )
        .await;

        // All-NULL desired: no mismatch, no write, existing values intact.
        let baseline = provider.update_billing_profile_call_count();
        let outcome = run(
            &provider,
            &mut BillingContactStatus::default(),
            &tenant_row(None, None, None, None),
            pool.clone(),
        )
        .await;
        assert!(matches!(outcome, Outcome::Idle));
        assert_eq!(
            provider.update_billing_profile_call_count() - baseline,
            0,
            "NULL desired values must not trigger a Stripe write",
        );

        // Email-only change: the update passes None for name/address, and the
        // provider's leave-unchanged-on-None contract preserves them.
        let baseline = provider.update_billing_profile_call_count();
        run(
            &provider,
            &mut BillingContactStatus::default(),
            &tenant_row(Some("new@example.com"), None, None, None),
            pool.clone(),
        )
        .await;
        assert_eq!(provider.update_billing_profile_call_count() - baseline, 1);

        let customer = current_customer(&provider).await;
        assert_eq!(customer.email.as_deref(), Some("new@example.com"));
        assert_eq!(metadata_name(&customer), Some("Existing Name"));
        assert_eq!(
            customer.address.and_then(|a| a.city),
            Some("New York".to_string()),
        );
    }

    // The billing name is written to and compared against customer metadata rather
    // than `Customer.name`, which Flow sets to the tenant slug.
    #[sqlx::test(migrations = "../../supabase/migrations")]
    async fn writes_name_to_metadata_not_customer_name(pool: sqlx::PgPool) {
        let provider = customer_with(None, None, None).await;

        let baseline = provider.update_billing_profile_call_count();
        run(
            &provider,
            &mut BillingContactStatus::default(),
            &tenant_row(None, Some("Acme Billing"), None, None),
            pool.clone(),
        )
        .await;
        assert_eq!(provider.update_billing_profile_call_count() - baseline, 1);

        let customer = current_customer(&provider).await;
        assert_eq!(metadata_name(&customer), Some("Acme Billing"));
        assert_eq!(
            customer.name, None,
            "billing name must not land on Customer.name",
        );
    }

    // A missing Stripe customer (or an unconfigured provider) is a clean no-op
    // that stays Idle, so no retry backoff accrues.
    #[sqlx::test(migrations = "../../supabase/migrations")]
    async fn missing_customer_is_idle(pool: sqlx::PgPool) {
        let empty = Arc::new(InMemoryBillingProvider::new());
        let row = tenant_row(Some("new@example.com"), None, None, None);
        let mut status = BillingContactStatus::default();

        let outcome = run(&empty, &mut status, &row, pool.clone()).await;
        assert!(matches!(outcome, Outcome::Idle));
        assert_eq!(status.failures, 0);

        // No provider at all is likewise idle.
        let tenant_store: Arc<dyn TenantStore> = Arc::new(PgTenantStore::new(pool.clone()));
        let outcome = reconcile(&mut status, &row, &None, &tenant_store)
            .await
            .unwrap();
        assert!(matches!(outcome, Outcome::Idle));
    }

    // Address changes are detected field-by-field, and the stored JSONB
    // round-trips into `stripe::Address`. An unchanged address is a no-op.
    #[tokio::test]
    #[sqlx::test(migrations = "../../supabase/migrations")]
    async fn reconciles_address_changes(pool: sqlx::PgPool) {
        // Changed address -> one write, new value applied.
        let provider = customer_with(None, None, Some(address("New York"))).await;
        let baseline = provider.update_billing_profile_call_count();
        run(
            &provider,
            &mut BillingContactStatus::default(),
            &tenant_row(None, None, Some(stored_address("Boston")), None),
            pool.clone(),
        )
        .await;
        assert_eq!(provider.update_billing_profile_call_count() - baseline, 1);
        let customer = current_customer(&provider).await;
        assert_eq!(
            customer.address.and_then(|a| a.city),
            Some("Boston".to_string()),
        );

        // Identical address -> no write.
        let provider = customer_with(None, None, Some(address("Boston"))).await;
        let baseline = provider.update_billing_profile_call_count();
        run(
            &provider,
            &mut BillingContactStatus::default(),
            &tenant_row(None, None, Some(stored_address("Boston")), None),
            pool.clone(),
        )
        .await;
        assert_eq!(
            provider.update_billing_profile_call_count() - baseline,
            0,
            "an unchanged address must not trigger a Stripe write",
        );
    }

    // The retry/backoff state machine: a failure records the failure and schedules
    // a retry; while that retry is pending reconcile short-circuits without
    // touching Stripe; a later success clears the state.
    #[sqlx::test(migrations = "../../supabase/migrations")]
    async fn retry_backoff_state_machine(pool: sqlx::PgPool) {
        let provider = customer_with(None, None, None).await;
        let mut status = BillingContactStatus::default();

        // A failure (here, an un-deserializable stored address) records a retry.
        let outcome = run(
            &provider,
            &mut status,
            &tenant_row(None, None, Some(serde_json::json!("not an address")), None),
            pool.clone(),
        )
        .await;
        assert!(matches!(outcome, Outcome::WaitForRetry(_)));
        assert_eq!(status.failures, 1);
        assert!(status.next_retry.is_some());
        assert!(status.last_error.is_some());

        // While the retry is pending, reconcile does not call do_reconcile: even a
        // real mismatch is left untouched until the backoff elapses.
        let mismatch = tenant_row(Some("new@example.com"), None, None, None);
        let baseline = provider.update_billing_profile_call_count();
        let outcome = run(&provider, &mut status, &mismatch, pool.clone()).await;
        assert!(matches!(outcome, Outcome::WaitForRetry(_)));
        assert_eq!(provider.update_billing_profile_call_count() - baseline, 0);

        // A success (once the retry window has elapsed) resets the failure state.
        status.next_retry = None;
        let outcome = run(&provider, &mut status, &mismatch, pool.clone()).await;
        assert!(matches!(outcome, Outcome::Idle));
        assert_eq!(status.failures, 0);
        assert!(status.next_retry.is_none());
        assert!(status.last_error.is_none());
    }
}
