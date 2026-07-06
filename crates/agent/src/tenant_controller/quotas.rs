use crate::{
    storage::tenants::{PaymentProvider, TenantStore},
    tenant_controller::{
        Tenant,
        billing_contact::{BillingContactStatus, Outcome, retry_backoff},
    },
};
use anyhow::Context;
use control_plane_api::billing::BillingProvider;
use std::sync::Arc;

/// Update the quotas for a tenant that entered their payment information.
/// Note that we are going to fetch the customer twice from the BillingProvider
pub async fn update_quotas(
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

    match do_quota_update(status, tenant_row, billing_provider, tenant_provider).await {
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

async fn do_quota_update(
    status: &mut BillingContactStatus,
    tenant_row: &Tenant,
    billing_provider: &Option<Arc<dyn BillingProvider>>,
    tenant_provider: &Arc<dyn TenantStore>,
) -> anyhow::Result<()> {
    // Quotas have already been updated.
    if status.updated_quotas {
        return Ok(());
    }

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
    // There are two ways to set the default payment method:
    // 1. customer.default_source.is_some() (old)
    // 2. customer.invoice_settings.default_payment_method.is_some() (new)

    // This is the old way of checking for a default payment method according to AI.
    let has_default_payment_configured = if customer.default_source.is_some() {
        true
    } else if let Some(invoice_settings) = customer.invoice_settings.as_ref() {
        // This is the new way of handling default payment method.
        invoice_settings.default_payment_method.is_some()
    } else {
        false
    };
    // If the default payment is configured OR the payment_provider is set to external
    if has_default_payment_configured
        || tenant_row.payment_provider == Some(PaymentProvider::External)
    {
        tenant_provider
            .update_tenant_quotas_by_name(&tenant_row.tenant)
            .await?;
        status.updated_quotas = true
    }
    Ok(())
}
