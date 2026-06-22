use std::sync::Arc;

use anyhow::Context;
use control_plane_api::billing::{BillingProvider, CUSTOMER_NAME_METADATA_KEY};

use super::TenantRow;

#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
pub struct BillingContactStatus {
    #[serde(default, skip_serializing_if = "is_zero")]
    pub failures: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_retry: Option<chrono::DateTime<chrono::Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

fn is_zero(i: &u32) -> bool {
    *i == 0
}

fn retry_backoff(failures: u32) -> std::time::Duration {
    match failures {
        0 => std::time::Duration::ZERO,
        1 => std::time::Duration::from_secs(60),
        2 => std::time::Duration::from_secs(300),
        _ => std::time::Duration::from_secs(900),
    }
}

pub enum Outcome {
    Idle,
    WaitForRetry(std::time::Duration),
}

pub async fn reconcile(
    status: &mut BillingContactStatus,
    tenant_row: &TenantRow,
    billing_provider: &Option<Arc<dyn BillingProvider>>,
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

    match do_reconcile(tenant_row, billing_provider).await {
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
    tenant_row: &TenantRow,
    billing_provider: &Option<Arc<dyn BillingProvider>>,
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

    provider
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
