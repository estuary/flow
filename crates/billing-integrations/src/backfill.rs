//! Backfill Stripe IDs recorded by billing automation.
//!
//! Stripe's List APIs are scanned before any database writes. By default the
//! command is read-only; `--apply` records only an ambiguity-free plan. Stripe
//! objects missing from List results may be deleted and cannot be distinguished
//! from objects that never existed.
//!
//! Run with:
//! `cargo run -p billing-integrations -- backfill-stripe-ids --connection-string ... --stripe-api-key ...`

use anyhow::{Context, bail};
use billing_types::{InvoiceMetadata, InvoiceType, TENANT_METADATA_KEY};
use chrono::{Datelike, NaiveDate};
use clap::Args;
use std::collections::{BTreeMap, BTreeSet, HashMap};

const INVOICE_TYPE_KEY: &str = "estuary.dev/invoice_type";
const PERIOD_START_KEY: &str = "estuary.dev/period_start";
const PERIOD_END_KEY: &str = "estuary.dev/period_end";
const BILLED_MONTH_KEY: &str = "estuary.dev/billed_month";

#[derive(Debug, Args)]
pub struct BackfillStripeIds {
    /// Control-plane DB connection string.
    #[clap(long)]
    connection_string: String,
    /// Stripe API key.
    #[clap(long)]
    stripe_api_key: String,
    /// Write the validated plan. Without this flag, the command is read-only.
    #[clap(long)]
    apply: bool,
}

/// The natural key of a bill, as recorded in a Stripe invoice's metadata.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum BillKey {
    /// A `billing_historicals` statement, keyed by its billed month.
    Usage { tenant: String, month: NaiveDate },
    /// A `manual_bills` row.
    Manual {
        tenant: String,
        date_start: NaiveDate,
        date_end: NaiveDate,
    },
}

/// The control-plane DB's tenants and bills, with their recorded Stripe IDs.
#[derive(Debug, Default)]
struct DbState {
    tenants: BTreeMap<String, Option<String>>,
    bills: BTreeMap<BillKey, Option<String>>,
}

/// IDs to record, and findings for an operator.
#[derive(Debug, Default)]
struct Plan {
    customers: Vec<(String, stripe::CustomerId)>,
    invoices: Vec<(BillKey, stripe::InvoiceId)>,
    findings: Vec<String>,
    observations: Vec<String>,
}

pub async fn backfill(cmd: &BackfillStripeIds) -> anyhow::Result<()> {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&cmd.connection_string)
        .await
        .context("connecting to the control-plane DB")?;
    let db = fetch_db_state(&pool).await?;
    pool.close().await;

    let client = stripe::Client::new(cmd.stripe_api_key.clone())
        .with_strategy(stripe::RequestStrategy::ExponentialBackoff(4));
    let customers = list_customers(&client).await?;
    let invoices = list_invoices(&client).await?;
    println!(
        "Scanned {} Stripe customers and {} Stripe invoices returned by the List APIs; deleted or otherwise unlisted objects cannot be distinguished from absent objects.",
        customers.len(),
        invoices.len()
    );

    let plan = plan(&db, customers, invoices);
    let null_customers = db.tenants.values().filter(|id| id.is_none()).count();
    let null_bills = db.bills.values().filter(|id| id.is_none()).count();
    let remaining_bills = null_bills.saturating_sub(plan.invoices.len());
    for observation in &plan.observations {
        println!("INFO: {observation}");
    }
    for finding in &plan.findings {
        println!("BLOCKED: {finding}");
    }
    println!(
        "{} blocking findings; {} informational observations; {} customer and {} invoice IDs can be filled; after this plan {} tenant customer IDs and {} bill invoice IDs would remain NULL",
        plan.findings.len(),
        plan.observations.len(),
        plan.customers.len(),
        plan.invoices.len(),
        null_customers.saturating_sub(plan.customers.len()),
        remaining_bills
    );
    if !plan.findings.is_empty() {
        bail!("resolve blocking findings before recording IDs; no database writes were made");
    }
    if !cmd.apply {
        println!("Read-only run; pass --apply to record this plan");
        return Ok(());
    }

    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&cmd.connection_string)
        .await
        .context("reconnecting to the control-plane DB to apply the plan")?;
    let (customers, invoices) = record(&pool, &plan).await?;
    println!("Recorded {customers} customer and {invoices} invoice IDs");
    Ok(())
}

async fn fetch_db_state(pool: &sqlx::PgPool) -> anyhow::Result<DbState> {
    let mut state = DbState::default();

    let tenants: Vec<(String, Option<String>)> =
        sqlx::query_as("select tenant::text, stripe_customer_id from public.tenants")
            .fetch_all(pool)
            .await?;
    state.tenants.extend(tenants);

    // Usage bills are keyed by month as `internal.invoices_ext` computes it.
    let usage: Vec<(String, NaiveDate, Option<String>)> = sqlx::query_as(
        r#"
        select tenant::text, date_trunc('month', billed_month)::date, stripe_invoice_id
        from internal.billing_historicals
        "#,
    )
    .fetch_all(pool)
    .await?;
    for (tenant, month, recorded) in usage {
        anyhow::ensure!(
            state
                .bills
                .insert(
                    BillKey::Usage {
                        tenant: tenant.clone(),
                        month
                    },
                    recorded
                )
                .is_none(),
            "multiple historical bills share tenant {tenant} and calendar month {month}"
        );
    }

    let manual: Vec<(String, NaiveDate, NaiveDate, Option<String>)> = sqlx::query_as(
        "select tenant::text, date_start, date_end, stripe_invoice_id from internal.manual_bills",
    )
    .fetch_all(pool)
    .await?;
    for (tenant, date_start, date_end, recorded) in manual {
        state.bills.insert(
            BillKey::Manual {
                tenant,
                date_start,
                date_end,
            },
            recorded,
        );
    }
    Ok(state)
}

async fn list_customers(client: &stripe::Client) -> anyhow::Result<Vec<stripe::Customer>> {
    let mut params = stripe::ListCustomers {
        limit: Some(100),
        ..Default::default()
    };
    let mut customers = Vec::new();
    loop {
        let page = stripe::Customer::list(client, &params)
            .await
            .context("listing Stripe customers")?;
        let has_more = page.has_more;
        let cursor = page.data.last().map(|c| c.id.clone());
        customers.extend(page.data);
        if !has_more {
            break;
        }
        params.starting_after =
            Some(cursor.context("Stripe customer page has_more without a cursor")?);
    }
    eprintln!("Listed {} Stripe customers", customers.len());
    Ok(customers)
}

async fn list_invoices(client: &stripe::Client) -> anyhow::Result<Vec<stripe::Invoice>> {
    let mut params = stripe::ListInvoices {
        limit: Some(100),
        ..Default::default()
    };
    let mut invoices = Vec::new();
    loop {
        let page = stripe::Invoice::list(client, &params)
            .await
            .context("listing Stripe invoices")?;
        let has_more = page.has_more;
        let cursor = page.data.last().map(|i| i.id.clone());
        invoices.extend(page.data);
        if !has_more {
            break;
        }
        params.starting_after =
            Some(cursor.context("Stripe invoice page has_more without a cursor")?);
        if invoices.len() % 5000 == 0 {
            eprintln!("Listed {} Stripe invoices...", invoices.len());
        }
    }
    eprintln!("Listed {} Stripe invoices", invoices.len());
    Ok(invoices)
}

/// Match supported historical Stripe metadata to a persisted bill key.
fn bill_key(metadata: &HashMap<String, String>) -> Result<Option<BillKey>, String> {
    let tenant = metadata
        .get(TENANT_METADATA_KEY)
        .ok_or_else(|| "missing tenant metadata".to_string())?
        .clone();
    if let Some(month) = metadata.get(BILLED_MONTH_KEY) {
        if [INVOICE_TYPE_KEY, PERIOD_START_KEY, PERIOD_END_KEY]
            .iter()
            .any(|key| metadata.contains_key(*key))
        {
            return Err("legacy billed_month conflicts with invoice_type/period metadata".into());
        }
        return NaiveDate::parse_from_str(&format!("{month}-01"), "%Y-%m-%d")
            .map(|month| Some(BillKey::Usage { tenant, month }))
            .map_err(|error| format!("invalid legacy billed_month {month:?}: {error}"));
    }

    let mut normalized = metadata.clone();
    if let Some(invoice_type) = normalized.get_mut(INVOICE_TYPE_KEY) {
        match invoice_type.as_str() {
            "Usage" | "usage" => *invoice_type = "final".to_string(),
            "Manual" => *invoice_type = "manual".to_string(),
            _ => {}
        }
    }
    let Some(parsed) = InvoiceMetadata::from_metadata_map(&normalized) else {
        return Err("incomplete or unrecognized tenant/invoice_type/period metadata".into());
    };
    let parse_date = |date: &str| {
        NaiveDate::parse_from_str(date, "%Y-%m-%d")
            .map_err(|error| format!("invalid period date {date:?}: {error}"))
    };
    let key = match parsed.invoice_type {
        InvoiceType::Final => parse_date(&parsed.period_start).map(|date| {
            Some(BillKey::Usage {
                tenant,
                month: date.with_day(1).unwrap(),
            })
        }),
        InvoiceType::Manual => parse_date(&parsed.period_start).and_then(|date_start| {
            parse_date(&parsed.period_end).map(|date_end| {
                Some(BillKey::Manual {
                    tenant,
                    date_start,
                    date_end,
                })
            })
        }),
        InvoiceType::Preview => Ok(None),
    };
    key
}

fn status(invoice: &stripe::Invoice) -> &str {
    invoice
        .status
        .as_ref()
        .map_or("unknown status", |status| status.as_str())
}

fn select_invoice(candidates: Vec<stripe::Invoice>) -> anyhow::Result<Option<stripe::Invoice>> {
    let (live, void): (Vec<_>, Vec<_>) = candidates
        .into_iter()
        .partition(|invoice| invoice.status != Some(stripe::InvoiceStatus::Void));
    match live.len() {
        0 => {
            let Some(latest) = void.iter().map(|invoice| invoice.created).max() else {
                return Ok(None);
            };
            let latest: Vec<_> = void
                .into_iter()
                .filter(|invoice| invoice.created == latest)
                .collect();
            anyhow::ensure!(
                latest.len() == 1,
                "newest voided invoices tie: {}",
                latest
                    .iter()
                    .map(|invoice| invoice.id.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            Ok(latest.into_iter().next())
        }
        1 => Ok(live.into_iter().next()),
        _ => bail!(
            "multiple non-void invoices match: {}",
            live.iter()
                .map(|invoice| format!("{} ({})", invoice.id, status(invoice)))
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

/// Match Stripe customers and invoices to the DB's tenants and bills.
fn plan(db: &DbState, customers: Vec<stripe::Customer>, invoices: Vec<stripe::Invoice>) -> Plan {
    let mut plan = Plan::default();
    let mut customers_by_tenant: BTreeMap<String, Vec<stripe::Customer>> = BTreeMap::new();
    let mut flow_customer_tenants: BTreeMap<String, String> = BTreeMap::new();
    for customer in customers {
        let Some(tenant) = customer
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.get(TENANT_METADATA_KEY))
        else {
            continue;
        };
        flow_customer_tenants.insert(customer.id.to_string(), tenant.clone());
        if !db.tenants.contains_key(tenant) {
            plan.observations.push(format!(
                "customer {} names unknown tenant {tenant}",
                customer.id
            ));
            continue;
        }
        customers_by_tenant
            .entry(tenant.clone())
            .or_default()
            .push(customer);
    }

    let mut tenant_customer: BTreeMap<String, String> = db
        .tenants
        .iter()
        .filter_map(|(tenant, recorded)| recorded.as_ref().map(|id| (tenant.clone(), id.clone())))
        .collect();
    let recorded_customer_owners: BTreeMap<String, String> = db
        .tenants
        .iter()
        .filter_map(|(tenant, recorded)| recorded.as_ref().map(|id| (id.clone(), tenant.clone())))
        .collect();
    for (tenant, candidates) in &customers_by_tenant {
        let Some(recorded) = db.tenants.get(tenant) else {
            continue;
        };
        let candidates = candidates.iter().filter(|customer| {
            match recorded_customer_owners.get(customer.id.as_str()) {
                Some(owner) if owner != tenant => {
                    plan.observations.push(format!("customer {} is already recorded for tenant {owner}; preserving that association", customer.id));
                    false
                }
                _ => true,
            }
        }).collect::<Vec<_>>();
        let ids = candidates
            .iter()
            .map(|customer| customer.id.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        match recorded {
            Some(id) => {
                tenant_customer.insert(tenant.clone(), id.clone());
                let extras: Vec<_> = candidates
                    .iter()
                    .filter(|candidate| candidate.id.as_str() != id)
                    .collect();
                if !extras.is_empty() {
                    plan.observations.push(format!(
                        "tenant {tenant} retains authoritative customer {id}; additional metadata matches: {}",
                        extras.iter().map(|customer| customer.id.as_str()).collect::<Vec<_>>().join(", ")
                    ));
                }
            }
            None if candidates.is_empty() => {}
            None if candidates.len() == 1 => {
                let id = candidates[0].id.clone();
                tenant_customer.insert(tenant.clone(), id.to_string());
                plan.customers.push((tenant.clone(), id));
            }
            None => plan.findings.push(format!(
                "tenant {tenant} matches multiple Stripe customers: {ids}"
            )),
        }
    }

    let recorded_invoice_ids: BTreeSet<&str> =
        db.bills.values().filter_map(|id| id.as_deref()).collect();
    let mut invoices_by_bill: BTreeMap<BillKey, Vec<stripe::Invoice>> = BTreeMap::new();
    for invoice in invoices {
        if recorded_invoice_ids.contains(invoice.id.as_str()) {
            continue;
        }
        let metadata = invoice.metadata.as_ref();
        let tenant = metadata.and_then(|metadata| metadata.get(TENANT_METADATA_KEY));
        if tenant.is_none() {
            let customer_id = invoice
                .customer
                .as_ref()
                .map(|customer| customer.id().to_string());
            if let Some(owner) = customer_id
                .as_ref()
                .and_then(|id| flow_customer_tenants.get(id))
            {
                plan.observations.push(format!(
                    "invoice {} belongs to Flow customer {owner} but lacks tenant metadata",
                    invoice.id
                ));
            }
            continue;
        }
        match bill_key(metadata.unwrap()) {
            Ok(Some(key)) => invoices_by_bill.entry(key).or_default().push(invoice),
            Ok(None) => {}
            Err(error) => plan.observations.push(format!(
                "invoice {} has unsupported or malformed Flow metadata: {error}",
                invoice.id
            )),
        }
    }

    for (key, candidates) in invoices_by_bill {
        let Some(recorded) = db.bills.get(&key) else {
            let new_ids = candidates
                .iter()
                .filter(|invoice| !recorded_invoice_ids.contains(invoice.id.as_str()))
                .collect::<Vec<_>>();
            if !new_ids.is_empty() {
                let tenant = match &key {
                    BillKey::Usage { tenant, .. } | BillKey::Manual { tenant, .. } => tenant,
                };
                let live_tenant = db.tenants.contains_key(tenant);
                plan.observations.push(format!(
                    "invoices {} match no persisted bill {key:?}{}",
                    new_ids
                        .iter()
                        .map(|invoice| invoice.id.as_str())
                        .collect::<Vec<_>>()
                        .join(", "),
                    if live_tenant {
                        " (review possible edited/deleted bill dates)"
                    } else {
                        " (tenant is not present in DB)"
                    }
                ));
            }
            continue;
        };
        let Some(recorded) = recorded else {
            let invoice = match select_invoice(candidates) {
                Ok(Some(invoice)) => invoice,
                Ok(None) => continue,
                Err(error) => {
                    plan.findings
                        .push(format!("bill {key:?} cannot select an invoice: {error:#}"));
                    continue;
                }
            };
            let tenant = match &key {
                BillKey::Usage { tenant, .. } | BillKey::Manual { tenant, .. } => tenant,
            };
            let Some(customer_id) = invoice
                .customer
                .as_ref()
                .map(|customer| customer.id().to_string())
            else {
                plan.findings.push(format!(
                    "invoice {} for {key:?} has no customer ID",
                    invoice.id
                ));
                continue;
            };
            if let Some(owner) = recorded_customer_owners.get(&customer_id) {
                if owner != tenant {
                    plan.findings.push(format!(
                        "invoice {} maps to {key:?}, but customer {customer_id} is recorded for tenant {owner}",
                        invoice.id
                    ));
                    continue;
                }
            } else if tenant_customer.get(tenant) == Some(&customer_id) {
                // A recorded or planned tenant association takes precedence over Stripe metadata.
            } else if let Some(owner) = flow_customer_tenants.get(&customer_id) {
                if owner != tenant {
                    plan.findings.push(format!(
                        "invoice {} metadata names tenant {tenant}, but its Stripe customer {customer_id} metadata names {owner}",
                        invoice.id
                    ));
                    continue;
                }
            } else {
                plan.findings.push(format!(
                    "invoice {} maps to {key:?} but its customer {customer_id} is unlisted; verify or record the customer association before applying",
                    invoice.id
                ));
                continue;
            }
            plan.invoices.push((key, invoice.id));
            continue;
        };

        // A persisted invoice ID is authoritative even when Stripe metadata has changed.
        if candidates
            .iter()
            .any(|invoice| invoice.id.as_str() != recorded)
        {
            plan.observations.push(format!(
                "bill {key:?} retains authoritative invoice {recorded}; Stripe metadata also matches other invoice IDs"
            ));
        }
    }
    plan
}

/// Record the plan's IDs where none is recorded yet, in one transaction.
/// Returns the numbers of customer and invoice IDs recorded.
async fn record(pool: &sqlx::PgPool, plan: &Plan) -> anyhow::Result<(u64, u64)> {
    anyhow::ensure!(
        plan.findings.is_empty(),
        "cannot apply a plan with blocking findings"
    );
    let mut txn = pool.begin().await?;
    let (mut customers, mut invoices) = (0, 0);

    for (tenant, customer_id) in &plan.customers {
        let updated = sqlx::query(
            r#"
            update public.tenants set stripe_customer_id = $2
            where tenant = $1 and stripe_customer_id is null
            "#,
        )
        .bind(tenant)
        .bind(customer_id.as_str())
        .execute(&mut *txn)
        .await
        .with_context(|| format!("recording customer {customer_id} of tenant {tenant}"))?
        .rows_affected();
        anyhow::ensure!(
            updated == 1,
            "conflict recording customer {customer_id} of tenant {tenant}; transaction rolled back"
        );
        customers += updated;
    }

    for (key, invoice_id) in &plan.invoices {
        let updated = match key {
            BillKey::Usage { tenant, month } => {
                sqlx::query(
                    r#"
                    update internal.billing_historicals set stripe_invoice_id = $3
                    where tenant = $1 and date_trunc('month', billed_month)::date = $2
                      and stripe_invoice_id is null
                    "#,
                )
                .bind(tenant)
                .bind(month)
                .bind(invoice_id.as_str())
                .execute(&mut *txn)
                .await
            }
            BillKey::Manual {
                tenant,
                date_start,
                date_end,
            } => {
                sqlx::query(
                    r#"
                    update internal.manual_bills set stripe_invoice_id = $4
                    where tenant = $1 and date_start = $2 and date_end = $3
                      and stripe_invoice_id is null
                    "#,
                )
                .bind(tenant)
                .bind(date_start)
                .bind(date_end)
                .bind(invoice_id.as_str())
                .execute(&mut *txn)
                .await
            }
        };
        let updated = updated
            .with_context(|| format!("recording invoice {invoice_id} of bill {key:?}"))?
            .rows_affected();
        anyhow::ensure!(
            updated == 1,
            "conflict recording invoice {invoice_id} of bill {key:?}; transaction rolled back"
        );
        invoices += updated;
    }

    txn.commit().await?;
    Ok((customers, invoices))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn customer(id: &str, tenant: &str) -> stripe::Customer {
        stripe::Customer {
            id: id.parse().unwrap(),
            metadata: Some(billing_types::tenant_metadata(tenant)),
            ..Default::default()
        }
    }

    fn invoice(
        id: &str,
        customer: &str,
        status: stripe::InvoiceStatus,
        metadata: &[(&str, &str)],
    ) -> stripe::Invoice {
        stripe::Invoice {
            id: id.parse().unwrap(),
            customer: Some(stripe::Expandable::Id(customer.parse().unwrap())),
            status: Some(status),
            metadata: Some(
                metadata
                    .iter()
                    .map(|(k, v)| (format!("estuary.dev/{k}"), v.to_string()))
                    .collect(),
            ),
            ..Default::default()
        }
    }

    fn usage(tenant: &str, month: &str) -> BillKey {
        BillKey::Usage {
            tenant: tenant.to_string(),
            month: month.parse().unwrap(),
        }
    }

    fn manual(tenant: &str, date_start: &str, date_end: &str) -> BillKey {
        BillKey::Manual {
            tenant: tenant.to_string(),
            date_start: date_start.parse().unwrap(),
            date_end: date_end.parse().unwrap(),
        }
    }

    #[test]
    fn backfill_cli_defaults_to_preview_and_accepts_apply() {
        use clap::Parser;
        for apply in [false, true] {
            let mut args = vec![
                "billing-integrations",
                "backfill-stripe-ids",
                "--connection-string",
                "db",
                "--stripe-api-key",
                "sk_test",
            ];
            if apply {
                args.push("--apply");
            }
            let cli = super::super::Cli::try_parse_from(args).unwrap();
            let super::super::Command::BackfillStripeIds(command) = cli.cmd else {
                panic!("expected backfill command");
            };
            assert_eq!(command.apply, apply);
        }
    }

    #[test]
    fn supports_historical_billed_month_metadata() {
        let metadata = HashMap::from([
            (TENANT_METADATA_KEY.to_string(), "acmeCo/".to_string()),
            (BILLED_MONTH_KEY.to_string(), "2023-08".to_string()),
        ]);
        assert_eq!(
            bill_key(&metadata).unwrap(),
            Some(usage("acmeCo/", "2023-08-01"))
        );

        let conflicting = HashMap::from([
            (TENANT_METADATA_KEY.to_string(), "acmeCo/".to_string()),
            (BILLED_MONTH_KEY.to_string(), "2023-08".to_string()),
            (INVOICE_TYPE_KEY.to_string(), "final".to_string()),
        ]);
        assert!(bill_key(&conflicting).is_err());
    }

    #[test]
    fn recorded_ids_cannot_be_reassigned_by_changed_metadata() {
        use stripe::InvoiceStatus::Paid;
        let db = DbState {
            tenants: BTreeMap::from([
                ("owner/".to_string(), Some("cus_owner".to_string())),
                ("target/".to_string(), None),
            ]),
            bills: BTreeMap::from([
                (
                    usage("owner/", "2026-08-01"),
                    Some("in_authoritative".to_string()),
                ),
                (usage("target/", "2026-08-01"), None),
            ]),
        };
        let customer = customer("cus_owner", "target/");
        let invoice = invoice(
            "in_authoritative",
            "cus_owner",
            Paid,
            &[
                ("tenant_name", "target/"),
                ("invoice_type", "final"),
                ("period_start", "2026-08-01"),
                ("period_end", "2026-08-31"),
            ],
        );
        let plan = plan(&db, vec![customer], vec![invoice]);
        assert!(plan.customers.is_empty());
        assert!(plan.invoices.is_empty());
        assert!(plan.findings.is_empty());
    }

    #[test]
    fn plans_recorded_ids() {
        use stripe::InvoiceStatus::{Draft, Open, Paid, Void};

        let db = DbState {
            tenants: BTreeMap::from([
                ("acmeCo/".to_string(), None),
                ("dupeCo/".to_string(), None),
                ("doneCo/".to_string(), Some("cus_done".to_string())),
            ]),
            bills: BTreeMap::from([
                (usage("acmeCo/", "2023-08-01"), None),
                (usage("acmeCo/", "2026-08-01"), None),
                (usage("acmeCo/", "2026-09-01"), None),
                (usage("acmeCo/", "2026-10-01"), None),
                (manual("acmeCo/", "2026-07-15", "2026-09-30"), None),
                (usage("dupeCo/", "2026-08-01"), None),
                (usage("doneCo/", "2026-08-01"), Some("in_done".to_string())),
            ]),
        };
        let customers = vec![
            customer("cus_acme", "acmeCo/"),
            customer("cus_dupe1", "dupeCo/"),
            customer("cus_dupe2", "dupeCo/"),
            customer("cus_other", "doneCo/"),
            customer("cus_gone", "goneCo/"),
            // Customers without Flow metadata are ignored.
            stripe::Customer {
                id: "cus_unrelated".parse().unwrap(),
                ..Default::default()
            },
        ];
        let final_aug = [
            ("tenant_name", "acmeCo/"),
            ("invoice_type", "final"),
            ("period_start", "2026-08-01"),
            ("period_end", "2026-08-31"),
        ];
        let invoices = vec![
            // An invoice from before September 2023's `invoice_type` rename.
            invoice(
                "in_legacy",
                "cus_acme",
                Paid,
                &[
                    ("tenant_name", "acmeCo/"),
                    ("invoice_type", "Usage"),
                    ("period_start", "2023-08-01"),
                    ("period_end", "2023-08-31"),
                ],
            ),
            // A replaced invoice and its replacement.
            invoice("in_aug_voided", "cus_acme", Void, &final_aug),
            invoice("in_aug", "cus_acme", Open, &final_aug),
            // Two live invoices for one bill.
            invoice(
                "in_sep1",
                "cus_acme",
                Draft,
                &[
                    ("tenant_name", "acmeCo/"),
                    ("invoice_type", "final"),
                    ("period_start", "2026-09-01"),
                    ("period_end", "2026-09-30"),
                ],
            ),
            invoice(
                "in_sep2",
                "cus_acme",
                Open,
                &[
                    ("tenant_name", "acmeCo/"),
                    ("invoice_type", "final"),
                    ("period_start", "2026-09-01"),
                    ("period_end", "2026-09-30"),
                ],
            ),
            // A manual bill whose end date was edited after it was invoiced.
            invoice(
                "in_manual",
                "cus_acme",
                Open,
                &[
                    ("tenant_name", "acmeCo/"),
                    ("invoice_type", "manual"),
                    ("period_start", "2026-07-15"),
                    ("period_end", "2026-09-14"),
                ],
            ),
            // Issued to a duplicate customer of the tenant.
            invoice(
                "in_dupe",
                "cus_dupe2",
                Paid,
                &[
                    ("tenant_name", "dupeCo/"),
                    ("invoice_type", "final"),
                    ("period_start", "2026-08-01"),
                    ("period_end", "2026-08-31"),
                ],
            ),
            // A bill whose recorded invoice differs from its metadata match.
            invoice(
                "in_done_other",
                "cus_done",
                Paid,
                &[
                    ("tenant_name", "doneCo/"),
                    ("invoice_type", "final"),
                    ("period_start", "2026-08-01"),
                    ("period_end", "2026-08-31"),
                ],
            ),
            invoice(
                "in_broken",
                "cus_acme",
                Paid,
                &[("tenant_name", "acmeCo/"), ("invoice_type", "final")],
            ),
            // Invoices without Flow metadata are ignored.
            invoice("in_unrelated", "cus_unrelated", Paid, &[]),
        ];

        let plan = plan(&db, customers, invoices);
        assert_eq!(plan.customers.len(), 1);
        assert_eq!(plan.customers[0].0, "acmeCo/");
        assert_eq!(plan.invoices.len(), 3);
        assert!(
            plan.invoices
                .iter()
                .any(|(key, id)| key == &usage("acmeCo/", "2023-08-01")
                    && id.as_str() == "in_legacy")
        );
        assert!(
            plan.invoices
                .iter()
                .any(|(key, id)| key == &usage("acmeCo/", "2026-08-01") && id.as_str() == "in_aug")
        );
        assert!(
            plan.findings
                .iter()
                .any(|finding| finding.contains("multiple non-void"))
        );
        assert!(
            plan.observations
                .iter()
                .any(|finding| finding.contains("no persisted bill")
                    && finding.contains("in_manual"))
        );
        assert!(
            plan.observations
                .iter()
                .any(|finding| finding.contains("authoritative invoice in_done"))
        );
        assert!(
            plan.observations
                .iter()
                .any(|finding| finding.contains("unsupported or malformed Flow metadata"))
        );
    }

    #[tokio::test]
    async fn stripe_inventory_uses_read_only_paginated_list_requests() {
        let (client, requests) = crate::stripe_stub::stripe_stub(vec![
            (axum::http::StatusCode::OK, serde_json::json!({"object":"list","data":[{"id":"cus_page1","object":"customer","metadata":{}}],"has_more":true,"url":"/v1/customers"})),
            (axum::http::StatusCode::OK, serde_json::json!({"object":"list","data":[{"id":"cus_page2","object":"customer","metadata":{}}],"has_more":false,"url":"/v1/customers"})),
            (axum::http::StatusCode::OK, serde_json::json!({"object":"list","data":[{"id":"in_page1","object":"invoice","customer":"cus_page2","status":"paid","metadata":{}}],"has_more":false,"url":"/v1/invoices"})),
        ]).await;

        let customers = list_customers(&client).await.unwrap();
        let invoices = list_invoices(&client).await.unwrap();
        assert_eq!(customers.len(), 2);
        assert_eq!(invoices.len(), 1);
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 3);
        assert!(requests.iter().all(|request| request.starts_with("GET ")));
        assert!(requests[0].starts_with("GET /v1/customers?"));
        assert!(requests[1].contains("starting_after=cus_page1"));
        assert!(requests[2].starts_with("GET /v1/invoices?"));
    }

    #[sqlx::test(migrations = "../../supabase/migrations")]
    async fn records_planned_ids(pool: sqlx::PgPool) {
        sqlx::raw_sql(
            r#"
            insert into tenants (id, tenant, stripe_customer_id) values
                (internal.id_generator(), 'acmeCo/', null),
                (internal.id_generator(), 'doneCo/', 'cus_done');
            insert into internal.billing_historicals (tenant, billed_month, report, stripe_invoice_id) values
                ('acmeCo/', '2026-08-01T00:00:00Z', '{}', null),
                ('doneCo/', '2026-08-01T00:00:00Z', '{}', 'in_done');
            insert into internal.manual_bills (tenant, usd_cents, description, date_start, date_end) values
                ('acmeCo/', 10000, 'Contract', '2026-07-15', '2026-09-14');
            "#,
        )
        .execute(&pool)
        .await
        .unwrap();

        let db = fetch_db_state(&pool).await.unwrap();
        assert_eq!(db.tenants.get("acmeCo/"), Some(&None));
        assert_eq!(db.bills.get(&usage("acmeCo/", "2026-08-01")), Some(&None));
        assert_eq!(
            db.bills.get(&manual("acmeCo/", "2026-07-15", "2026-09-14")),
            Some(&None)
        );

        // IDs recorded concurrently since the plan was made aren't overwritten.
        let record_plan = Plan {
            customers: vec![("acmeCo/".to_string(), "cus_acme".parse().unwrap())],
            invoices: vec![
                (usage("acmeCo/", "2026-08-01"), "in_aug".parse().unwrap()),
                (
                    manual("acmeCo/", "2026-07-15", "2026-09-14"),
                    "in_manual".parse().unwrap(),
                ),
            ],
            ..Default::default()
        };
        assert_eq!(record(&pool, &record_plan).await.unwrap(), (1, 2));

        let db = fetch_db_state(&pool).await.unwrap();
        assert_eq!(
            db.tenants.get("acmeCo/"),
            Some(&Some("cus_acme".to_string()))
        );
        assert_eq!(
            db.tenants.get("doneCo/"),
            Some(&Some("cus_done".to_string()))
        );
        assert_eq!(
            db.bills.get(&usage("acmeCo/", "2026-08-01")),
            Some(&Some("in_aug".to_string()))
        );
        assert_eq!(
            db.bills.get(&usage("doneCo/", "2026-08-01")),
            Some(&Some("in_done".to_string()))
        );
        assert_eq!(
            db.bills.get(&manual("acmeCo/", "2026-07-15", "2026-09-14")),
            Some(&Some("in_manual".to_string()))
        );

        let empty = plan(&db, vec![], vec![]);
        assert_eq!(record(&pool, &empty).await.unwrap(), (0, 0));
        assert_eq!(
            fetch_db_state(&pool).await.unwrap().tenants.get("doneCo/"),
            Some(&Some("cus_done".to_string()))
        );
    }

    #[sqlx::test(migrations = "../../supabase/migrations")]
    async fn cas_conflict_rolls_back_earlier_customer_update(pool: sqlx::PgPool) {
        sqlx::raw_sql(
            r#"
            insert into tenants (id, tenant, stripe_customer_id) values
                (internal.id_generator(), 'acmeCo/', null);
            insert into internal.billing_historicals (tenant, billed_month, report, stripe_invoice_id)
                values ('acmeCo/', '2026-08-01T00:00:00Z', '{}', null);
            "#,
        )
        .execute(&pool)
        .await
        .unwrap();

        let plan = Plan {
            customers: vec![("acmeCo/".to_string(), "cus_acme".parse().unwrap())],
            invoices: vec![(usage("acmeCo/", "2026-08-01"), "in_aug".parse().unwrap())],
            ..Default::default()
        };
        sqlx::query("update internal.billing_historicals set stripe_invoice_id = 'in_raced' where tenant = 'acmeCo/'")
            .execute(&pool)
            .await
            .unwrap();
        assert!(record(&pool, &plan).await.is_err());

        let state = fetch_db_state(&pool).await.unwrap();
        assert_eq!(state.tenants.get("acmeCo/"), Some(&None));
        assert_eq!(
            state.bills.get(&usage("acmeCo/", "2026-08-01")),
            Some(&Some("in_raced".to_string()))
        );
    }
}
