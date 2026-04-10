use crate::stripe_utils::{SearchParams, stripe_search};
use anyhow::{Context, bail};
use chrono::{Duration, ParseError, Utc};
use futures::{FutureExt, StreamExt, TryFutureExt, TryStreamExt};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use sqlx::Postgres;
use sqlx::{Pool, postgres::PgPoolOptions, types::chrono::NaiveDate};
use std::collections::HashMap;
use stripe::InvoiceStatus;

pub const TENANT_METADATA_KEY: &str = "estuary.dev/tenant_name";
const CREATED_BY_BILLING_AUTOMATION: &str = "estuary.dev/created_by_automation";
pub const INVOICE_TYPE_KEY: &str = "estuary.dev/invoice_type";
pub const BILLING_PERIOD_START_KEY: &str = "estuary.dev/period_start";
pub const BILLING_PERIOD_END_KEY: &str = "estuary.dev/period_end";

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
#[clap(rename_all = "kebab_case")]
enum ChargeType {
    AutoCharge,
    SendInvoice,
}

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
/// Publish bills from the control-plane database as Stripe invoices.
pub struct PublishInvoice {
    /// Control-plane DB connection string
    #[clap(long)]
    connection_string: String,
    /// Stripe API key.
    #[clap(long)]
    stripe_api_key: String,
    /// Comma-separated list of tenants to publish invoices for
    #[clap(long, value_delimiter = ',', required_unless_present("all_tenants"))]
    tenants: Vec<String>,
    /// Generate invoices for all tenants that have bills in the provided month.
    #[clap(long, conflicts_with("tenants"))]
    all_tenants: bool,
    /// The month to generate invoices for, in format "YYYY-MM-DD"
    #[clap(long, value_parser = parse_date)]
    month: NaiveDate,
    /// Whether to delete and recreate finalized invoices
    #[clap(long)]
    recreate_finalized: bool,
    /// Stop execution after first failure
    #[clap(long)]
    fail_fast: bool,
    /// Whether to attempt to automatically charge the invoice or send it to be paid manually.
    ///
    /// NOTE: Invoices are still created as drafts and require approval, this setting only
    /// changes what happens once the invoice is approved.
    #[clap(long, value_enum, default_value_t = ChargeType::AutoCharge)]
    charge_type: ChargeType,
    /// Number of invoices to publish concurrently
    #[clap(long, default_value_t = 2)]
    pub concurrency: usize,
    /// Clean up dangling invoices that are not in the database
    #[clap(long, default_value_t = false)]
    pub clean_up: bool,
    /// Run in read-only mode: classify all invoices and report what would
    /// happen, without creating or modifying anything in Stripe.
    #[clap(long, default_value_t = false)]
    pub dry_run: bool,
}

fn parse_date(arg: &str) -> Result<NaiveDate, ParseError> {
    NaiveDate::parse_from_str(arg, "%Y-%m-%d")
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, sqlx::Type, Serialize, Deserialize)]
#[sqlx(rename_all = "snake_case")]
enum InvoiceType {
    #[serde(rename = "final")]
    Final,
    #[serde(rename = "preview")]
    Preview,
    #[serde(rename = "manual")]
    Manual,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Extra {
    trial_start: Option<NaiveDate>,
    trial_credit: Option<i64>,
    recurring_fee: Option<i64>,
    task_usage_hours: Option<f64>,
    processed_data_gb: Option<f64>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct LineItem {
    count: f64,
    subtotal: i64,
    rate: i64,
    description: Option<String>,
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Hash)]
enum InvoiceResult {
    Created(PaymentProvider),
    Updated,
    LessThanMinimum,
    FreeTier,
    FutureTrialStart,
    NoDataMoved,
    NoFullPipeline,
    AlreadyProcessed,
    Error,
}

impl InvoiceResult {
    pub fn message(&self, dry_run: bool) -> String {
        match self {
            InvoiceResult::Created(provider) => {
                let verb = if dry_run {
                    "Would publish"
                } else {
                    "Published"
                };
                if provider == &PaymentProvider::Stripe {
                    format!("{verb} new invoice")
                } else {
                    format!("{verb} new invoice for tenant using {provider:?} provider")
                }
            }
            InvoiceResult::Updated => {
                if dry_run {
                    "Would update existing invoice".to_string()
                } else {
                    "Updated existing invoice".to_string()
                }
            }
            InvoiceResult::LessThanMinimum => {
                "Skipping invoice for less than the minimum chargable amount ($0.50)".to_string()
            }
            InvoiceResult::FreeTier => "Skipping usage invoice for tenant in free tier".to_string(),
            InvoiceResult::FutureTrialStart => {
                "Skipping invoice ending before free trial start date".to_string()
            }
            InvoiceResult::NoDataMoved => {
                "Skipping invoice for tenant with no data movement".to_string()
            }
            InvoiceResult::NoFullPipeline => {
                "Skipping invoice for tenant without an active pipeline".to_string()
            }
            InvoiceResult::AlreadyProcessed => {
                "Skipping invoice already processed in a previous billing run".to_string()
            }
            InvoiceResult::Error => "Error publishing invoices".to_string(),
        }
    }
}

/// The outcome of the classify phase: what action should be taken for this invoice.
enum InvoiceAction {
    /// Invoice should not be created. Carries the skip reason and the
    /// customer (if found) for potential clean-up of stale drafts.
    Skip {
        result: InvoiceResult,
        customer: Option<stripe::Customer>,
    },
    /// Create a new invoice. `replace` is set when --recreate-finalized
    /// requires deleting an existing invoice first.
    Create { replace: Option<stripe::InvoiceId> },
    /// Update an existing draft invoice's line items.
    Update {
        existing_invoice_id: stripe::InvoiceId,
    },
}

#[derive(
    Serialize, Deserialize, Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash, Copy, sqlx::Type,
)]
#[sqlx(type_name = "payment_provider_type", rename_all = "lowercase")]
enum PaymentProvider {
    Stripe,
    External,
}

#[derive(Serialize, Deserialize, Debug, Clone, sqlx::FromRow)]
struct Invoice {
    subtotal: i64,
    line_items: sqlx::types::Json<Vec<LineItem>>,
    date_start: NaiveDate,
    date_end: NaiveDate,
    billed_prefix: String,
    invoice_type: InvoiceType,
    extra: Option<sqlx::types::Json<Option<Extra>>>,
    has_payment_method: Option<bool>,
    has_full_pipeline: bool,
    payment_provider: PaymentProvider,
    tenant_trial_start: Option<NaiveDate>,
}

impl Invoice {
    pub async fn get_stripe_invoice(
        &self,
        client: &stripe::Client,
        customer_id: &str,
    ) -> anyhow::Result<Option<stripe::Invoice>> {
        let date_start_repr = self.date_start.format("%F").to_string();
        let date_end_repr = self.date_end.format("%F").to_string();

        let invoice_type_val =
            serde_json::to_value(self.invoice_type.clone()).expect("InvoiceType is serializable");
        let invoice_type_str = invoice_type_val
            .as_str()
            .expect("InvoiceType is serializable");

        let invoice_search = stripe_search::<stripe::Invoice>(
            &client,
            "invoices",
            SearchParams {
                query: format!(
                    r#"
                    -status:"deleted" AND
                    customer:"{customer_id}" AND
                    metadata["{INVOICE_TYPE_KEY}"]:"{invoice_type_str}" AND
                    metadata["{BILLING_PERIOD_START_KEY}"]:"{date_start_repr}" AND
                    metadata["{BILLING_PERIOD_END_KEY}"]:"{date_end_repr}"
                "#
                ),
                ..Default::default()
            },
        )
        .await
        .context("Searching for an invoice")?;

        Ok(invoice_search.into_iter().next())
    }

    /// Read-only classification: determines what action should be taken for this
    /// invoice without making any writes to Stripe.
    #[tracing::instrument(skip(self, client), fields(tenant=self.billed_prefix, invoice_type=format!("{:?}",self.invoice_type), subtotal=format!("${:.2}", self.subtotal as f64 / 100.0)))]
    async fn classify(
        &self,
        client: &stripe::Client,
        recreate_finalized: bool,
    ) -> anyhow::Result<InvoiceAction> {
        // --- Phase 1: Cheap local checks (no Stripe calls) ---

        match (&self.invoice_type, &self.extra) {
            (InvoiceType::Preview, _) => {
                bail!("Should not create Stripe invoices for preview invoices")
            }
            (InvoiceType::Final, None) => {
                bail!("Invoice should have extra")
            }
            _ => {}
        };

        if let InvoiceType::Final = self.invoice_type {
            match self.tenant_trial_start {
                Some(trial_start) if self.date_end < trial_start => {
                    return Ok(InvoiceAction::Skip {
                        result: InvoiceResult::FutureTrialStart,
                        customer: None,
                    });
                }
                None => {
                    return Ok(InvoiceAction::Skip {
                        result: InvoiceResult::FreeTier,
                        customer: None,
                    });
                }
                _ => {}
            }
        }

        if self.subtotal < 50 {
            return Ok(InvoiceAction::Skip {
                result: InvoiceResult::LessThanMinimum,
                customer: None,
            });
        }

        // --- Phase 2: Stripe calls (only for invoices that survived Phase 1) ---

        // For Final invoices, verify the payment method state with Stripe.
        // The DB capture has been known to be unreliable, so Stripe is the
        // source of truth. If the tenant has no payment method, skip on
        // NoDataMoved / NoFullPipeline.
        let mut found_customer: Option<Option<stripe::Customer>> = None;

        if let (InvoiceType::Final, Some(extra)) = (&self.invoice_type, &self.extra) {
            let validated_has_payment_method =
                if let Some(has_payment_method) = self.has_payment_method {
                    let customer = find_customer(client, &self.billed_prefix).await?;
                    let real_has_pm = customer
                        .as_ref()
                        .and_then(|c| c.invoice_settings.as_ref())
                        .and_then(|i| i.default_payment_method.as_ref())
                        .is_some();

                    if has_payment_method != real_has_pm {
                        tracing::warn!(
                            ?has_payment_method,
                            stripe_payment_method = real_has_pm,
                            "Inconsistent payment method state"
                        );
                    }

                    found_customer = Some(customer);
                    real_has_pm
                } else {
                    false
                };

            if !validated_has_payment_method {
                let unwrapped_extra = extra.clone().0.expect(
                    "This is just a sqlx quirk, if the outer Option is Some then this will be Some",
                );

                if unwrapped_extra.processed_data_gb.unwrap_or_default() == 0.0 {
                    return Ok(InvoiceAction::Skip {
                        result: InvoiceResult::NoDataMoved,
                        customer: found_customer.flatten(),
                    });
                }

                if !self.has_full_pipeline {
                    return Ok(InvoiceAction::Skip {
                        result: InvoiceResult::NoFullPipeline,
                        customer: found_customer.flatten(),
                    });
                }
            }
        }

        // Look up customer (reuse if already fetched during payment method validation)
        let customer = match found_customer {
            Some(c) => c,
            None => find_customer(client, &self.billed_prefix).await?,
        };

        let customer = match customer {
            Some(c) => c,
            // No customer in Stripe means no existing invoice is possible
            None => return Ok(InvoiceAction::Create { replace: None }),
        };

        let customer_id = customer.id.to_string();

        // Search for an existing invoice in Stripe
        if let Some(invoice) = self
            .get_stripe_invoice(client, customer_id.as_str())
            .await?
        {
            match invoice.status {
                Some(stripe::InvoiceStatus::Open | stripe::InvoiceStatus::Draft)
                    if recreate_finalized =>
                {
                    Ok(InvoiceAction::Create {
                        replace: Some(invoice.id),
                    })
                }
                Some(stripe::InvoiceStatus::Draft) => {
                    tracing::debug!(
                        "Found existing draft invoice {id}",
                        id = invoice.id.to_string()
                    );
                    Ok(InvoiceAction::Update {
                        existing_invoice_id: invoice.id,
                    })
                }
                Some(stripe::InvoiceStatus::Open)
                    if matches!(self.invoice_type, InvoiceType::Manual) =>
                {
                    tracing::debug!(
                        "Manual invoice {id} already open, skipping",
                        id = invoice.id.to_string()
                    );
                    Ok(InvoiceAction::Skip {
                        result: InvoiceResult::AlreadyProcessed,
                        customer: Some(customer),
                    })
                }
                Some(stripe::InvoiceStatus::Open) => {
                    bail!(
                        "Found open invoice {id}. Pass --recreate-finalized to delete and recreate this invoice.",
                        id = invoice.id.to_string()
                    )
                }
                Some(
                    status @ (stripe::InvoiceStatus::Paid
                    | stripe::InvoiceStatus::Void
                    | stripe::InvoiceStatus::Uncollectible),
                ) if matches!(self.invoice_type, InvoiceType::Manual) => {
                    tracing::debug!(
                        "Manual invoice {id} already in state {status}, skipping",
                        id = invoice.id.to_string(),
                        status = status
                    );
                    Ok(InvoiceAction::Skip {
                        result: InvoiceResult::AlreadyProcessed,
                        customer: Some(customer),
                    })
                }
                Some(status) => {
                    bail!(
                        "Found invoice {id} in unsupported state {status}, skipping.",
                        id = invoice.id.to_string(),
                        status = status
                    );
                }
                None => {
                    bail!(
                        "Unexpected missing status from invoice {id}",
                        id = invoice.id.to_string()
                    );
                }
            }
        } else {
            Ok(InvoiceAction::Create { replace: None })
        }
    }

    /// Execute the classified action: performs all Stripe writes (customer creation,
    /// invoice creation/update, line item management, verification).
    #[tracing::instrument(skip(self, client, db_client, action), fields(tenant=self.billed_prefix, invoice_type=format!("{:?}",self.invoice_type), subtotal=format!("${:.2}", self.subtotal as f64 / 100.0)))]
    async fn execute(
        &self,
        client: &stripe::Client,
        db_client: &Pool<Postgres>,
        action: InvoiceAction,
        mode: ChargeType,
    ) -> anyhow::Result<InvoiceResult> {
        let (is_update, replace, existing_invoice_id) = match action {
            InvoiceAction::Skip { result, .. } => return Ok(result),
            InvoiceAction::Create { replace, .. } => (false, replace, None),
            InvoiceAction::Update {
                existing_invoice_id,
                ..
            } => (true, None, Some(existing_invoice_id)),
        };

        // Ensure customer exists and has an email (required for invoicing)
        let customer =
            ensure_customer_for_invoicing(client, db_client, &self.billed_prefix).await?;

        // Anything before 12:00:00 renders as the previous day in Stripe
        let date_start_secs = self
            .date_start
            .and_hms_opt(12, 0, 0)
            .expect("Error manipulating date")
            .and_local_timezone(Utc)
            .single()
            .expect("Error manipulating date")
            .timestamp();
        let date_end_secs = self
            .date_end
            .and_hms_opt(12, 0, 0)
            .expect("Error manipulating date")
            .and_local_timezone(Utc)
            .single()
            .expect("Error manipulating date")
            .timestamp();

        let date_start_human = self.date_start.format("%B %d %Y").to_string();
        let date_end_human = self.date_end.format("%B %d %Y").to_string();

        let date_start_repr = self.date_start.format("%F").to_string();
        let date_end_repr = self.date_end.format("%F").to_string();

        let invoice_type_val =
            serde_json::to_value(self.invoice_type.clone()).expect("InvoiceType is serializable");
        let invoice_type_str = invoice_type_val
            .as_str()
            .expect("InvoiceType is serializable");

        // Delete existing invoice if --recreate-finalized was used
        if let Some(ref replace_id) = replace {
            // Re-verify the invoice status before deleting (guard against race conditions)
            let existing = stripe::Invoice::retrieve(client, replace_id, &[]).await?;
            match existing.status {
                Some(state @ (stripe::InvoiceStatus::Open | stripe::InvoiceStatus::Draft)) => {
                    tracing::warn!(
                        "Found invoice {id} in state {state}, deleting and recreating",
                        id = replace_id.to_string(),
                        state = state
                    );
                    stripe::Invoice::delete(client, replace_id).await?;
                }
                Some(status) => {
                    bail!(
                        "Invoice {id} changed to state {status} since classification, cannot delete.",
                        id = replace_id.to_string(),
                        status = status
                    );
                }
                None => {
                    bail!(
                        "Unexpected missing status from invoice {id}",
                        id = replace_id.to_string()
                    );
                }
            }
        }

        // Create or reuse the invoice
        // Manual invoices should always be sent as invoices rather than
        // charged to the customer's payment method.
        let mode = if self.invoice_type == InvoiceType::Manual {
            ChargeType::SendInvoice
        } else {
            mode
        };

        let invoice = if let Some(existing_id) = existing_invoice_id {
            tracing::debug!(
                "Updating existing invoice {id}",
                id = existing_id.to_string()
            );
            stripe::Invoice::retrieve(client, &existing_id, &[]).await?
        } else {
            let description_text = format!(
                "Your Flow bill for the billing period between {date_start_human} - {date_end_human}. Tenant: {tenant}",
                tenant = self.billed_prefix
            );
            let invoice = stripe::Invoice::create(
                client,
                stripe::CreateInvoice {
                    customer: Some(customer.id.to_owned()),
                    due_date: match mode {
                        ChargeType::SendInvoice => {
                            Some((Utc::now() + Duration::days(30)).timestamp())
                        }
                        ChargeType::AutoCharge => None,
                    },
                    description: Some(description_text.as_str()),
                    collection_method: Some(match mode {
                        ChargeType::AutoCharge => stripe::CollectionMethod::ChargeAutomatically,
                        ChargeType::SendInvoice => stripe::CollectionMethod::SendInvoice,
                    }),
                    auto_advance: Some(false),
                    custom_fields: Some(vec![
                        stripe::CreateInvoiceCustomFields {
                            name: "Billing Period Start".to_string(),
                            value: date_start_human.to_owned(),
                        },
                        stripe::CreateInvoiceCustomFields {
                            name: "Billing Period End".to_string(),
                            value: date_end_human.to_owned(),
                        },
                    ]),
                    metadata: Some(HashMap::from([
                        (
                            TENANT_METADATA_KEY.to_string(),
                            self.billed_prefix.to_owned(),
                        ),
                        (INVOICE_TYPE_KEY.to_string(), invoice_type_str.to_owned()),
                        (BILLING_PERIOD_START_KEY.to_string(), date_start_repr),
                        (BILLING_PERIOD_END_KEY.to_string(), date_end_repr),
                    ])),
                    ..Default::default()
                },
            )
            .await
            .context("Creating a new invoice")?;
            tracing::debug!("Created a new invoice {id}", id = invoice.id);
            invoice
        };

        // Clear out line items from invoice, if there are any
        for item in stripe::InvoiceItem::list(
            client,
            &stripe::ListInvoiceItems {
                invoice: Some(invoice.id.to_owned()),
                ..Default::default()
            },
        )
        .await?
        .data
        .into_iter()
        {
            tracing::debug!(
                "Delete invoice line item: '{desc}'",
                desc = item.description.to_owned().unwrap_or_default()
            );
            stripe::InvoiceItem::delete(client, &item.id).await?;
        }

        let mut diff: f64 = 0.0;

        for item in self.line_items.iter() {
            let description = item.description.clone().unwrap_or("".to_string());
            tracing::debug!("Created new invoice line item: '{description}'");
            diff = diff + ((item.count.ceil() - item.count) * item.rate as f64);
            stripe::InvoiceItem::create(
                client,
                stripe::CreateInvoiceItem {
                    quantity: Some(item.count.ceil() as u64),
                    unit_amount: Some(item.rate),
                    currency: Some(stripe::Currency::USD),
                    description: Some(description.as_str()),
                    invoice: Some(invoice.id.to_owned()),
                    period: Some(stripe::Period {
                        start: Some(date_start_secs),
                        end: Some(date_end_secs),
                    }),
                    ..stripe::CreateInvoiceItem::new(customer.id.to_owned())
                },
            )
            .await?;
        }

        if diff > 0.0 {
            tracing::warn!(
                "Invoice line items use fractional quantities, which Stripe does not allow. Rounding up resulted in a difference of ${difference:.2}",
                difference = diff.ceil() / 100.0
            );
        }

        // Re-fetch invoice and customer for fresh data (balance may have changed)
        let check_invoice = stripe::Invoice::retrieve(client, &invoice.id, &[]).await?;
        let fresh_customer = stripe::Customer::retrieve(client, &customer.id, &[]).await?;
        let credit_balance = fresh_customer.balance.unwrap_or(0);

        let expected = (self.subtotal + (diff.ceil() as i64) + credit_balance).max(0);

        if !check_invoice.amount_due.eq(&Some(expected)) {
            bail!(
                "The correct bill is ${our_bill:.2}, but the invoice's total is ${their_bill:.2}",
                our_bill = self.subtotal as f64 / 100.0,
                their_bill = check_invoice.amount_due.unwrap_or(0) as f64 / 100.0
            )
        }

        if is_update {
            Ok(InvoiceResult::Updated)
        } else {
            Ok(InvoiceResult::Created(self.payment_provider))
        }
    }
}

pub async fn do_publish_invoices(cmd: &PublishInvoice) -> anyhow::Result<()> {
    let stripe_client = stripe::Client::new(cmd.stripe_api_key.to_owned())
        .with_strategy(stripe::RequestStrategy::ExponentialBackoff(4));
    let db_pool = PgPoolOptions::new()
        .max_connections(5)
        .after_connect(|conn, _meta| {
            Box::pin(async move {
                // Raise the statement timeout to 10 minutes
                sqlx::query("set statement_timeout to 600000")
                    .execute(conn)
                    .await?;
                Ok(())
            })
        })
        .connect(&cmd.connection_string)
        .await?;

    let month_human_repr = cmd.month.format("%B %Y");

    tracing::info!("Fetching billing data for {month_human_repr}");

    let invoices: Vec<Invoice> = if cmd.tenants.len() > 0 {
        sqlx::query_as!(
            Invoice,
            r#"
                select
                    date_start as "date_start!",
                    date_end as "date_end!",
                    billed_prefix as "billed_prefix!",
                    invoice_type as "invoice_type!: InvoiceType",
                    line_items as "line_items!: sqlx::types::Json<Vec<LineItem>>",
                    subtotal::bigint as "subtotal!",
                    extra as "extra: sqlx::types::Json<Option<Extra>>",
                    customer.has_payment_method as has_payment_method,
                    coalesce(dataflow.has_full_pipeline, false) as "has_full_pipeline!",
                    tenants.payment_provider as "payment_provider!: PaymentProvider",
                    tenants.trial_start as tenant_trial_start
                from invoices_ext
                left join tenants on tenants.tenant = billed_prefix
                left join lateral(
                	select bool_or("invoice_settings/default_payment_method" is not null) as has_payment_method
                	from stripe.customers
                	where customers.metadata->>'estuary.dev/tenant_name' = billed_prefix
                	group by billed_prefix
                ) as customer on true
                left join lateral(
                	select
                		sum(catalog_stats_monthly.usage_seconds) filter (where live_specs.spec_type = 'capture') > 0
                		and sum(catalog_stats_monthly.usage_seconds) filter (where live_specs.spec_type = 'materialization') > 0
                		as has_full_pipeline
                    from catalog_stats_monthly
                    join live_specs on live_specs.catalog_name ^@ catalog_stats_monthly.catalog_name
                    where
                    	catalog_stats_monthly.catalog_name = billed_prefix
                    	and tstzrange(date_trunc('day', $1::date), date_trunc('day', ($1::date)) + interval '1 month' - interval '1 day') @> catalog_stats_monthly.ts
                ) as dataflow on true
                where ((
                    date_start >= date_trunc('day', $1::date)
                    and date_end <= date_trunc('day', ($1::date)) + interval '1 month' - interval '1 day'
                    and invoice_type = 'final'
                ) or (
                    date_start <= date_trunc('day', ($1::date)) + interval '1 month' - interval '1 day'
                    and date_end >= date_trunc('day', $1::date)
                    and invoice_type = 'manual'
                ))
                and billed_prefix = any($2)
            "#,
            cmd.month,
            &cmd.tenants[..]
        )
        .fetch_all(&db_pool)
        .await?
    } else {
        sqlx::query_as!(
            Invoice,
            r#"
                select
                    date_start as "date_start!",
                    date_end as "date_end!",
                    billed_prefix as "billed_prefix!",
                    invoice_type as "invoice_type!: InvoiceType",
                    line_items as "line_items!: sqlx::types::Json<Vec<LineItem>>",
                    subtotal::bigint as "subtotal!",
                    extra as "extra: sqlx::types::Json<Option<Extra>>",
                    customer.has_payment_method as has_payment_method,
                    coalesce(dataflow.has_full_pipeline, false) as "has_full_pipeline!",
                    tenants.payment_provider as "payment_provider!: PaymentProvider",
                    tenants.trial_start as tenant_trial_start
                from invoices_ext
                left join tenants on tenants.tenant = billed_prefix
                left join lateral(
                	select bool_or("invoice_settings/default_payment_method" is not null) as has_payment_method
                	from stripe.customers
                	where customers.metadata->>'estuary.dev/tenant_name' = billed_prefix
                	group by billed_prefix
                ) as customer on true
                left join lateral(
                	select
                		sum(catalog_stats_monthly.usage_seconds) filter (where live_specs.spec_type = 'capture') > 0
                		and sum(catalog_stats_monthly.usage_seconds) filter (where live_specs.spec_type = 'materialization') > 0
                		as has_full_pipeline
                    from catalog_stats_monthly
                    join live_specs on live_specs.catalog_name ^@ catalog_stats_monthly.catalog_name
                    where
                    	catalog_stats_monthly.catalog_name = billed_prefix
                    	and tstzrange(date_trunc('day', $1::date), date_trunc('day', ($1::date)) + interval '1 month' - interval '1 day') @> catalog_stats_monthly.ts
                ) as dataflow on true
                where (
                    date_start >= date_trunc('day', $1::date)
                    and date_end <= date_trunc('day', ($1::date)) + interval '1 month' - interval '1 day'
                    and invoice_type = 'final'
                ) or (
                    date_start <= date_trunc('day', ($1::date)) + interval '1 month' - interval '1 day'
                    and date_end >= date_trunc('day', $1::date)
                    and invoice_type = 'manual'
                )
            "#,
            cmd.month
        )
        .fetch_all(&db_pool)
        .await?
    };

    let mut invoice_type_counter: HashMap<InvoiceType, usize> = HashMap::new();
    invoices.iter().for_each(|invoice| {
        *invoice_type_counter
            .entry(invoice.invoice_type.clone())
            .or_default() += 1;
    });

    if cmd.dry_run {
        tracing::info!(
            "[DRY RUN] Classifying {usage} usage-based invoices and {manual} manually-entered invoices without making any changes to Stripe.",
            usage = invoice_type_counter
                .remove(&InvoiceType::Final)
                .unwrap_or_default(),
            manual = invoice_type_counter
                .remove(&InvoiceType::Manual)
                .unwrap_or_default(),
        );
    } else {
        tracing::info!(
            "Processing {usage} usage-based invoices, and {manual} manually-entered invoices.",
            usage = invoice_type_counter
                .remove(&InvoiceType::Final)
                .unwrap_or_default(),
            manual = invoice_type_counter
                .remove(&InvoiceType::Manual)
                .unwrap_or_default(),
        );
    }

    let invoice_futures: Vec<_> = invoices
        .iter()
        .map(|response| {
            let client = stripe_client.clone();
            let db_pool = db_pool.clone();

            let annotation = match response.invoice_type {
                InvoiceType::Manual => Some(format!(
                    "[manual: {} - {}]",
                    response.date_start.format("%Y-%m-%d"),
                    response.date_end.format("%Y-%m-%d")
                )),
                _ => None,
            };

            async move {
                let action = response
                    .classify(&client, cmd.recreate_finalized)
                    .await;

                match action {
                    Err(err) => {
                        let formatted = format!(
                            "Error classifying {invoice_type:?} invoice for {tenant}",
                            tenant = response.billed_prefix,
                            invoice_type = response.invoice_type
                        );
                        Err(anyhow::anyhow!("{formatted}: {err:#}"))
                    }
                    Ok(InvoiceAction::Skip { result, customer }) => {
                        tracing::debug!(
                            tenant = response.billed_prefix,
                            invoice_type = format!("{:?}", response.invoice_type),
                            subtotal = format!("${:.2}", response.subtotal as f64 / 100.0),
                            "{}",
                            result.message(cmd.dry_run)
                        );

                        if cmd.clean_up {
                            let task_res: Result<(), anyhow::Error> = async {
                                let customer = match customer {
                                    Some(c) => c,
                                    None => return Ok(()),
                                };
                                let customer_id = customer.id.to_string();

                                if let Some(invoice) =
                                    response.get_stripe_invoice(&client, &customer_id).await?
                                {
                                    if let Some(InvoiceStatus::Draft) = invoice.status {
                                        if cmd.dry_run {
                                            tracing::warn!(
                                                tenant = response.billed_prefix.to_string(),
                                                "[dry-run] Would delete stale draft invoice {}",
                                                invoice.id
                                            );
                                        } else {
                                            tracing::warn!(
                                                tenant = response.billed_prefix.to_string(),
                                                "Deleting draft invoice!"
                                            );
                                            stripe::Invoice::delete(&client, &invoice.id).await?;
                                        }
                                    }
                                }
                                Ok(())
                            }
                            .await;

                            if let Err(e) = task_res {
                                tracing::warn!("Failed to check for or clear potential leaked draft invoices for {}, this is probably not a problem: {e:#}", response.billed_prefix.to_owned());
                            }
                        }

                        Ok((result, response.subtotal, response.billed_prefix.to_owned(), annotation))
                    }
                    Ok(action) if cmd.dry_run => {
                        let result = match &action {
                            InvoiceAction::Create { replace: Some(id), .. } => {
                                tracing::info!(
                                    tenant = response.billed_prefix,
                                    "[dry-run] Would delete existing invoice {} and recreate",
                                    id
                                );
                                InvoiceResult::Created(response.payment_provider)
                            }
                            InvoiceAction::Create { .. } => {
                                InvoiceResult::Created(response.payment_provider)
                            }
                            InvoiceAction::Update { .. } => InvoiceResult::Updated,
                            InvoiceAction::Skip { .. } => unreachable!(),
                        };
                        tracing::debug!(
                            tenant = response.billed_prefix,
                            invoice_type = format!("{:?}", response.invoice_type),
                            subtotal = format!("${:.2}", response.subtotal as f64 / 100.0),
                            "[dry-run] {}",
                            result.message(cmd.dry_run)
                        );
                        Ok((result, response.subtotal, response.billed_prefix.to_owned(), annotation))
                    }
                    Ok(action) => {
                        let res = response
                            .execute(&client, &db_pool, action, cmd.charge_type)
                            .await;
                        match res {
                            Err(err) => {
                                let formatted = format!(
                                    "Error publishing {invoice_type:?} invoice for {tenant}",
                                    tenant = response.billed_prefix,
                                    invoice_type = response.invoice_type
                                );
                                Err(anyhow::anyhow!("{formatted}: {err:#}"))
                            }
                            Ok(res) => {
                                tracing::debug!(
                                    tenant = response.billed_prefix,
                                    invoice_type = format!("{:?}", response.invoice_type),
                                    subtotal = format!("${:.2}", response.subtotal as f64 / 100.0),
                                    "{}",
                                    res.message(cmd.dry_run)
                                );
                                Ok((res, response.subtotal, response.billed_prefix.to_owned(), annotation))
                            }
                        }
                    }
                }
            }
            .boxed()
            .map_err(|e| (e, response.clone()))
        })
        .collect();

    let total = invoice_futures.len();

    let collected: HashMap<InvoiceResult, (i64, i32, Vec<(String, i64, Option<String>)>)> =
        futures::stream::iter(invoice_futures)
            .buffer_unordered(cmd.concurrency)
            .or_else(|(err, invoice)| async move {
                if !cmd.fail_fast {
                    tracing::error!("[{}]: {err:#}", invoice.billed_prefix);
                    Ok((InvoiceResult::Error, 0, invoice.billed_prefix, None))
                } else {
                    Err(err)
                }
            })
            .try_fold(
                HashMap::new(),
                |mut map, (res, subtotal, tenant, annotation)| async move {
                    let overall_count = map.values().map(|(_, count, _)| *count).sum::<i32>() + 1;
                    let msg = res.message(cmd.dry_run);

                    let (subtotal_sum, count_for_result_type, tenants) =
                        map.entry(res).or_insert((0, 0, vec![]));
                    *subtotal_sum += subtotal;
                    *count_for_result_type += 1;

                    tracing::info!("[{overall_count}/{total}, {tenant}]: {msg}");
                    tenants.push((tenant, subtotal, annotation));
                    Ok(map)
                },
            )
            .await?;

    for (status, (subtotal_agg, count, tenants)) in collected.iter() {
        tracing::info!(
            "[{:4} invoices]: {:70}${:.2}",
            count,
            status.message(cmd.dry_run),
            *subtotal_agg as f64 / 100.0
        );
        let limit = match status {
            InvoiceResult::Created(_) | InvoiceResult::Updated => 9999,
            InvoiceResult::NoDataMoved
            | InvoiceResult::NoFullPipeline
            | InvoiceResult::LessThanMinimum
            | InvoiceResult::FreeTier
            | InvoiceResult::AlreadyProcessed => 0,
            _ => 10,
        };
        let sorted_tenants = tenants
            .iter()
            .sorted_by(|(_, a, _), (_, b, _)| b.cmp(a))
            .collect_vec();

        let (displayed_tenants, remainder_tenants) =
            sorted_tenants.split_at(limit.min(tenants.len()));
        for (tenant, subtotal, annotation) in displayed_tenants {
            match annotation {
                Some(note) => {
                    tracing::info!(" - {} ${:.2} {}", tenant, *subtotal as f64 / 100.0, note)
                }
                None => tracing::info!(" - {} ${:.2}", tenant, *subtotal as f64 / 100.0),
            }
        }
        if limit > 0 && remainder_tenants.len() > 0 {
            tracing::info!(" - ... {} Others", remainder_tenants.len(),);
        }
    }

    Ok(())
}

/// Read-only: search Stripe for an existing customer by tenant metadata.
#[tracing::instrument(skip(client))]
async fn find_customer(
    client: &stripe::Client,
    tenant: &str,
) -> anyhow::Result<Option<stripe::Customer>> {
    let customers = stripe_search::<stripe::Customer>(
        client,
        "customers",
        SearchParams {
            query: format!("metadata[\"{TENANT_METADATA_KEY}\"]:\"{tenant}\""),
            ..Default::default()
        },
    )
    .await
    .context(format!("Searching for tenant {tenant}"))?;

    if let Some(customer) = customers.into_iter().next() {
        tracing::debug!("Found existing customer {id}", id = customer.id.to_string());
        Ok(Some(customer))
    } else {
        Ok(None)
    }
}

/// Ensures a Stripe customer exists for this tenant and is ready for invoicing.
/// Finds an existing customer or creates a new one, then ensures the customer
/// has an email set (looking up the earliest admin on the tenant if needed).
#[tracing::instrument(skip(client, db_client))]
async fn ensure_customer_for_invoicing(
    client: &stripe::Client,
    db_client: &Pool<Postgres>,
    tenant: &str,
) -> anyhow::Result<stripe::Customer> {
    let customer = if let Some(customer) = find_customer(client, tenant).await? {
        customer
    } else {
        tracing::debug!("Creating new customer");
        let description = format!("Represents the billing entity for Flow tenant '{tenant}'");
        stripe::Customer::create(
            client,
            stripe::CreateCustomer {
                name: Some(tenant),
                description: Some(description.as_str()),
                metadata: Some(HashMap::from([
                    (TENANT_METADATA_KEY.to_string(), tenant.to_string()),
                    (
                        CREATED_BY_BILLING_AUTOMATION.to_string(),
                        "true".to_string(),
                    ),
                ])),
                ..Default::default()
            },
        )
        .await?
    };

    if customer.email.is_none() {
        let responses = sqlx::query!(
            r#"
                select users.email as email
                from user_grants
                join auth.users as users on user_grants.user_id = users.id
                where users.email is not null and user_grants.object_role = $1
                and user_grants.capability = 'admin'
                order by users.created_at asc
            "#,
            tenant
        )
        .fetch_all(db_client)
        .await?;

        if let Some(email) = responses
            .iter()
            .find_map(|response| response.email.to_owned())
        {
            tracing::warn!(
                "Stripe customer object is missing an email. Going with {email}, an admin on that tenant."
            );
            stripe::Customer::update(
                client,
                &customer.id,
                stripe::UpdateCustomer {
                    email: Some(&email),
                    ..Default::default()
                },
            )
            .await?;
        } else {
            bail!(
                "Stripe customer object is missing an email. No admins found for tenant {tenant}, unable to create invoice without email. Found users: {found:?} Skipping",
                found = responses,
                tenant = tenant
            );
        }
    }
    Ok(customer)
}
