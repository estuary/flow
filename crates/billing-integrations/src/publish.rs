use anyhow::{Context, bail};
use billing_types::{
    BillingProvider, InvoiceMetadata, InvoiceType, PaymentProvider, StripeBillingProvider,
    record_customer_id,
};
use chrono::{Duration, Utc};
use futures::{FutureExt, StreamExt, TryFutureExt, TryStreamExt};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use sqlx::Postgres;
use sqlx::{Pool, types::chrono::NaiveDate};
use std::collections::HashMap;

const CREATED_BY_BILLING_AUTOMATION: &str = "estuary.dev/created_by_automation";

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
    /// The month to generate invoices for: YYYY-MM (also accepts YYYY-MM-01)
    #[clap(long, value_parser = crate::parse_month)]
    month: NaiveDate,
    /// Recreate existing usage invoices: delete drafts or void open invoices.
    /// Manual invoices are excluded; paid and uncollectible invoices are not replaced.
    #[clap(long, alias = "recreate-finalized")]
    recreate_existing: bool,
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
    /// Clean up recorded draft invoices of skipped bills
    #[clap(long, default_value_t = false)]
    pub clean_up: bool,
    /// Run in read-only mode: classify all invoices and report what would
    /// happen, without creating or modifying anything in Stripe.
    ///
    /// The preview checks that each invoice could be issued (a billing email is
    /// resolvable), but it cannot reconcile the final invoice total against
    /// Stripe, since that requires actually creating the invoice and its line
    /// items.
    #[clap(long, default_value_t = false)]
    pub dry_run: bool,
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
    Replaced,
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
            InvoiceResult::Replaced => {
                if dry_run {
                    "Would void/delete and replace existing invoice".to_string()
                } else {
                    "Replaced existing invoice".to_string()
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
            InvoiceResult::Error => {
                if dry_run {
                    "Would fail to publish invoices".to_string()
                } else {
                    "Error publishing invoices".to_string()
                }
            }
        }
    }
}

/// The outcome of the classify phase: what action should be taken for this invoice.
enum InvoiceAction {
    /// Invoice should not be created. Carries the skip reason.
    Skip { result: InvoiceResult },
    /// Create a new invoice. `replace` is set when --recreate-existing
    /// requires deleting an existing invoice first. `customer` is the customer
    /// found during classification, or None when none exists yet (execute then
    /// creates one).
    Create {
        replace: Option<stripe::InvoiceId>,
        customer: Option<stripe::Customer>,
    },
    /// Update an existing draft invoice's line items. `customer` is the tenant's
    /// recorded customer, already found during classification.
    Update {
        existing_invoice_id: stripe::InvoiceId,
        customer: stripe::Customer,
    },
}

impl InvoiceAction {
    /// The customer located during classification, if one is recorded.
    fn customer(&self) -> Option<&stripe::Customer> {
        match self {
            InvoiceAction::Skip { .. } => None,
            InvoiceAction::Create { customer, .. } => customer.as_ref(),
            InvoiceAction::Update { customer, .. } => Some(customer),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, sqlx::FromRow)]
pub(crate) struct Invoice {
    subtotal: i64,
    line_items: sqlx::types::Json<Vec<LineItem>>,
    date_start: NaiveDate,
    date_end: NaiveDate,
    billed_prefix: String,
    invoice_type: InvoiceType,
    extra: Option<sqlx::types::Json<Option<Extra>>>,
    has_full_pipeline: bool,
    payment_provider: PaymentProvider,
    tenant_trial_start: Option<NaiveDate>,
    pub(crate) stripe_invoice_id: Option<String>,
    stripe_customer_id: Option<String>,
}

impl Invoice {
    /// Retrieve this bill's recorded Stripe invoice. `None` if no invoice is
    /// recorded, or if Stripe no longer has it.
    async fn locate_invoice(
        &self,
        provider: &dyn BillingProvider,
    ) -> anyhow::Result<Option<stripe::Invoice>> {
        let Some(recorded) = &self.stripe_invoice_id else {
            return Ok(None);
        };
        let invoice_id: stripe::InvoiceId = recorded
            .parse()
            .with_context(|| format!("invalid recorded Stripe invoice ID {recorded}"))?;
        provider.retrieve_invoice(&invoice_id).await
    }

    /// Record `invoice_id` as this bill's Stripe invoice, or clear it if `None`. The bill's recorded invoice must be unchanged since the bill was loaded, and a manual bill's dates must be unedited. Otherwise this is an error, so a created invoice is never silently left unrecorded.
    async fn record_invoice_id(
        &self,
        db_client: &Pool<Postgres>,
        invoice_id: Option<&stripe::InvoiceId>,
    ) -> anyhow::Result<()> {
        let expected = self.stripe_invoice_id.as_deref();
        let invoice_id = invoice_id.map(stripe::InvoiceId::as_str);
        let updated = match self.invoice_type {
            InvoiceType::Final => sqlx::query!(
                r#"
                update internal.billing_historicals
                set stripe_invoice_id = $3
                where tenant = $1
                  and date_trunc('month', billed_month)::date = $2
                  and stripe_invoice_id is not distinct from $4
                "#,
                self.billed_prefix,
                self.date_start,
                invoice_id,
                expected,
            )
            .execute(db_client)
            .await?
            .rows_affected(),
            InvoiceType::Manual => sqlx::query!(
                r#"
                update internal.manual_bills
                set stripe_invoice_id = $4
                where tenant = $1
                  and date_start = $2
                  and date_end = $3
                  and stripe_invoice_id is not distinct from $5
                "#,
                self.billed_prefix,
                self.date_start,
                self.date_end,
                invoice_id,
                expected,
            )
            .execute(db_client)
            .await?
            .rows_affected(),
            InvoiceType::Preview => bail!("preview bills have no Stripe invoice"),
        };

        match (updated, invoice_id) {
            (1, _) => Ok(()),
            (_, Some(invoice_id)) => bail!(
                "The bill or its recorded invoice changed during this run, so Stripe invoice {invoice_id} isn't recorded for it"
            ),
            (_, None) => bail!(
                "The bill or its recorded invoice changed during this run, so its recorded invoice isn't cleared"
            ),
        }
    }

    pub(crate) fn metadata(&self) -> InvoiceMetadata {
        InvoiceMetadata {
            tenant: self.billed_prefix.to_owned(),
            invoice_type: self.invoice_type,
            period_start: self.date_start.format("%F").to_string(),
            period_end: self.date_end.format("%F").to_string(),
        }
    }

    /// Read-only classification: determines what action should be taken for this
    /// invoice without making any writes to Stripe.
    #[tracing::instrument(skip(self, provider), fields(tenant=self.billed_prefix, invoice_type=format!("{:?}",self.invoice_type), subtotal=format!("${:.2}", self.subtotal as f64 / 100.0)))]
    async fn classify(
        &self,
        provider: &dyn BillingProvider,
        recreate_existing: bool,
    ) -> anyhow::Result<InvoiceAction> {
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
                    });
                }
                None => {
                    return Ok(InvoiceAction::Skip {
                        result: InvoiceResult::FreeTier,
                    });
                }
                _ => {}
            }
        }

        if self.subtotal < 50 {
            return Ok(InvoiceAction::Skip {
                result: InvoiceResult::LessThanMinimum,
            });
        }

        // Reuse this lookup for payment-method checks and execution.
        let customer = match self.stripe_customer_id.as_deref() {
            Some(recorded) => {
                let id: stripe::CustomerId = recorded
                    .parse()
                    .with_context(|| format!("invalid recorded Stripe customer ID {recorded}"))?;
                let customer = provider
                    .retrieve_customer(&id)
                    .await
                    .with_context(|| format!("Retrieving Stripe customer {id}"))?;
                if customer.deleted {
                    bail!(
                        "Stripe customer {id} of tenant {tenant} is deleted; clear tenants.stripe_customer_id to invoice a new customer",
                        tenant = self.billed_prefix
                    );
                }
                Some(customer)
            }
            None => None,
        };

        if let (InvoiceType::Final, Some(extra)) = (&self.invoice_type, &self.extra) {
            // If the outer Option is Some the inner should be too; treat a
            // null inner as a data error rather than panicking and aborting
            // the whole run.
            let Some(unwrapped_extra) = extra.0.as_ref() else {
                bail!(
                    "Final invoice for {tenant} has a null `extra` payload",
                    tenant = self.billed_prefix
                );
            };

            let has_payment_method = customer
                .as_ref()
                .and_then(|c| c.invoice_settings.as_ref())
                .and_then(|i| i.default_payment_method.as_ref())
                .is_some();

            if !has_payment_method {
                if unwrapped_extra.processed_data_gb.unwrap_or_default() == 0.0 {
                    return Ok(InvoiceAction::Skip {
                        result: InvoiceResult::NoDataMoved,
                    });
                }

                if !self.has_full_pipeline {
                    return Ok(InvoiceAction::Skip {
                        result: InvoiceResult::NoFullPipeline,
                    });
                }
            }
        }

        let Some(invoice) = self.locate_invoice(provider).await? else {
            if let Some(invoice_id) = &self.stripe_invoice_id {
                // The bill was invoiced, but its draft has since been deleted in
                // Stripe. Like a voided invoice, that's taken as deliberate: it's
                // only replaced under --recreate-existing, and never for a manual bill.
                if matches!(self.invoice_type, InvoiceType::Manual) {
                    tracing::warn!(
                        "Recorded manual invoice {invoice_id} no longer exists in Stripe, skipping"
                    );
                    return Ok(InvoiceAction::Skip {
                        result: InvoiceResult::AlreadyProcessed,
                    });
                }
                if !recreate_existing {
                    bail!(
                        "Recorded invoice {invoice_id} no longer exists in Stripe. Pass --recreate-existing to create a replacement."
                    );
                }
                tracing::warn!("Recorded invoice {invoice_id} no longer exists; replacing it");
            }
            return Ok(InvoiceAction::Create {
                replace: None,
                customer,
            });
        };

        // A tenant can have a recorded invoice without a recorded customer. Updating or replacing the invoice needs the customer.
        let Some(customer) = customer else {
            bail!(
                "Found invoice {id}, but tenant {tenant} has no Stripe customer",
                id = invoice.id,
                tenant = self.billed_prefix
            );
        };
        match invoice.status {
            // Manual invoices are excluded from --recreate-existing: an
            // already-sent (open) manual invoice must not be voided and
            // reissued, and a manual draft is refreshed via the Update arm below.
            Some(stripe::InvoiceStatus::Open | stripe::InvoiceStatus::Draft)
                if recreate_existing && !matches!(self.invoice_type, InvoiceType::Manual) =>
            {
                Ok(InvoiceAction::Create {
                    replace: Some(invoice.id),
                    customer: Some(customer),
                })
            }
            // A voided invoice can be neither deleted nor updated, so treat
            // it as absent and create a fresh replacement. This also
            // recovers a --recreate-existing run that voided an open
            // invoice but failed before creating its replacement. Without
            // the flag, the unsupported-state error below keeps a voided
            // invoice loud rather than silently re-billing the tenant.
            Some(stripe::InvoiceStatus::Void)
                if recreate_existing && !matches!(self.invoice_type, InvoiceType::Manual) =>
            {
                tracing::warn!(
                    "Found voided invoice {id}; treating it as absent",
                    id = invoice.id.to_string()
                );
                Ok(InvoiceAction::Create {
                    replace: None,
                    customer: Some(customer),
                })
            }
            Some(stripe::InvoiceStatus::Draft) => {
                tracing::debug!(
                    "Found existing draft invoice {id}",
                    id = invoice.id.to_string()
                );
                Ok(InvoiceAction::Update {
                    existing_invoice_id: invoice.id,
                    customer,
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
                })
            }
            Some(stripe::InvoiceStatus::Open) => {
                bail!(
                    "Found open invoice {id}. Pass --recreate-existing to void and recreate this invoice.",
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
    }

    /// Execute the classified action: performs all Stripe writes (customer creation,
    /// invoice creation/update, line item management, verification).
    #[tracing::instrument(skip(self, client, db_client, action), fields(tenant=self.billed_prefix, invoice_type=format!("{:?}",self.invoice_type), subtotal=format!("${:.2}", self.subtotal as f64 / 100.0)))]
    async fn execute(
        &self,
        provider: &dyn BillingProvider,
        client: &stripe::Client,
        db_client: &Pool<Postgres>,
        action: InvoiceAction,
        mode: ChargeType,
    ) -> anyhow::Result<InvoiceResult> {
        let (replace, existing_invoice_id, found_customer) = match action {
            InvoiceAction::Skip { .. } => {
                unreachable!("Skip actions are resolved by the caller and never executed")
            }
            InvoiceAction::Create { replace, customer } => (replace, None, customer),
            InvoiceAction::Update {
                existing_invoice_id,
                customer,
            } => (None, Some(existing_invoice_id), Some(customer)),
        };
        let is_update = existing_invoice_id.is_some();

        let customer =
            ensure_customer_for_invoicing(provider, db_client, &self.billed_prefix, found_customer)
                .await?;

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
        let description = format!(
            "Your Flow bill for the billing period between {date_start_human} - {date_end_human}. Tenant: {tenant}",
            tenant = self.billed_prefix
        );
        let custom_fields = vec![
            stripe::CreateInvoiceCustomFields {
                name: "Billing Period Start".to_string(),
                value: date_start_human,
            },
            stripe::CreateInvoiceCustomFields {
                name: "Billing Period End".to_string(),
                value: date_end_human,
            },
        ];
        let metadata = self.metadata().to_metadata_map();

        // Remove the existing invoice if --recreate-existing was used
        if let Some(ref replace_id) = replace {
            // Re-verify the invoice status before removing it (guard against race conditions)
            let existing = stripe::Invoice::retrieve(client, replace_id, &[]).await?;
            match existing.status {
                Some(stripe::InvoiceStatus::Draft) => {
                    tracing::warn!(
                        "Found draft invoice {id}, deleting and recreating",
                        id = replace_id.to_string(),
                    );
                    stripe::Invoice::delete(client, replace_id).await?;
                }
                // Stripe only permits deleting drafts; a finalized invoice
                // must be voided instead.
                Some(stripe::InvoiceStatus::Open) => {
                    tracing::warn!(
                        "Found open invoice {id}, voiding and recreating",
                        id = replace_id.to_string(),
                    );
                    stripe::Invoice::void(client, replace_id).await?;
                }
                Some(status) => {
                    bail!(
                        "Invoice {id} changed to state {status} since classification, cannot replace.",
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

        // Manual invoices should always be sent as invoices rather than
        // charged to the customer's payment method.
        let mode = if self.invoice_type == InvoiceType::Manual {
            ChargeType::SendInvoice
        } else {
            mode
        };
        let collection_method = match mode {
            ChargeType::AutoCharge => stripe::CollectionMethod::ChargeAutomatically,
            ChargeType::SendInvoice => stripe::CollectionMethod::SendInvoice,
        };
        // `send_invoice` requires a due date; `charge_automatically` must not carry one.
        let due_date = match mode {
            ChargeType::SendInvoice => Some((Utc::now() + Duration::days(30)).timestamp()),
            ChargeType::AutoCharge => None,
        };

        let invoice = if let Some(existing_id) = existing_invoice_id {
            tracing::debug!(
                "Updating existing invoice {id}",
                id = existing_id.to_string()
            );
            let existing = stripe::Invoice::retrieve(client, &existing_id, &[]).await?;

            // Re-verify the invoice is still an updatable draft. It was a draft at
            // classification time, but a concurrent run or a human finalizing /
            // paying / voiding it in Stripe could have changed that; mutating a
            // finalized invoice's line items would otherwise fail with an opaque
            // Stripe error. Mirrors the guard on the delete/recreate path above.
            match existing.status {
                Some(stripe::InvoiceStatus::Draft) => {}
                Some(status) => {
                    bail!(
                        "Invoice {id} changed to state {status} since classification, cannot update.",
                        id = existing_id.to_string(),
                        status = status
                    );
                }
                None => {
                    bail!(
                        "Unexpected missing status from invoice {id}",
                        id = existing_id.to_string()
                    );
                }
            }

            // A manual bill's dates can be edited after its invoice is drafted,
            // so every update refreshes its description, custom fields, and metadata.
            #[derive(serde::Serialize)]
            struct UpdateDraft<'a> {
                #[serde(skip_serializing_if = "Option::is_none")]
                collection_method: Option<stripe::CollectionMethod>,
                #[serde(skip_serializing_if = "Option::is_none")]
                due_date: Option<i64>,
                description: &'a str,
                custom_fields: &'a [stripe::CreateInvoiceCustomFields],
                metadata: &'a HashMap<String, String>,
            }
            let mut update = UpdateDraft {
                collection_method: None,
                due_date: None,
                description: &description,
                custom_fields: &custom_fields,
                metadata: &metadata,
            };

            // The create path sets the collection method from `mode`, but the update
            // path reuses whatever the invoice was originally created with. Reconcile
            // toward `send_invoice` only, so a manual invoice previously stored as
            // `charge_automatically` stops silently auto-charging the card. The
            // reverse direction is deliberately not applied: it would undo the send
            // workflow's switch to `send_invoice` for tenants without a payment
            // method on file.
            if collection_method == stripe::CollectionMethod::SendInvoice
                && existing.collection_method != Some(collection_method)
            {
                tracing::debug!(
                    "Reconciling collection method of invoice {id} to {collection_method:?}",
                    id = existing_id.to_string()
                );
                update.collection_method = Some(collection_method);
                update.due_date = Some(due_date.expect("send_invoice mode always sets a due date"));
            }

            client
                .post_form(&format!("/invoices/{existing_id}"), update)
                .await
                .context("Updating existing draft invoice")?
        } else {
            let invoice = stripe::Invoice::create(
                client,
                stripe::CreateInvoice {
                    customer: Some(customer.id.to_owned()),
                    due_date,
                    description: Some(&description),
                    collection_method: Some(collection_method),
                    auto_advance: Some(false),
                    custom_fields: Some(custom_fields),
                    metadata: Some(metadata),
                    ..Default::default()
                },
            )
            .await
            .context("Creating a new invoice")?;
            tracing::debug!("Created a new invoice {id}", id = invoice.id);

            // Record the invoice before populating it, so a failure below leaves
            // a recorded draft that the next run updates rather than duplicates.
            self.record_invoice_id(db_client, Some(&invoice.id))
                .await
                .with_context(|| format!("Recording created Stripe invoice {}", invoice.id))?;
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
        } else if replace.is_some() {
            Ok(InvoiceResult::Replaced)
        } else {
            Ok(InvoiceResult::Created(self.payment_provider))
        }
    }
}

pub(crate) async fn load_invoices(
    pool: &sqlx::PgPool,
    month: NaiveDate,
    tenants: &[String],
    exclude_tenants: &[String],
) -> anyhow::Result<Vec<Invoice>> {
    Ok(sqlx::query_as::<_, Invoice>(
        r#"
        select
            date_start,
            date_end,
            billed_prefix,
            invoice_type,
            line_items,
            subtotal::bigint as subtotal,
            extra,
            coalesce(dataflow.has_full_pipeline, false) as has_full_pipeline,
            tenants.payment_provider,
            tenants.trial_start as tenant_trial_start,
            invoices_ext.stripe_invoice_id,
            tenants.stripe_customer_id
        from internal.invoices_ext
        join tenants on tenants.tenant = billed_prefix
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
        and (cardinality($2::text[]) = 0 or billed_prefix = any($2))
        and not (billed_prefix = any($3))
        "#,
    )
    .bind(month)
    .bind(tenants)
    .bind(exclude_tenants)
    .fetch_all(pool)
    .await?)
}

pub async fn do_publish_invoices(cmd: &PublishInvoice) -> anyhow::Result<()> {
    let stripe_client = stripe::Client::new(cmd.stripe_api_key.to_owned())
        .with_strategy(stripe::RequestStrategy::ExponentialBackoff(4));
    let billing_provider = StripeBillingProvider::from_client(stripe_client.clone());
    let db_pool = crate::connect_db(&cmd.connection_string, 5).await?;

    let month_human_repr = cmd.month.format("%B %Y");

    tracing::info!("Fetching billing data for {month_human_repr}");

    let invoices = load_invoices(&db_pool, cmd.month, &cmd.tenants, &[]).await?;

    let mut invoice_type_counter: HashMap<InvoiceType, usize> = HashMap::new();
    invoices.iter().for_each(|invoice| {
        *invoice_type_counter
            .entry(invoice.invoice_type.clone())
            .or_default() += 1;
    });

    let usage = invoice_type_counter
        .remove(&InvoiceType::Final)
        .unwrap_or_default();
    let manual = invoice_type_counter
        .remove(&InvoiceType::Manual)
        .unwrap_or_default();
    if cmd.dry_run {
        tracing::info!(
            "[dry-run] Classifying {usage} usage-based invoices and {manual} manually-entered invoices without making any changes to Stripe."
        );
    } else {
        tracing::info!(
            "Processing {usage} usage-based invoices, and {manual} manually-entered invoices."
        );
    }

    let invoice_futures: Vec<_> = invoices
        .iter()
        .map(|response| {
            let client = stripe_client.clone();
            let provider = billing_provider.clone();
            let db_pool = db_pool.clone();

            async move {
                let result = process_invoice(cmd, &provider, &client, &db_pool, response).await?;
                anyhow::Ok((
                    result,
                    response.subtotal,
                    response.billed_prefix.to_owned(),
                    manual_annotation(response),
                ))
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
                    let annotation = manual_annotation(&invoice);
                    Ok((InvoiceResult::Error, 0, invoice.billed_prefix, annotation))
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
            InvoiceResult::Created(_) | InvoiceResult::Updated | InvoiceResult::Replaced => 9999,
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

/// Process a single invoice end-to-end: classify it, then execute the action
/// (or preview it under --dry-run), returning the summary bucket for the run
/// report. Skips optionally clean up a stale draft left behind in Stripe.
async fn process_invoice(
    cmd: &PublishInvoice,
    provider: &dyn BillingProvider,
    client: &stripe::Client,
    db_pool: &Pool<Postgres>,
    invoice: &Invoice,
) -> anyhow::Result<InvoiceResult> {
    let action = invoice
        .classify(provider, cmd.recreate_existing)
        .await
        .with_context(|| {
            format!(
                "Error classifying {invoice_type:?} invoice for {tenant}",
                tenant = invoice.billed_prefix,
                invoice_type = invoice.invoice_type
            )
        })?;

    // A real run resolves a billing email inside execute(), which bails when
    // none can be found. Mirror that read-only so the preview reports such
    // tenants as errors instead of claiming the invoice would be published.
    // (The invoice total cannot be reconciled without creating the invoice in
    // Stripe, so that check is necessarily skipped.)
    if cmd.dry_run && !matches!(action, InvoiceAction::Skip { .. }) {
        if !billing_email_available(db_pool, action.customer(), &invoice.billed_prefix).await? {
            let reason = "no customer email, tenants.billing_emails, or admin user to invoice";
            tracing::warn!(
                tenant = invoice.billed_prefix,
                "[dry-run] Would fail: {reason}"
            );
            if cmd.fail_fast {
                bail!(
                    "[dry-run] Would fail to publish invoice for {tenant}: {reason}",
                    tenant = invoice.billed_prefix
                );
            }
            return Ok(InvoiceResult::Error);
        }
    }

    let result = match (action, cmd.dry_run) {
        (InvoiceAction::Skip { result, .. }, _) => {
            if cmd.clean_up {
                clean_up_stale_draft(provider, client, db_pool, invoice, cmd.dry_run).await;
            }
            result
        }
        (
            InvoiceAction::Create {
                replace: Some(id), ..
            },
            true,
        ) => {
            tracing::info!(
                tenant = invoice.billed_prefix,
                "[dry-run] Would replace existing invoice {}",
                id
            );
            InvoiceResult::Replaced
        }
        (InvoiceAction::Create { .. }, true) => InvoiceResult::Created(invoice.payment_provider),
        (InvoiceAction::Update { .. }, true) => InvoiceResult::Updated,
        (action, false) => invoice
            .execute(provider, client, db_pool, action, cmd.charge_type)
            .await
            .with_context(|| {
                format!(
                    "Error publishing {invoice_type:?} invoice for {tenant}",
                    tenant = invoice.billed_prefix,
                    invoice_type = invoice.invoice_type
                )
            })?,
    };

    tracing::debug!(
        tenant = invoice.billed_prefix,
        invoice_type = format!("{:?}", invoice.invoice_type),
        subtotal = format!("${:.2}", invoice.subtotal as f64 / 100.0),
        "{}",
        result.message(cmd.dry_run)
    );
    Ok(result)
}

/// Best-effort removal of a stale draft invoice for a bill that was skipped:
/// checks Stripe for the bill's draft and deletes it (or reports it under
/// --dry-run). Failures are logged rather than failing the run.
async fn clean_up_stale_draft(
    provider: &dyn BillingProvider,
    client: &stripe::Client,
    db_client: &Pool<Postgres>,
    invoice: &Invoice,
    dry_run: bool,
) {
    let task_res: anyhow::Result<()> = async {
        let Some(stripe_invoice) = invoice.locate_invoice(provider).await? else {
            return Ok(());
        };
        if stripe_invoice.status != Some(stripe::InvoiceStatus::Draft) {
            return Ok(());
        }
        if dry_run {
            tracing::warn!(
                tenant = invoice.billed_prefix.to_string(),
                "[dry-run] Would delete stale draft invoice {}",
                stripe_invoice.id
            );
            return Ok(());
        }

        tracing::warn!(
            tenant = invoice.billed_prefix,
            "Deleting stale draft invoice"
        );
        stripe::Invoice::delete(client, &stripe_invoice.id).await?;
        invoice.record_invoice_id(db_client, None).await?;
        Ok(())
    }
    .await;

    if let Err(e) = task_res {
        tracing::warn!(
            "Failed to delete or clear stale draft invoice for {}; inspect its recorded ID and recover it if needed: {e:#}",
            invoice.billed_prefix
        );
    }
}

/// Display tag distinguishing a manually-entered invoice (and its period) in the
/// run summary; usage invoices get no tag.
fn manual_annotation(invoice: &Invoice) -> Option<String> {
    match invoice.invoice_type {
        InvoiceType::Manual => Some(format!(
            "[manual: {} - {}]",
            invoice.date_start.format("%Y-%m-%d"),
            invoice.date_end.format("%Y-%m-%d")
        )),
        _ => None,
    }
}

/// Ensures a Stripe customer exists for this tenant and is ready for invoicing.
/// Uses `found` (the customer located during classification) when present,
/// otherwise creates and records a new one (carrying the
/// tenant's billing email and address from the control-plane DB), then ensures
/// the customer has an email set (falling back to the earliest admin on the
/// tenant if needed).
#[tracing::instrument(skip(provider, db_client, found))]
async fn ensure_customer_for_invoicing(
    provider: &dyn BillingProvider,
    db_client: &Pool<Postgres>,
    tenant: &str,
    found: Option<stripe::Customer>,
) -> anyhow::Result<stripe::Customer> {
    let customer = if let Some(customer) = found {
        customer
    } else {
        tracing::debug!("Creating new customer");
        let billing_row = tenant_billing_row(db_client, tenant).await?;
        let billing_email = billing_row
            .as_ref()
            .and_then(|r| r.billing_email.as_deref());
        let billing_address: Option<stripe::Address> = billing_row
            .as_ref()
            .and_then(|r| r.billing_address.as_ref())
            .map(|v| serde_json::from_value::<stripe::Address>(v.clone()))
            .transpose()
            .context("deserializing billing_address")?;
        let metadata = HashMap::from([(
            CREATED_BY_BILLING_AUTOMATION.to_string(),
            "true".to_string(),
        )]);
        let new_customer = provider
            .create_customer(tenant, billing_email, billing_address, metadata)
            .await?;

        record_customer_id(db_client, tenant, &new_customer.id)
            .await
            .context("Recording created customer")?;

        // No controller wake here: the customer is created with the DB's
        // billing email and address, and the billing name (customer metadata)
        // is reconciled by the tenant controller when the contact is edited.
        // Waking it during an invoicing run would add a Stripe lookup per new
        // customer for no contact change.
        new_customer
    };

    if customer.email.is_none() {
        let db_email = tenant_billing_row(db_client, tenant)
            .await?
            .and_then(|r| r.billing_email);

        let email = match db_email {
            Some(email) => {
                tracing::info!("Using billing_emails from tenants table: {email}");
                email
            }
            None => match earliest_admin_email(db_client, tenant).await? {
                Some(email) => {
                    tracing::warn!(
                        "Stripe customer object is missing an email. Going with {email}, an admin on that tenant."
                    );
                    email
                }
                None => bail!(
                    "Stripe customer object is missing an email. No admins found for tenant {tenant}, unable to create invoice without email. Skipping"
                ),
            },
        };

        provider
            .update_customer_billing_profile(&customer.id, Some(&email), None, None)
            .await?;
    }
    Ok(customer)
}

/// A tenant's billing contact info from the control-plane DB.
struct TenantBillingRow {
    billing_email: Option<String>,
    billing_address: Option<serde_json::Value>,
}

/// Read-only: the tenant's billing contact row, if the tenant exists.
async fn tenant_billing_row(
    db_client: &Pool<Postgres>,
    tenant: &str,
) -> anyhow::Result<Option<TenantBillingRow>> {
    Ok(sqlx::query_as!(
        TenantBillingRow,
        r#"SELECT billing_emails[1] AS billing_email, billing_address FROM tenants WHERE tenant = $1"#,
        tenant,
    )
    .fetch_optional(db_client)
    .await?)
}

/// Read-only: the email of the earliest-created admin user on the tenant, if any.
async fn earliest_admin_email(
    db_client: &Pool<Postgres>,
    tenant: &str,
) -> anyhow::Result<Option<String>> {
    // NOTE: the SQL text below is intentionally indented to stay byte-identical
    // to the query this was extracted from, so it keeps hitting the same
    // offline sqlx cache entry.
    let responses = sqlx::query!(
        r#"
                    select users.email as email
                    from user_grants
                    join auth.users as users on user_grants.user_id = users.id
                    where users.email is not null and user_grants.object_role = $1
                    and user_grants.capability = 'admin'
                    -- Exclude service accounts: their synthetic addresses must
                    -- never be chosen as a tenant's Stripe billing contact.
                    and not exists (
                      select 1 from internal.service_accounts sa where sa.user_id = users.id
                    )
                    order by users.created_at asc
                "#,
        tenant
    )
    .fetch_all(db_client)
    .await?;

    Ok(responses.into_iter().find_map(|r| r.email))
}

/// Read-only mirror of the email requirement enforced by
/// `ensure_customer_for_invoicing`: the classified customer's email, else
/// the first of `tenants.billing_emails`, else the earliest tenant admin. Returns whether an
/// email could be resolved without performing any writes, so --dry-run can
/// report tenants that would fail to invoice rather than over-reporting success.
async fn billing_email_available(
    db_client: &Pool<Postgres>,
    customer: Option<&stripe::Customer>,
    tenant: &str,
) -> anyhow::Result<bool> {
    if customer.is_some_and(|c| c.email.is_some()) {
        return Ok(true);
    }

    if tenant_billing_row(db_client, tenant)
        .await?
        .and_then(|r| r.billing_email)
        .is_some()
    {
        return Ok(true);
    }

    Ok(earliest_admin_email(db_client, tenant).await?.is_some())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stripe_stub::stripe_stub;
    use axum::http::StatusCode;

    const TENANT: &str = "acmeCo/";
    const CUSTOMER_ID: &str = "cus_acme";

    /// A $100 bill for `TENANT`: a manual bill spanning three months, or an
    /// August usage bill that's eligible for invoicing.
    fn bill(invoice_type: InvoiceType, recorded: Option<&str>) -> Invoice {
        let (date_start, date_end) = match invoice_type {
            InvoiceType::Manual => ("2026-07-15", "2026-09-14"),
            _ => ("2026-08-01", "2026-08-31"),
        };
        Invoice {
            subtotal: 10_000,
            line_items: sqlx::types::Json(Vec::new()),
            date_start: date_start.parse().unwrap(),
            date_end: date_end.parse().unwrap(),
            billed_prefix: TENANT.to_string(),
            invoice_type,
            extra: Some(sqlx::types::Json(Some(Extra {
                trial_start: None,
                trial_credit: None,
                recurring_fee: None,
                task_usage_hours: None,
                processed_data_gb: Some(1.0),
            }))),
            has_full_pipeline: true,
            payment_provider: PaymentProvider::Stripe,
            tenant_trial_start: Some("2020-01-01".parse().unwrap()),
            stripe_invoice_id: recorded.map(str::to_string),
            stripe_customer_id: Some(CUSTOMER_ID.to_string()),
        }
    }

    fn stripe_invoice(id: &str, status: stripe::InvoiceStatus) -> stripe::Invoice {
        stripe::Invoice {
            id: id.parse().unwrap(),
            status: Some(status),
            ..Default::default()
        }
    }

    fn customer() -> stripe::Customer {
        stripe::Customer {
            id: CUSTOMER_ID.parse().unwrap(),
            email: Some("billing@acme.test".to_string()),
            balance: Some(0),
            ..Default::default()
        }
    }

    fn ok(body: impl serde::Serialize) -> (StatusCode, serde_json::Value) {
        (StatusCode::OK, serde_json::to_value(body).unwrap())
    }

    fn not_found() -> (StatusCode, serde_json::Value) {
        (
            StatusCode::NOT_FOUND,
            serde_json::json!({"error": {
                "type": "invalid_request_error",
                "code": "resource_missing",
                "message": "No such invoice",
            }}),
        )
    }

    fn describe(action: &InvoiceAction) -> String {
        match action {
            InvoiceAction::Skip { result } => format!("skip: {}", result.message(false)),
            InvoiceAction::Create { replace, customer } => format!(
                "create: replacing {:?}; customer {}",
                replace.as_ref().map(|id| id.as_str()),
                customer.as_ref().map_or("none", |c| c.id.as_str()),
            ),
            InvoiceAction::Update {
                existing_invoice_id,
                customer,
            } => format!("update: {existing_invoice_id}; customer {}", customer.id,),
        }
    }

    #[tokio::test]
    async fn recorded_objects_are_retrieved_by_id() {
        use stripe::InvoiceStatus::{Draft, Open};

        let cases = [
            (
                bill(InvoiceType::Manual, Some("in_recorded")),
                false,
                ok(stripe_invoice("in_recorded", Draft)),
            ),
            (
                bill(InvoiceType::Manual, Some("in_recorded")),
                false,
                ok(stripe_invoice("in_recorded", Open)),
            ),
            // A recorded invoice that was deleted isn't recreated for a manual
            // bill, and is for a usage bill only under --recreate-existing.
            (
                bill(InvoiceType::Manual, Some("in_recorded")),
                false,
                not_found(),
            ),
            (
                bill(InvoiceType::Final, Some("in_recorded")),
                false,
                not_found(),
            ),
            (
                bill(InvoiceType::Final, Some("in_recorded")),
                true,
                not_found(),
            ),
            (
                bill(InvoiceType::Final, Some("in_recorded")),
                true,
                ok(stripe_invoice("in_recorded", Open)),
            ),
        ];

        let mut outcomes = Vec::new();
        for (bill, recreate_existing, invoice_response) in cases {
            let (client, requests) = stripe_stub(vec![ok(customer()), invoice_response]).await;
            let provider = StripeBillingProvider::from_client(client.clone());
            let outcome = match bill.classify(&provider, recreate_existing).await {
                Ok(action) => describe(&action),
                Err(err) => format!("error: {err:#}"),
            };
            outcomes.push(format!(
                "{:?} recreate={recreate_existing}: {outcome}",
                bill.invoice_type
            ));
            assert_eq!(
                *requests.lock().unwrap(),
                ["GET /v1/customers/cus_acme", "GET /v1/invoices/in_recorded"],
            );
        }
        insta::assert_snapshot!(outcomes.join("\n"), @r#"
        Manual recreate=false: update: in_recorded; customer cus_acme
        Manual recreate=false: skip: Skipping invoice already processed in a previous billing run
        Manual recreate=false: skip: Skipping invoice already processed in a previous billing run
        Final recreate=false: error: Recorded invoice in_recorded no longer exists in Stripe. Pass --recreate-existing to create a replacement.
        Final recreate=true: create: replacing None; customer cus_acme
        Final recreate=true: create: replacing Some("in_recorded"); customer cus_acme
        "#);
    }

    #[sqlx::test(migrations = "../../supabase/migrations")]
    async fn created_customer_is_recorded_before_missing_email_fails(pool: sqlx::PgPool) {
        sqlx::query("insert into tenants (id, tenant) values (internal.id_generator(), $1)")
            .bind(TENANT)
            .execute(&pool)
            .await
            .unwrap();

        let created = stripe::Customer {
            id: "cus_created".parse().unwrap(),
            email: None,
            ..Default::default()
        };
        let (client, requests) = stripe_stub(vec![ok(created)]).await;
        let provider = StripeBillingProvider::from_client(client);
        let err = ensure_customer_for_invoicing(&provider, &pool, TENANT, None)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("No admins found"), "{err:#}");
        assert_eq!(*requests.lock().unwrap(), ["POST /v1/customers"],);

        let recorded: Option<String> =
            sqlx::query_scalar("select stripe_customer_id from tenants where tenant = $1")
                .bind(TENANT)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(recorded.as_deref(), Some("cus_created"));
    }

    #[sqlx::test(migrations = "../../supabase/migrations")]
    async fn dry_run_execution_and_cleanup_are_read_only(pool: sqlx::PgPool) {
        sqlx::query("insert into tenants (id, tenant, billing_email, stripe_customer_id) values (internal.id_generator(), $1, $2, 'cus_acme')")
            .bind(TENANT)
            .bind("billing@acme.test")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            "insert into internal.billing_historicals (tenant, billed_month, report) values ($1, '2026-08-01', $2::jsonb)",
        )
        .bind(TENANT)
        .bind(r#"{"billed_month":"2026-08-01T00:00:00Z"}"#)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "insert into internal.manual_bills (tenant, usd_cents, description, date_start, date_end, stripe_invoice_id) values ($1, 10000, 'Update candidate', '2026-07-15', '2026-09-14', 'in_draft')",
        )
        .bind(TENANT)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "insert into internal.manual_bills (tenant, usd_cents, description, date_start, date_end, stripe_invoice_id) values ($1, 10000, 'Cleanup candidate', '2025-01-01', '2025-01-31', 'in_stale')",
        )
        .bind(TENANT)
        .execute(&pool)
        .await
        .unwrap();

        let mut cmd = PublishInvoice {
            connection_string: String::new(),
            stripe_api_key: String::new(),
            tenants: vec![TENANT.to_string()],
            all_tenants: false,
            month: "2026-08-01".parse().unwrap(),
            recreate_existing: false,
            fail_fast: false,
            charge_type: ChargeType::AutoCharge,
            concurrency: 1,
            clean_up: false,
            dry_run: true,
        };

        // Without recorded IDs, preview makes no Stripe requests.
        let mut candidate = bill(InvoiceType::Final, None);
        candidate.stripe_customer_id = None;
        let (client, requests) = stripe_stub(vec![]).await;
        let provider = StripeBillingProvider::from_client(client.clone());
        assert!(matches!(
            process_invoice(&cmd, &provider, &client, &pool, &candidate)
                .await
                .unwrap(),
            InvoiceResult::Created(PaymentProvider::Stripe)
        ));
        assert!(requests.lock().unwrap().is_empty());

        // A recorded draft is retrieved by ID and left unchanged by preview.
        let update = bill(InvoiceType::Manual, Some("in_draft"));
        let (client, requests) = stripe_stub(vec![
            ok(customer()),
            ok(stripe_invoice("in_draft", stripe::InvoiceStatus::Draft)),
        ])
        .await;
        let provider = StripeBillingProvider::from_client(client.clone());
        assert!(matches!(
            process_invoice(&cmd, &provider, &client, &pool, &update)
                .await
                .unwrap(),
            InvoiceResult::Updated
        ));
        assert_eq!(
            *requests.lock().unwrap(),
            ["GET /v1/customers/cus_acme", "GET /v1/invoices/in_draft",]
        );

        // Cleanup preview retrieves the recorded object but never deletes it.
        cmd.clean_up = true;
        let mut skipped = bill(InvoiceType::Manual, Some("in_stale"));
        skipped.date_start = "2025-01-01".parse().unwrap();
        skipped.date_end = "2025-01-31".parse().unwrap();
        let (client, requests) = stripe_stub(vec![
            ok(customer()),
            ok(stripe_invoice("in_stale", stripe::InvoiceStatus::Open)),
            ok(stripe_invoice("in_stale", stripe::InvoiceStatus::Draft)),
        ])
        .await;
        let provider = StripeBillingProvider::from_client(client.clone());
        assert!(matches!(
            process_invoice(&cmd, &provider, &client, &pool, &skipped)
                .await
                .unwrap(),
            InvoiceResult::AlreadyProcessed
        ));
        assert_eq!(
            *requests.lock().unwrap(),
            [
                "GET /v1/customers/cus_acme",
                "GET /v1/invoices/in_stale",
                "GET /v1/invoices/in_stale",
            ]
        );
        let ids: (Option<String>, Option<String>, Option<String>, Option<String>) = sqlx::query_as(
            "select stripe_customer_id, (select stripe_invoice_id from internal.billing_historicals where tenant = $1 and billed_month = '2026-08-01'), (select stripe_invoice_id from internal.manual_bills where tenant = $1 and date_start = '2026-07-15'), (select stripe_invoice_id from internal.manual_bills where tenant = $1 and date_start = '2025-01-01') from tenants where tenant = $1",
        )
        .bind(TENANT)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            ids,
            (
                Some("cus_acme".into()),
                None,
                Some("in_draft".into()),
                Some("in_stale".into())
            )
        );
    }

    #[sqlx::test(migrations = "../../supabase/migrations")]
    async fn stale_draft_id_is_cleared_only_after_delete(pool: sqlx::PgPool) {
        sqlx::query("insert into tenants (id, tenant) values (internal.id_generator(), $1)")
            .bind(TENANT)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            "insert into internal.manual_bills (tenant, usd_cents, description, date_start, date_end, stripe_invoice_id) values ($1, 10000, 'Cleanup candidate', '2025-01-01', '2025-01-31', 'in_stale')",
        )
        .bind(TENANT)
        .execute(&pool)
        .await
        .unwrap();

        let mut invoice = bill(InvoiceType::Manual, Some("in_stale"));
        invoice.date_start = "2025-01-01".parse().unwrap();
        invoice.date_end = "2025-01-31".parse().unwrap();
        let failed_delete = (
            StatusCode::INTERNAL_SERVER_ERROR,
            serde_json::json!({"error":{"message":"failure"}}),
        );
        let deleted = ok(stripe_invoice("in_stale", stripe::InvoiceStatus::Draft));

        for (delete_response, expected) in [(failed_delete, Some("in_stale")), (deleted, None)] {
            let (client, requests) = stripe_stub(vec![
                ok(stripe_invoice("in_stale", stripe::InvoiceStatus::Draft)),
                delete_response,
            ])
            .await;
            let provider = StripeBillingProvider::from_client(client.clone());
            clean_up_stale_draft(&provider, &client, &pool, &invoice, false).await;

            assert_eq!(
                *requests.lock().unwrap(),
                ["GET /v1/invoices/in_stale", "DELETE /v1/invoices/in_stale",]
            );
            let id: Option<String> = sqlx::query_scalar(
                "select stripe_invoice_id from internal.manual_bills where tenant = $1 and date_start = '2025-01-01'",
            )
            .bind(TENANT)
            .fetch_one(&pool)
            .await
            .unwrap();
            assert_eq!(id.as_deref(), expected);
        }
    }

    #[sqlx::test(migrations = "../../supabase/migrations")]
    async fn loads_month_bills_from_db_with_tenant_filters(pool: sqlx::PgPool) {
        sqlx::raw_sql(
            r#"
            insert into tenants (id, tenant, created_at) values
                (internal.id_generator(), 'acmeCo/', '2020-01-01'),
                (internal.id_generator(), 'otherCo/', '2020-01-01');
            insert into internal.billing_historicals (tenant, billed_month, report, stripe_invoice_id) values
                ('acmeCo/', '2026-08-01', '{"billed_month":"2026-08-01","subtotal":10000}', 'in_acme'),
                ('otherCo/', '2026-08-01', '{"billed_month":"2026-08-01","subtotal":10000}', null);
            insert into internal.manual_bills (tenant, usd_cents, description, date_start, date_end, stripe_invoice_id) values
                ('acmeCo/', 10000, 'overlap', '2026-07-15', '2026-09-14', 'in_manual'),
                ('acmeCo/', 10000, 'outside', '2026-09-01', '2026-09-30', null);
            "#,
        )
        .execute(&pool)
        .await
        .unwrap();

        let invoices = load_invoices(
            &pool,
            "2026-08-01".parse().unwrap(),
            &[],
            &["otherCo/".to_string()],
        )
        .await
        .unwrap();
        let mut selected: Vec<_> = invoices
            .iter()
            .map(|invoice| {
                (
                    invoice.invoice_type,
                    invoice.date_start,
                    invoice.date_end,
                    invoice.stripe_invoice_id.as_deref(),
                )
            })
            .collect();
        selected.sort_by_key(|row| row.1);

        assert_eq!(
            selected,
            [
                (
                    InvoiceType::Manual,
                    "2026-07-15".parse().unwrap(),
                    "2026-09-14".parse().unwrap(),
                    Some("in_manual"),
                ),
                (
                    InvoiceType::Final,
                    "2026-08-01".parse().unwrap(),
                    "2026-08-31".parse().unwrap(),
                    Some("in_acme"),
                ),
            ]
        );

        let selected = load_invoices(
            &pool,
            "2026-08-01".parse().unwrap(),
            &["otherCo/".to_string()],
            &[],
        )
        .await
        .unwrap();
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].stripe_invoice_id, None);
    }

    #[sqlx::test(migrations = "../../supabase/migrations")]
    async fn records_bill_invoice_ids(pool: sqlx::PgPool) {
        sqlx::raw_sql(
            r#"
            insert into tenants (id, tenant) values (internal.id_generator(), 'acmeCo/');
            insert into internal.billing_historicals (tenant, billed_month, report)
                values ('acmeCo/', '2026-08-01T00:00:00Z', '{"billed_month": "2026-08-01T00:00:00Z"}');
            insert into internal.manual_bills (tenant, usd_cents, description, date_start, date_end)
                values ('acmeCo/', 10000, 'Contract', '2026-07-15', '2026-09-14');
            "#,
        )
        .execute(&pool)
        .await
        .unwrap();

        let id = |id: &str| id.parse::<stripe::InvoiceId>().unwrap();
        let usage = bill(InvoiceType::Final, None);
        let manual = bill(InvoiceType::Manual, None);

        usage
            .record_invoice_id(&pool, Some(&id("in_usage")))
            .await
            .unwrap();
        // An invoice is only recorded over the one loaded with the bill.
        assert!(
            usage
                .record_invoice_id(&pool, Some(&id("in_other")))
                .await
                .is_err()
        );
        bill(InvoiceType::Final, Some("in_usage"))
            .record_invoice_id(&pool, Some(&id("in_replacement")))
            .await
            .unwrap();

        manual
            .record_invoice_id(&pool, Some(&id("in_manual")))
            .await
            .unwrap();
        // The recorded invoice follows the manual bill through edits to its
        // dates, but a run that read the bill's old dates can't record onto it.
        sqlx::query("update internal.manual_bills set date_end = '2026-09-30'")
            .execute(&pool)
            .await
            .unwrap();
        let err = bill(InvoiceType::Manual, Some("in_manual"))
            .record_invoice_id(&pool, None)
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("changed during this run"),
            "{err:#}"
        );

        let recorded: Vec<(chrono::NaiveDate, chrono::NaiveDate, String, Option<String>)> =
            sqlx::query_as(
                r#"
                select date_start, date_end, invoice_type, stripe_invoice_id
                from internal.invoices_ext
                where billed_prefix = 'acmeCo/' and invoice_type != 'preview'
                order by invoice_type
                "#,
            )
            .fetch_all(&pool)
            .await
            .unwrap();
        insta::assert_debug_snapshot!(recorded, @r#"
        [
            (
                2026-08-01,
                2026-08-31,
                "final",
                Some(
                    "in_replacement",
                ),
            ),
            (
                2026-07-15,
                2026-09-30,
                "manual",
                Some(
                    "in_manual",
                ),
            ),
        ]
        "#);
    }

    #[sqlx::test(migrations = "../../supabase/migrations")]
    async fn draft_details_follow_bill_dates(pool: sqlx::PgPool) {
        let manual = bill(InvoiceType::Manual, Some("in_draft"));
        let mut draft = stripe_invoice("in_draft", stripe::InvoiceStatus::Draft);
        draft.collection_method = Some(stripe::CollectionMethod::SendInvoice);
        draft.amount_due = Some(manual.subtotal);
        let (client, requests) = stripe_stub(vec![
            ok(&draft),
            ok(&draft),
            ok(stripe::List::<stripe::InvoiceItem>::default()),
            ok(&draft),
            ok(customer()),
        ])
        .await;

        let action = InvoiceAction::Update {
            existing_invoice_id: "in_draft".parse().unwrap(),
            customer: customer(),
        };
        let result = manual
            .execute(
                &StripeBillingProvider::from_client(client.clone()),
                &client,
                &pool,
                action,
                ChargeType::SendInvoice,
            )
            .await
            .unwrap();
        assert!(matches!(result, InvoiceResult::Updated));
        assert_eq!(
            *requests.lock().unwrap(),
            [
                "GET /v1/invoices/in_draft",
                "POST /v1/invoices/in_draft",
                "GET /v1/invoiceitems?invoice=in_draft",
                "GET /v1/invoices/in_draft",
                "GET /v1/customers/cus_acme",
            ]
        );
    }
}
