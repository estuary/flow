use anyhow::{bail, Context};
use chrono::{Months, ParseError, Utc};
use core::fmt;
use futures::{Future, FutureExt, StreamExt, TryStreamExt};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sqlx::{postgres::PgPoolOptions, types::chrono::NaiveDate, Pool};
use sqlx::{types::chrono::DateTime, Postgres};
use std::collections::HashMap;
use stripe::List;

const TENANT_METADATA_KEY: &str = "estuary.dev/tenant_name";
const CREATED_BY_BILLING_AUTOMATION: &str = "estuary.dev/created_by_automation";
const INVOICE_TYPE_KEY: &str = "estuary.dev/invoice_type";
const BILLING_PERIOD_START_KEY: &str = "estuary.dev/period_start";
const BILLING_PERIOD_END_KEY: &str = "estuary.dev/period_end";

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
    #[clap(long, value_delimiter = ',', required_unless_present("all-tenants"))]
    tenants: Vec<String>,
    /// Generate invoices for all tenants that have bills in the provided month.
    #[clap(long, conflicts_with("tenants"))]
    all_tenants: bool,
    /// The month to generate invoices for, in format "YYYY-MM-DD"
    #[clap(long, parse(try_from_str = parse_date))]
    month: NaiveDate,
    /// Whether to delete and recreate finalized invoices
    #[clap(long)]
    recreate_finalized: bool,
    /// Stop execution after first failure
    #[clap(long)]
    fail_fast: bool,
}

fn parse_date(arg: &str) -> Result<NaiveDate, ParseError> {
    NaiveDate::parse_from_str(arg, "%Y-%m-%d")
}

#[derive(Debug)]
enum InvoiceType {
    Usage,
    Manual,
}

impl InvoiceType {
    pub fn to_string(&self) -> String {
        match self {
            InvoiceType::Usage => "usage".to_string(),
            InvoiceType::Manual => "manual".to_string(),
        }
    }
    pub fn from_str(str: &str) -> Option<Self> {
        match str {
            "Usage" => Some(Self::Usage),
            "Manual" => Some(Self::Manual),
            _ => None,
        }
    }
}

impl fmt::Display for InvoiceType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_string())
    }
}

#[derive(Serialize, Default, Debug)]
struct SearchParams {
    pub query: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub page: Option<u64>,
}

async fn stripe_search<R: DeserializeOwned + 'static + Send>(
    client: &stripe::Client,
    resource: &str,
    params: SearchParams,
) -> Result<List<R>, stripe::StripeError> {
    client
        .get_query(&format!("/{}/search", resource), &params)
        .await
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct ManualBill {
    tenant: String,
    usd_cents: i32,
    description: String,
    date_start: NaiveDate,
    date_end: NaiveDate,
}

impl ManualBill {
    pub async fn upsert_invoice(
        &self,
        client: &stripe::Client,
        db_client: &Pool<Postgres>,
        recreate_finalized: bool,
    ) -> anyhow::Result<()> {
        upsert_invoice(
            client,
            db_client,
            self.date_start,
            self.date_end,
            self.tenant.to_owned(),
            InvoiceType::Manual,
            self.usd_cents as i64,
            vec![LineItem {
                count: 1.0,
                subtotal: self.usd_cents as i64,
                rate: self.usd_cents as i64,
                description: Some(self.description.to_owned()),
            }],
            recreate_finalized,
        )
        .await?;

        Ok(())
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct LineItem {
    count: f64,
    subtotal: i64,
    rate: i64,
    description: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Bill {
    subtotal: i64,
    line_items: Vec<LineItem>,
    billed_month: DateTime<Utc>,
    billed_prefix: String,
    recurring_fee: i64,
    task_usage_hours: f64,
    processed_data_gb: f64,
}

impl Bill {
    pub async fn upsert_invoice(
        &self,
        client: &stripe::Client,
        db_client: &Pool<Postgres>,
        recreate_finalized: bool,
    ) -> anyhow::Result<()> {
        if !(self.recurring_fee > 0
            || self.processed_data_gb > 0.0
            || self.task_usage_hours > 0.0
            || self.subtotal > 0)
        {
            tracing::debug!("Skipping tenant with no usage");
            return Ok(());
        }
        upsert_invoice(
            client,
            db_client,
            self.billed_month.date_naive(),
            self.billed_month
                .date_naive()
                .checked_add_months(Months::new(1))
                .expect("Only fails when adding > max 32-bit int months"),
            self.billed_prefix.to_owned(),
            InvoiceType::Usage,
            self.subtotal,
            self.line_items.clone(),
            recreate_finalized,
        )
        .await?;

        Ok(())
    }
}

pub async fn do_publish_invoices(cmd: &PublishInvoice) -> anyhow::Result<()> {
    let stripe_client = stripe::Client::new(cmd.stripe_api_key.to_owned());
    let db_pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&cmd.connection_string)
        .await?;

    let month_human_repr = cmd.month.format("%B %Y");

    tracing::info!("Fetching billing data for {month_human_repr}");

    let billing_historicals: Vec<_> = if cmd.tenants.len() > 0 {
        sqlx::query!(
            r#"
                select report as "report!: sqlx::types::Json<Bill>"
                from billing_historicals
                where billed_month = date_trunc('day', $1::date)
                and tenant = any($2)
            "#,
            cmd.month,
            &cmd.tenants[..]
        )
        .fetch_all(&db_pool)
        .await?
        .into_iter()
        .map(|response| response.report)
        .collect()
    } else {
        sqlx::query!(
            r#"
                select report as "report!: sqlx::types::Json<Bill>"
                from billing_historicals
                where billed_month = date_trunc('day', $1::date)
            "#,
            cmd.month
        )
        .fetch_all(&db_pool)
        .await?
        .into_iter()
        .map(|response| response.report)
        .collect()
    };

    let billing_historicals_futures: Vec<_> = billing_historicals
        .iter()
        .map(|response| {
            let client = stripe_client.clone();
            let db_pool = db_pool.clone();
            async move {
                let res = response
                    .upsert_invoice(&client, &db_pool, cmd.recreate_finalized)
                    .await;
                if let Err(error) = res {
                    let formatted = format!(
                        "Error publishing invoice for {tenant}",
                        tenant = response.billed_prefix,
                    );
                    bail!("{}\n{err:?}", formatted, err = error);
                }
                Ok(())
            }
            .boxed()
        })
        .collect();

    let manual_bills: Vec<ManualBill> = if cmd.tenants.len() > 0 {
        sqlx::query_as!(
            ManualBill,
            r#"
                select tenant, usd_cents, description, date_start, date_end
                from manual_bills
                where date_start >= date_trunc('day', $1::date)
                and tenant = any($2)
            "#,
            cmd.month,
            &cmd.tenants[..]
        )
        .fetch_all(&db_pool)
        .await?
    } else {
        sqlx::query_as!(
            ManualBill,
            r#"
                select tenant, usd_cents, description, date_start, date_end
                from manual_bills
                where date_start >= date_trunc('day', $1::date)
            "#,
            cmd.month
        )
        .fetch_all(&db_pool)
        .await?
    };

    let manual_futures: Vec<_> = manual_bills
        .iter()
        .map(|response| {
            let client = stripe_client.clone();
            let db_pool = db_pool.clone();
            async move {
                let res = response
                    .upsert_invoice(&client, &db_pool, cmd.recreate_finalized)
                    .await;
                if let Err(error) = res {
                    let formatted = format!(
                        "Error publishing invoice for {tenant}",
                        tenant = response.tenant,
                    );
                    bail!("{}\n{err:?}", formatted, err = error);
                }
                Ok(())
            }
            .boxed()
        })
        .collect();

    tracing::info!(
        "Processing {usage} usage-based bills, and {manual} manually-entered bills.",
        usage = billing_historicals.len(),
        manual = manual_bills.len()
    );

    futures::stream::iter(
        manual_futures
            .into_iter()
            .chain(billing_historicals_futures.into_iter()),
    )
    // Let's run 10 `upsert_invoice()`s at a time
    .buffer_unordered(10)
    .or_else(|err| async move {
        if !cmd.fail_fast {
            tracing::error!("{}", err.to_string());
            Ok(())
        } else {
            Err(err)
        }
    })
    // Collects into Result<(), anyhow::Error> because a stream of ()s can be collected into a single ()
    .try_collect()
    .await
}

#[tracing::instrument(skip_all)]
async fn get_or_create_customer_for_tenant(
    client: &stripe::Client,
    db_client: &Pool<Postgres>,
    tenant: String,
) -> anyhow::Result<stripe::Customer> {
    let customers = stripe_search::<stripe::Customer>(
        client,
        "customers",
        SearchParams {
            query: format!("metadata[\"{TENANT_METADATA_KEY}\"]:\"{tenant}\""),
            ..Default::default()
        },
    )
    .await
    .context("Searching for a customer")?;

    let customer = if let Some(customer) = customers.data.into_iter().next() {
        tracing::debug!("Found existing customer {id}", id = customer.id.to_string());
        customer
    } else {
        tracing::debug!("Creating new customer");
        let new_customer = stripe::Customer::create(
            client,
            stripe::CreateCustomer {
                name: Some(tenant.as_str()),
                description: Some(
                    format!("Represents the billing entity for Flow tenant '{tenant}'").as_str(),
                ),
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
        .await?;

        new_customer
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
            tracing::warn!("Stripe customer object is missing an email. Going with {email}, an admin on that tenant.");
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
            bail!("Stripe customer object is missing an email. No admins found for that tenant, unable to create invoice without email. Found users: {found:?} Skipping", found=responses);
        }
    }
    Ok(customer)
}

#[tracing::instrument(skip(client, db_client, subtotal, items), fields(subtotal=format!("${:.2}", subtotal as f64 / 100.0)))]
async fn upsert_invoice(
    client: &stripe::Client,
    db_client: &Pool<Postgres>,
    date_start: NaiveDate,
    date_end: NaiveDate,
    tenant: String,
    invoice_type: InvoiceType,
    subtotal: i64,
    items: Vec<LineItem>,
    recreate_finalized: bool,
) -> anyhow::Result<stripe::Invoice> {
    // Anything before 12:00:00 renders as the previous day in Stripe
    let date_start_secs = date_start
        .and_hms_opt(12, 0, 0)
        .expect("Error manipulating date")
        .and_local_timezone(Utc)
        .single()
        .expect("Error manipulating date")
        .timestamp();
    let date_end_secs = date_end
        .and_hms_opt(12, 0, 0)
        .expect("Error manipulating date")
        .and_local_timezone(Utc)
        .single()
        .expect("Error manipulating date")
        .timestamp();

    let timestamp_now = Utc::now().timestamp();

    tracing::debug!(date_start_secs, date_end_secs, "Debug");

    let date_start_human = date_start.format("%B %d %Y").to_string();
    let date_end_human = date_end.format("%B %d %Y").to_string();

    let date_start_repr = date_start.format("%F").to_string();
    let date_end_repr = date_end.format("%F").to_string();

    let invoice_type_str = invoice_type.to_string();

    let customer = get_or_create_customer_for_tenant(client, db_client, tenant.to_owned()).await?;
    let customer_id = customer.id.to_string();

    let invoice_search = stripe_search::<stripe::Invoice>(
        client,
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

    let maybe_invoice = if let Some(invoice) = invoice_search.data.into_iter().next() {
        match invoice.status {
            Some(state @ (stripe::InvoiceStatus::Open | stripe::InvoiceStatus::Draft))
                if recreate_finalized =>
            {
                tracing::warn!(
                    "Found invoice {id} in state {state} deleting and recreating",
                    id = invoice.id.to_string(),
                    state = state
                );
                stripe::Invoice::delete(client, &invoice.id).await?;
                None
            }
            Some(stripe::InvoiceStatus::Draft) => {
                tracing::debug!(
                    "Updating existing invoice {id}",
                    id = invoice.id.to_string()
                );
                Some(invoice)
            }
            Some(stripe::InvoiceStatus::Open) => {
                bail!("Found finalized invoice {id}. Pass --recreate-finalized to delete and recreate this invoice.", id = invoice.id.to_string())
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
        None
    };

    let invoice = match maybe_invoice {
        Some(inv) => inv,
        None => {
            let invoice = stripe::Invoice::create(
                client,
                stripe::CreateInvoice {
                    customer: Some(customer.id.to_owned()),
                    // Stripe timestamps are measured in _seconds_ since epoch
                    // Due date must be in the future
                    due_date: if date_end_secs > timestamp_now { Some(date_end_secs) } else {Some(timestamp_now + 10)},
                    description: Some(
                        format!(
                            "Your Flow bill for the billing preiod between {date_start_human} - {date_end_human}"
                        )
                        .as_str(),
                    ),
                    collection_method: Some(stripe::CollectionMethod::SendInvoice),
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
                        stripe::CreateInvoiceCustomFields {
                            name: "Tenant".to_string(),
                            value: tenant.to_owned(),
                        },
                    ]),
                    metadata: Some(HashMap::from([
                        (TENANT_METADATA_KEY.to_string(), tenant.to_owned()),
                        (INVOICE_TYPE_KEY.to_string(), invoice_type_str.to_owned()),
                        (BILLING_PERIOD_START_KEY.to_string(), date_start_repr),
                        (BILLING_PERIOD_END_KEY.to_string(), date_end_repr)
                    ])),
                    ..Default::default()
                },
            )
            .await.context("Creating a new invoice")?;

            tracing::debug!("Created a new invoice {id}", id = invoice.id);

            invoice
        }
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

    for item in items.iter() {
        let description = item
            .description
            .clone()
            .ok_or(anyhow::anyhow!("Missing line item description. Skipping"))?;
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
        tracing::warn!("Invoice line items use fractional quantities, which Stripe does not allow. Rounding up resulted in a difference of ${difference:.2}", difference = diff.ceil()/100.0);
    }

    // Let's double-check that the invoice total matches the desired total
    let check_invoice = stripe::Invoice::retrieve(client, &invoice.id, &[]).await?;

    if !check_invoice
        .amount_due
        .eq(&Some(subtotal + (diff.ceil() as i64)))
    {
        bail!(
            "The correct bill is ${our_bill:.2}, but the invoice's total is ${their_bill:.2}",
            our_bill = subtotal as f64 / 100.0,
            their_bill = check_invoice.amount_due.unwrap_or(0) as f64 / 100.0
        )
    }

    tracing::info!("Published invoice");

    Ok(invoice)
}
