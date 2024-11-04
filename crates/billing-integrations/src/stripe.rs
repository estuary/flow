use anyhow::{bail, Context};
use chrono::{Duration, ParseError, Utc};
use futures::{FutureExt, StreamExt, TryStreamExt};
use itertools::Itertools;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sqlx::Postgres;
use sqlx::{postgres::PgPoolOptions, types::chrono::NaiveDate, Pool};
use std::collections::HashMap;
use stripe::{InvoiceStatus, List};

const TENANT_METADATA_KEY: &str = "estuary.dev/tenant_name";
const CREATED_BY_BILLING_AUTOMATION: &str = "estuary.dev/created_by_automation";
const INVOICE_TYPE_KEY: &str = "estuary.dev/invoice_type";
const BILLING_PERIOD_START_KEY: &str = "estuary.dev/period_start";
const BILLING_PERIOD_END_KEY: &str = "estuary.dev/period_end";

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
    Error,
}

impl InvoiceResult {
    pub fn message(&self) -> String {
        match self {
            InvoiceResult::Created(provider) => {
                if provider == &PaymentProvider::Stripe {
                    "Published new invoice".to_string()
                } else {
                    format!("Published new invoice for tenant using {provider:?} provider")
                }
            }
            InvoiceResult::Updated => "Updated existing invoice".to_string(),
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
            InvoiceResult::Error => "Error publishing invoices".to_string(),
        }
    }
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
    capture_hours: Option<f64>,
    materialization_hours: Option<f64>,
    payment_provider: PaymentProvider,
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

        Ok(invoice_search.data.into_iter().next())
    }

    #[tracing::instrument(skip(self, client, db_client), fields(tenant=self.billed_prefix, invoice_type=format!("{:?}",self.invoice_type), subtotal=format!("${:.2}", self.subtotal as f64 / 100.0)))]
    pub async fn upsert_invoice(
        &self,
        client: &stripe::Client,
        db_client: &Pool<Postgres>,
        recreate_finalized: bool,
        mode: ChargeType,
    ) -> anyhow::Result<InvoiceResult> {
        match (&self.invoice_type, &self.extra) {
            (InvoiceType::Preview, _) => {
                bail!("Should not create Stripe invoices for preview invoices")
            }
            (InvoiceType::Final, Some(extra)) if !self.has_payment_method.unwrap_or(false) => {
                // The Stripe capture in the database has been known to be unreliable.
                // Let's double-check with Stripe to make sure it agrees that we really
                // do not have a payment method set.
                if let Some(customer) = get_or_create_customer_for_tenant(
                    client,
                    db_client,
                    self.billed_prefix.to_owned(),
                    false, // If there's no customer, there's no way there can be a payment method
                )
                .await?
                {
                    if let Some(_) = customer
                        .invoice_settings
                        .and_then(|i| i.default_payment_method)
                    {
                        bail!("Stripe reports customer {} ({}) has a payment method set, database disagrees.", customer.id.to_string(), self.billed_prefix.to_owned());
                    }
                }
                let unwrapped_extra = extra.clone().0.expect(
                    "This is just a sqlx quirk, if the outer Option is Some then this will be Some",
                );
                if unwrapped_extra.processed_data_gb.unwrap_or_default() == 0.0 {
                    return Ok(InvoiceResult::NoDataMoved);
                }

                if self.capture_hours.unwrap_or_default() == 0.0
                    || self.materialization_hours.unwrap_or_default() == 0.0
                {
                    return Ok(InvoiceResult::NoFullPipeline);
                }
            }
            (InvoiceType::Final, None) => {
                bail!("Invoice should have extra")
            }
            _ => {}
        };

        // An invoice should be generated in Stripe if the tenant is on a paid plan, which means:
        // * The tenant has a free trial start date
        // * The tenant's free trial start date is before the invoice period's end date
        if let InvoiceType::Final = self.invoice_type {
            match get_tenant_trial_date(&db_client, self.billed_prefix.to_owned()).await? {
                Some(trial_start) if self.date_end < trial_start => {
                    return Ok(InvoiceResult::FutureTrialStart);
                }
                None => {
                    return Ok(InvoiceResult::FreeTier);
                }
                _ => {}
            }
        }

        // The minimum chargable amount of USD in Stripe is $0.50.
        // https://stripe.com/docs/currencies#minimum-and-maximum-charge-amounts
        if self.subtotal < 50 {
            return Ok(InvoiceResult::LessThanMinimum);
        }

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

        let customer = get_or_create_customer_for_tenant(
            client,
            db_client,
            self.billed_prefix.to_owned(),
            true,
        )
        .await?
        .expect("Should never return None");
        let customer_id = customer.id.to_string();

        let maybe_invoice = if let Some(invoice) = self
            .get_stripe_invoice(&client, customer_id.as_str())
            .await?
        {
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
                    bail!("Found open invoice {id}. Pass --recreate-finalized to delete and recreate this invoice.", id = invoice.id.to_string())
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

        let invoice = match maybe_invoice.clone() {
            Some(inv) => inv,
            None => {
                let invoice = stripe::Invoice::create(
                    client,
                    stripe::CreateInvoice {
                        customer: Some(customer.id.to_owned()),
                        // Stripe timestamps are measured in _seconds_ since epoch
                        // Due date must be in the future. Bill net-30, so 30 days from today
                        due_date: match mode {
                            ChargeType::SendInvoice => Some((Utc::now() + Duration::days(30)).timestamp()),
                            ChargeType::AutoCharge => None
                        },
                        description: Some(
                            format!(
                                "Your Flow bill for the billing period between {date_start_human} - {date_end_human}. Tenant: {tenant}",
                                tenant=self.billed_prefix.to_owned()
                            )
                            .as_str(),
                        ),
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
                            (TENANT_METADATA_KEY.to_string(), self.billed_prefix.to_owned()),
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

        for item in self.line_items.iter() {
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

        // Customers can have an invoice credit balance, so let's make sure we take that into account.
        let credit_balance = customer.balance.unwrap_or(0);

        let expected = (self.subtotal + (diff.ceil() as i64) + credit_balance).max(0);

        if !check_invoice.amount_due.eq(&Some(expected)) {
            bail!(
                "The correct bill is ${our_bill:.2}, but the invoice's total is ${their_bill:.2}",
                our_bill = self.subtotal as f64 / 100.0,
                their_bill = check_invoice.amount_due.unwrap_or(0) as f64 / 100.0
            )
        }

        if maybe_invoice.is_some() {
            return Ok(InvoiceResult::Updated);
        } else {
            return Ok(InvoiceResult::Created(self.payment_provider));
        }
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
                    dataflow_hours.capture_hours::float as capture_hours,
                    dataflow_hours.materialization_hours::float as materialization_hours,
                    tenants.payment_provider as "payment_provider!: PaymentProvider"
                from invoices_ext
                left join tenants on tenants.tenant = billed_prefix
                inner join lateral(
                	select bool_or("invoice_settings/default_payment_method" is not null) as has_payment_method
                	from stripe.customers
                	where customers.metadata->>'estuary.dev/tenant_name' = billed_prefix
                	group by billed_prefix
                ) as customer on true
                inner join lateral(
                	select
                		sum(catalog_stats.usage_seconds) filter (where live_specs.spec_type = 'capture') / (60.0 * 60) as capture_hours,
                    	sum(catalog_stats.usage_seconds) filter (where live_specs.spec_type = 'materialization') / (60.0 * 60)  as materialization_hours
                    from catalog_stats
                    join live_specs on live_specs.catalog_name = catalog_stats.catalog_name
                    where
                    	catalog_stats.catalog_name ^@ billed_prefix
                    	and grain = 'monthly'
                    	and tstzrange(date_trunc('day', $1::date), date_trunc('day', ($1::date)) + interval '1 month' - interval '1 day') @> catalog_stats.ts
                ) as dataflow_hours on true
                where ((
                    date_start >= date_trunc('day', $1::date)
                    and date_end <= date_trunc('day', ($1::date)) + interval '1 month' - interval '1 day'
                    and invoice_type = 'final'
                ) or (
                    invoice_type = 'manual'
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
                    dataflow_hours.capture_hours::float as capture_hours,
                    dataflow_hours.materialization_hours::float as materialization_hours,
                    tenants.payment_provider as "payment_provider!: PaymentProvider"
                from invoices_ext
                left join tenants on tenants.tenant = billed_prefix
                inner join lateral(
                	select bool_or("invoice_settings/default_payment_method" is not null) as has_payment_method
                	from stripe.customers
                	where customers.metadata->>'estuary.dev/tenant_name' = billed_prefix
                	group by billed_prefix
                ) as customer on true
                inner join lateral(
                	select
                		sum(catalog_stats.usage_seconds) filter (where live_specs.spec_type = 'capture') / (60.0 * 60) as capture_hours,
                    	sum(catalog_stats.usage_seconds) filter (where live_specs.spec_type = 'materialization') / (60.0 * 60)  as materialization_hours
                    from catalog_stats
                    join live_specs on live_specs.catalog_name = catalog_stats.catalog_name
                    where
                    	catalog_stats.catalog_name ^@ billed_prefix
                    	and grain = 'monthly'
                    	and tstzrange(date_trunc('day', $1::date), date_trunc('day', ($1::date)) + interval '1 month' - interval '1 day') @> catalog_stats.ts
                ) as dataflow_hours on true
                where (
                    date_start >= date_trunc('day', $1::date)
                    and date_end <= date_trunc('day', ($1::date)) + interval '1 month' - interval '1 day'
                    and invoice_type = 'final'
                ) or (
                    invoice_type = 'manual'
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

    tracing::info!(
        "Processing {usage} usage-based invoices, and {manual} manually-entered invoices.",
        usage = invoice_type_counter
            .remove(&InvoiceType::Final)
            .unwrap_or_default(),
        manual = invoice_type_counter
            .remove(&InvoiceType::Manual)
            .unwrap_or_default(),
    );

    let invoice_futures: Vec<_> = invoices
        .iter()
        .map(|response| {
            let client = stripe_client.clone();
            let db_pool = db_pool.clone();
            async move {
                let res = response
                    .upsert_invoice(&client, &db_pool, cmd.recreate_finalized, cmd.charge_type)
                    .await;
                match res {
                    Err(err) => {
                        let formatted = format!(
                            "Error publishing {invoice_type:?} invoice for {tenant}",
                            tenant = response.billed_prefix,
                            invoice_type = response.invoice_type
                        );
                        bail!("{}: {err:?}", formatted, err = err);
                    }
                    Ok(res) => {
                        tracing::debug!(
                            tenant = response.billed_prefix,
                            invoice_type = format!("{:?}", response.invoice_type),
                            subtotal = format!("${:.2}", response.subtotal as f64 / 100.0),
                            "{}",
                            res.message()
                        );
                        match res {
                            InvoiceResult::Created(_)
                            | InvoiceResult::Updated
                            | InvoiceResult::Error => {}
                            // Remove any incorrectly created invoices that are now skipped for whatever reason
                            _ => {
                                let customer = match get_or_create_customer_for_tenant(
                                    &client,
                                    &db_pool,
                                    response.billed_prefix.to_owned(),
                                    false,
                                )
                                .await?
                                {
                                    Some(c) => c,
                                    None => {
                                        return Ok((
                                            res,
                                            response.subtotal,
                                            response.billed_prefix.to_owned(),
                                        ))
                                    }
                                };

                                let customer_id = customer.id.to_string();

                                if let Some(invoice) =
                                    response.get_stripe_invoice(&client, &customer_id).await?
                                {
                                    if let Some(InvoiceStatus::Draft) = invoice.status {
                                        tracing::warn!(
                                            tenant = response.billed_prefix.to_string(),
                                            "Deleting draft invoice!"
                                        );
                                        stripe::Invoice::delete(&client, &invoice.id).await?;
                                    }
                                }
                            }
                        }
                        Ok((res, response.subtotal, response.billed_prefix.to_owned()))
                    }
                }
            }
            .boxed()
        })
        .collect();

    let collected: HashMap<InvoiceResult, (i64, i32, Vec<(String, i64)>)> =
        futures::stream::iter(invoice_futures)
            .buffer_unordered(5)
            .or_else(|err| async move {
                if !cmd.fail_fast {
                    tracing::error!("{}", err.to_string());
                    Ok((InvoiceResult::Error, 0, "".to_string()))
                } else {
                    Err(err)
                }
            })
            .try_fold(
                HashMap::new(),
                |mut map, (res, subtotal, tenant)| async move {
                    let (subtotal_sum, count, tenants) = map.entry(res).or_insert((0, 0, vec![]));
                    *subtotal_sum += subtotal;
                    *count += 1;
                    tenants.push((tenant, subtotal));
                    Ok(map)
                },
            )
            .await?;

    for (status, (subtotal_agg, count, tenants)) in collected.iter() {
        tracing::info!(
            "[{:4} invoices]: {:70}${:.2}",
            count,
            status.message(),
            *subtotal_agg as f64 / 100.0
        );
        let limit = match status {
            InvoiceResult::Created(_) | InvoiceResult::Updated => 30,
            InvoiceResult::NoDataMoved
            | InvoiceResult::NoFullPipeline
            | InvoiceResult::LessThanMinimum
            | InvoiceResult::FreeTier => 0,
            _ => 4,
        };
        let sorted_tenants = tenants
            .iter()
            .sorted_by(|(_, a), (_, b)| b.cmp(a))
            .collect_vec();

        let (displayed_tenants, remainder_tenants) =
            sorted_tenants.split_at(limit.min(tenants.len()));
        for (tenant, subtotal) in displayed_tenants {
            tracing::info!(" - {:} ${:.2}", tenant, *subtotal as f64 / 100.0);
        }
        if limit > 0 && remainder_tenants.len() > 0 {
            tracing::info!(" - ... {} Others", remainder_tenants.len(),);
        }
    }

    Ok(())
}
#[tracing::instrument(skip(db_client))]
async fn get_tenant_trial_date(
    db_client: &Pool<Postgres>,
    tenant: String,
) -> anyhow::Result<Option<NaiveDate>> {
    let query_result = sqlx::query!(
        r#"
            select tenants.trial_start
            from tenants
            where tenants.tenant = $1
        "#,
        tenant
    )
    .fetch_one(db_client)
    .await?;

    Ok(query_result.trial_start)
}

#[tracing::instrument(skip(client, db_client))]
async fn get_or_create_customer_for_tenant(
    client: &stripe::Client,
    db_client: &Pool<Postgres>,
    tenant: String,
    create: bool,
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

    let customer = if let Some(customer) = customers.data.into_iter().next() {
        tracing::debug!("Found existing customer {id}", id = customer.id.to_string());
        customer
    } else if create {
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
    } else {
        return Ok(None);
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
            bail!("Stripe customer object is missing an email. No admins found for tenant {tenant}, unable to create invoice without email. Found users: {found:?} Skipping", found=responses, tenant=tenant);
        }
    }
    Ok(Some(customer))
}
