use crate::controlplane;
use anyhow::bail;
use futures::{StreamExt, TryStreamExt};
use itertools::Itertools;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{
    collections::HashMap,
    time::{SystemTime, UNIX_EPOCH},
};
use stripe::List;
use time::{macros::format_description, Date, Duration, OffsetDateTime};

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
/// Publish bills from the control-plane database as Stripe invoices.
/// Recommend running with RUST_LOG=info to get logging
pub struct PublishInvoice {
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
    month: Date,
    /// Whether to delete and recreate finalized invoices
    #[clap(long)]
    recreate_finalized: bool,
    /// Stop execution after first failure
    #[clap(long)]
    fail_fast: bool,
}

fn parse_date(arg: &str) -> Result<Date, time::error::Parse> {
    Date::parse(arg, &format_description!("[year]-[month]-[day]"))
}

#[derive(Deserialize, Debug)]
struct LineItem {
    count: f64,
    subtotal: i64,
    description: Option<String>,
}

#[derive(Deserialize, Debug)]
struct Bill {
    subtotal: i64,
    line_items: Vec<LineItem>,
    #[serde(with = "time::serde::rfc3339")]
    billed_month: OffsetDateTime,
    billed_prefix: String,
    recurring_fee: i64,
    task_usage_hours: f64,
    processed_data_gb: f64,
}

#[derive(Deserialize, Debug)]
struct UserResponse {
    capability: String,
    user_email: String,
}

const TENANT_METADATA_KEY: &str = "estuary.dev/tenant_name";
const CREATED_BY_BILLING_AUTOMATION: &str = "estuary.dev/created_by_automation";
const BILLED_MONTH_KEY: &str = "estuary.dev/billed_month";

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

async fn get_or_create_customer_for_tenant(
    client: &stripe::Client,
    ctrlplane_client: &controlplane::Client,
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
    .await?;

    let customer = if let Some(customer) = customers.data.into_iter().next() {
        tracing::debug!(
            "Found existing customer {id} for tenant {tenant}",
            id = customer.id.to_string()
        );
        customer
    } else {
        tracing::debug!("Creating new customer for tenant {tenant}");
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
        let query = vec![
            ("select", "capability,user_full_name,user_email".to_string()),
            (
                "and",
                format!("(user_email.neq.null,object_role.eq.{tenant})"),
            ),
        ];
        let responses = ctrlplane_client
            .from("combined_grants_ext")
            .build()
            .query(query.as_slice())
            .send()
            .await?
            .json::<Vec<UserResponse>>()
            .await?;

        let found_admin = responses
            .iter()
            .find(|response| response.user_email.len() > 0 && response.capability.eq(&"admin"));
        if let Some(admin) = found_admin {
            tracing::warn!("Stripe customer object for {tenant} is missing an email. Going with {email}, an admin on that tenant.", email=admin.user_email);
            stripe::Customer::update(
                client,
                &customer.id,
                stripe::UpdateCustomer {
                    email: Some(&admin.user_email),
                    ..Default::default()
                },
            )
            .await?;
        } else {
            bail!("Stripe customer object for {tenant} is missing an email. No admins found for that tenant, unable to create invoice without email. Skipping");
        }
    }
    Ok(customer)
}

impl Bill {
    pub async fn upsert_invoice(
        &self,
        client: &stripe::Client,
        ctrlplane_client: &controlplane::Client,
        recreate_finalized: bool,
    ) -> anyhow::Result<()> {
        let tenant = self.billed_prefix.to_owned();

        if !(self.recurring_fee > 0
            || self.processed_data_gb > 0.0
            || self.task_usage_hours > 0.0
            || self.subtotal > 0)
        {
            tracing::debug!("Skipping tenant '{tenant}' with no usage");
            return Ok(());
        } else {
            tracing::info!(
                "Publishing invoice for '{tenant}': ${amount:.2}",
                amount = self.subtotal as f64 / 100.0
            );
        }

        let customer =
            get_or_create_customer_for_tenant(client, ctrlplane_client, tenant.to_owned()).await?;
        let customer_id = customer.id.to_string();
        let billed_month_repr = self
            .billed_month
            .format(&format_description!("[year]-[month]"))?;
        let billed_month_human_repr = self
            .billed_month
            .format(&format_description!("[month repr:long] [year]"))?;

        let maybe_invoice = if let Some(invoice) = stripe_search::<stripe::Invoice>(
            client,
            "invoices",
            SearchParams {
                query: format!("customer:\"{customer_id}\" AND -status:\"deleted\" AND metadata[\"{BILLED_MONTH_KEY}\"]:\"{billed_month_repr}\""),
                ..Default::default()
            },
        )
        .await?
        .data
        .into_iter()
        .find(|invoice| {
            invoice
                .metadata
                .get(BILLED_MONTH_KEY)
                .eq(&Some(&billed_month_repr))
        }) {
            match invoice.status {
                Some(state @ (stripe::InvoiceStatus::Open | stripe::InvoiceStatus::Draft))
                    if recreate_finalized =>
                {
                    tracing::warn!(
                        "Found invoice {id} for {tenant}, in state {state} deleting and recreating",
                        id = invoice.id.to_string(),
                        state = state
                    );
                    stripe::Invoice::delete(client, &invoice.id).await?;
                    None
                }
                Some(stripe::InvoiceStatus::Draft) => {
                    tracing::debug!(
                        "Updating existing invoice {id} for {tenant}",
                        id = invoice.id.to_string()
                    );
                    Some(invoice)
                }
                Some(stripe::InvoiceStatus::Open) => {
                    bail!("Found finalized invoice {id} for {tenant}. Pass --recreate-finalized to delete and recreate this invoice.", id = invoice.id.to_string())
                }
                Some(status) => {
                    bail!(
                        "Found invoice {id} for {tenant} in unsupported state {status}, skipping.",
                        id = invoice.id.to_string(),
                        status = status
                    );
                }
                None => {
                    bail!(
                        "Unexpected missing status from invoice {id} for {tenant}",
                        id = invoice.id.to_string()
                    );
                }
            }
        } else {
            None
        };

        let due_date: SystemTime = SystemTime::now() + (Duration::DAY * 30);

        let invoice = match maybe_invoice {
            Some(inv) => inv,
            None => {
                tracing::debug!("Creating a new invoice for {tenant}");
                stripe::Invoice::create(
                    client,
                    stripe::CreateInvoice {
                        customer: Some(customer.id.to_owned()),
                        // Stripe timestamps are measured in _seconds_ since epoch
                        due_date: Some(due_date.duration_since(UNIX_EPOCH)?.as_secs() as i64),
                        description: Some(
                            format!(
                                "Your Flow bill for the {billed_month_human_repr} billing preiod."
                            )
                            .as_str(),
                        ),
                        collection_method: Some(stripe::CollectionMethod::SendInvoice),
                        auto_advance: Some(false),
                        custom_fields: Some(vec![
                            stripe::CreateInvoiceCustomFields {
                                name: "Billing Period".to_string(),
                                value: billed_month_human_repr,
                            },
                            stripe::CreateInvoiceCustomFields {
                                name: "Tenant".to_string(),
                                value: tenant.to_owned(),
                            },
                        ]),
                        metadata: Some(HashMap::from([
                            (BILLED_MONTH_KEY.to_string(), billed_month_repr),
                            (TENANT_METADATA_KEY.to_string(), tenant.to_owned()),
                        ])),
                        ..Default::default()
                    },
                )
                .await?
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
            stripe::InvoiceItem::delete(client, &item.id).await?;
        }

        for item in self.line_items.iter() {
            tracing::debug!(
                "Created new invoice line item for {tenant}: '{desc}'",
                desc = item.description.to_owned().unwrap_or_default()
            );
            stripe::InvoiceItem::create(
                client,
                stripe::CreateInvoiceItem {
                    amount: Some(item.subtotal),
                    currency: Some(stripe::Currency::USD),
                    description: Some(&format!(
                        "{desc} - {amount}",
                        desc = item.description.to_owned().unwrap_or_default(),
                        amount = (item.count * 100.0).floor() / 100.0
                    )),
                    invoice: Some(invoice.id.to_owned()),
                    ..stripe::CreateInvoiceItem::new(customer.id.to_owned())
                },
            )
            .await?;
        }

        // Let's double-check that the invoice total matches the desired total
        let check_invoice = stripe::Invoice::retrieve(client, &invoice.id, &[]).await?;

        if !check_invoice.amount_due.eq(&Some(self.subtotal)) {
            bail!(
                "The correct bill is ${our_bill:.2}, but the invoice's total is ${their_bill:.2}",
                our_bill = self.subtotal as f64 / 100.0,
                their_bill = check_invoice.amount_due.unwrap_or(0) as f64 / 100.0
            )
        }

        Ok(())
    }
}

#[derive(Deserialize, Debug)]
struct Response {
    report: Bill,
}

pub async fn do_publish_invoices(
    ctx: &mut crate::CliContext,
    cmd: &PublishInvoice,
) -> anyhow::Result<()> {
    let ctrl_plane_client = ctx.controlplane_client().await?;
    let stripe_client = stripe::Client::new(cmd.stripe_api_key.to_owned());

    let month_human_repr = cmd
        .month
        .format(&format_description!("[month repr:long] [year]"))?;

    let month_pg_repr = cmd
        .month
        .format(&format_description!("[year]-[month]-[day]"))?;

    tracing::info!("Fetching billing data for {month_human_repr}");

    let mut query = vec![
        ("select", "report".to_string()),
        ("billed_month", format!("eq.{month_pg_repr}")),
    ];

    if cmd.tenants.len() > 0 {
        let joined = cmd
            .tenants
            .iter()
            .map(|tenant| format!("\"{tenant}\""))
            .join(",");
        // See docs for filtering operations in postrgrest:
        // https://postgrest.org/en/stable/references/api/tables_views.html?highlight=querying#operators
        let inner = format!("in.({joined})");
        query.push(("tenant", inner));
    }

    let req = ctrl_plane_client
        .from("billing_historicals")
        .build()
        .query(query.as_slice());
    tracing::debug!(?req, "built request to execute");

    let responses = req.send().await?.json::<Vec<Response>>().await?;

    let futures = responses.iter().map(|response| {
        let client = stripe_client.clone();
        let ctrlplane_client = ctrl_plane_client.clone();
        async move {
            let res = response
                .report
                .upsert_invoice(&client, &ctrlplane_client, cmd.recreate_finalized)
                .await;
            if let Err(error) = res {
                let formatted = format!(
                    "Error publishing invoice for tenant {tenant}: {err}",
                    tenant = response.report.billed_prefix,
                    err = error.to_string()
                );
                bail!("{}", formatted);
            }
            Ok(())
        }
    });

    futures::stream::iter(futures)
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
