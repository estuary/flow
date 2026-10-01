use crate::stripe_utils::{Invoice, fetch_invoices};
use anyhow::Context;
use billing_types::{InvoiceSearch, InvoiceType, StatusFilter};
use chrono::{Datelike, Duration, NaiveDate, Utc};
use clap::Args;
use futures::{
    TryFutureExt,
    stream::{self, StreamExt},
};
use indicatif::{ProgressBar, ProgressStyle};
use itertools::Itertools;
use num_format::{Locale, ToFormattedString};
use std::collections::HashSet;
use stripe::{Client, FinalizeInvoiceParams, Invoice as StripeInvoice};

const PROGRESS_BAR_TEMPLATE: &str = "{spinner} [{elapsed_precise}] [{bar:40}] {pos}/{len} {msg}";

#[derive(Debug, Args)]
#[clap(rename_all = "kebab-case")]
/// Process and finalize invoices for a specific billing period. Invoices with auto_advance enabled will be charged automatically by Stripe.
pub struct SendInvoices {
    /// Stripe API key.
    #[clap(long)]
    pub stripe_api_key: String,
    /// The month to send invoices for: YYYY-MM (also accepts YYYY-MM-01)
    #[clap(long, value_parser = crate::parse_month)]
    pub month: NaiveDate,
    /// A list of tenants to exclude
    #[clap(long, value_delimiter = ',', conflicts_with = "tenants")]
    pub exclude_tenants: Vec<String>,
    /// A list of tenants to include (if set, excludes all others)
    #[clap(long, value_delimiter = ',', required_unless_present = "all_tenants")]
    pub tenants: Vec<String>,
    /// Whether to run on all tenants
    #[clap(long, conflicts_with = "tenants")]
    pub all_tenants: bool,
    /// Check for and fix invoices with auto_advance turned off
    #[clap(long)]
    pub fix_auto_advance: bool,
}

pub async fn do_send_invoices(cmd: &SendInvoices) -> anyhow::Result<()> {
    let stripe_client = Client::new(cmd.stripe_api_key.to_owned())
        .with_strategy(stripe::RequestStrategy::ExponentialBackoff(4));
    let month_start = cmd.month.format("%Y-%m-%d").to_string();
    let month_human_repr = cmd.month.format("%B %Y");
    tracing::info!("Fetching Stripe invoices to process for {month_human_repr}");

    let draft_final_query = InvoiceSearch {
        invoice_type: Some(InvoiceType::Final),
        period_start: Some(&month_start),
        status: StatusFilter::Only(stripe::InvoiceStatus::Draft),
        ..Default::default()
    }
    .to_query();
    let open_final_query = InvoiceSearch {
        invoice_type: Some(InvoiceType::Final),
        period_start: Some(&month_start),
        status: StatusFilter::Only(stripe::InvoiceStatus::Open),
        ..Default::default()
    }
    .to_query();

    // Separate queries for manual invoices (we'll filter dates client-side)
    let draft_manual_query = InvoiceSearch {
        invoice_type: Some(InvoiceType::Manual),
        status: StatusFilter::Only(stripe::InvoiceStatus::Draft),
        ..Default::default()
    }
    .to_query();
    let open_manual_query = InvoiceSearch {
        invoice_type: Some(InvoiceType::Manual),
        status: StatusFilter::Only(stripe::InvoiceStatus::Open),
        ..Default::default()
    }
    .to_query();

    // 1. Fetch invoices: final invoices with exact date match + all manual invoices
    let (
        mut draft_final_invoices,
        mut finalized_final_invoices,
        draft_manual_invoices,
        finalized_manual_invoices,
    ) = futures::try_join!(
        fetch_invoices(&stripe_client, &draft_final_query),
        fetch_invoices(&stripe_client, &open_final_query),
        fetch_invoices(&stripe_client, &draft_manual_query),
        fetch_invoices(&stripe_client, &open_manual_query)
    )?;

    // Filter manual invoices by date range client-side
    let month_start_date = cmd.month;
    let month_end_date = if month_start_date.month0() == 11 {
        // December is month0 = 11
        NaiveDate::from_ymd_opt(month_start_date.year() + 1, 1, 1).unwrap() - Duration::days(1)
    } else {
        NaiveDate::from_ymd_opt(month_start_date.year(), month_start_date.month0() + 2, 1).unwrap()
            - Duration::days(1)
    };

    let filter_manual_invoices =
        |invoices: Vec<crate::stripe_utils::Invoice>| -> Vec<crate::stripe_utils::Invoice> {
            invoices
                .into_iter()
                .filter(|inv| match (inv.period_start(), inv.period_end()) {
                    (Some(start_str), Some(end_str)) => {
                        let start = NaiveDate::parse_from_str(&start_str, "%Y-%m-%d")
                            .expect("period_start metadata should be a valid date");
                        let end = NaiveDate::parse_from_str(&end_str, "%Y-%m-%d")
                            .expect("period_end metadata should be a valid date");
                        start <= month_end_date && end >= month_start_date
                    }
                    _ => false,
                })
                .collect()
        };

    let filtered_draft_manual = filter_manual_invoices(draft_manual_invoices);
    let filtered_finalized_manual = filter_manual_invoices(finalized_manual_invoices);

    // Combine final and manual invoices
    draft_final_invoices.extend(filtered_draft_manual);
    finalized_final_invoices.extend(filtered_finalized_manual);

    // Rename for consistency with rest of function
    let mut draft_invoices = draft_final_invoices;
    let mut finalized_invoices = finalized_final_invoices;

    tracing::info!(
        "Fetched {} draft invoices for {month_human_repr}.",
        draft_invoices.len()
    );

    // Filter out any excluded tenants
    draft_invoices.retain(|inv| !cmd.exclude_tenants.contains(&inv.tenant()));
    finalized_invoices.retain(|inv| !cmd.exclude_tenants.contains(&inv.tenant()));

    if !cmd.all_tenants {
        // If a list of tenants is provided, filter to only those tenants
        draft_invoices.retain(|inv| cmd.tenants.contains(&inv.tenant()));
        finalized_invoices.retain(|inv| cmd.tenants.contains(&inv.tenant()));
    }

    tracing::info!(
        "Running against {} draft invoices for {month_human_repr}.",
        draft_invoices.len()
    );

    let selected_drafts = draft_invoices.len();
    draft_invoices = update_draft_collection_methods(&stripe_client, draft_invoices).await?;
    let mut failures = selected_drafts - draft_invoices.len();

    if !draft_invoices.is_empty() {
        print_invoice_table("Invoices to finalize", &draft_invoices);
        prompt_to_continue("Enter Y to finalize these invoices, or anything else to abort: ")
            .await?;

        // 2b. Move the draft invoices to the `open` state
        let selected = draft_invoices.len();
        let mut finalized = finalize_invoices(&stripe_client, draft_invoices).await?;
        failures += selected - finalized.len();
        finalized_invoices.append(&mut finalized);
    }

    if finalized_invoices.is_empty() {
        tracing::info!("No invoices to send for {month_human_repr}");
    }

    // 2c. Check for and fix auto_advance if flag is set
    if cmd.fix_auto_advance {
        failures += check_and_fix_auto_advance(&stripe_client, &finalized_invoices).await?;
    }

    // 3. Show final status of invoices (auto-advance will handle charging automatically)
    if !finalized_invoices.is_empty() {
        print_invoice_table("Final invoice status", &finalized_invoices);
        tracing::info!(
            "Processed {} invoices for {month_human_repr}. Invoices with auto_advance enabled will be charged automatically by Stripe.",
            finalized_invoices.len()
        );
    }
    anyhow::ensure!(
        failures == 0,
        "Failed to process {failures} invoice(s); review the errors above before retrying"
    );
    Ok(())
}

/// Invoices that are created with the `charge_automatically` collection method
/// can only proceed if the customer has a payment method on file. If not, the
/// invoice's collection method must be changed to 'send_invoice' and a due date
/// must be set in order to send the notification for manual payment.
async fn update_draft_collection_methods(
    stripe_client: &Client,
    mut to_update: Vec<Invoice>,
) -> anyhow::Result<Vec<Invoice>> {
    // Search results and an earlier publish run may have stale payment-method state.
    let mut refreshed = Vec::new();
    let mut needs_update = HashSet::new();
    for inv in to_update {
        let result = async {
            let current = Invoice::from(
                StripeInvoice::retrieve(stripe_client, inv.id(), &["customer"]).await?,
            );
            anyhow::ensure!(
                current.status() == Some(stripe::InvoiceStatus::Draft),
                "invoice is no longer a draft"
            );
            let needs_update = collection_method_needs_update(&current)
                .context("Invalid draft collection configuration; rerun publish-invoices")?;
            Ok::<_, anyhow::Error>((current, needs_update))
        }
        .await;
        match result {
            Ok((current, update)) => {
                if update {
                    needs_update.insert(current.id().clone());
                }
                refreshed.push(current);
            }
            Err(error) => {
                tracing::error!(invoice = %inv.id(), tenant = %inv.tenant(), error = %format!("{error:#}"), "Skipping invoice")
            }
        }
    }
    to_update = refreshed;

    // Modify the table row for those that need to be updated showing the transition
    let table_rows = to_update
        .iter()
        .filter_map(|inv| {
            if needs_update.contains(inv.id()) {
                let mut row = inv.to_table_row();
                row[4] = comfy_table::Cell::new("charge_automatically => send_invoice")
                    .fg(comfy_table::Color::Yellow)
                    .add_attribute(comfy_table::Attribute::Bold);
                Some(row)
            } else {
                None
            }
        })
        .collect_vec();

    if !table_rows.is_empty() {
        let table = build_invoice_table(table_rows, None);
        println!(
            "\nThe following draft invoices will be updated to use the 'send_invoice' collection method:"
        );
        println!("{}", table);

        prompt_to_continue("Enter Y to update collection methods, or anything else to abort: ")
            .await?;

        let (updates, mut unchanged): (Vec<_>, Vec<_>) = to_update
            .into_iter()
            .partition(|inv| needs_update.contains(inv.id()));
        unchanged.extend(update_collection_methods(stripe_client, updates).await?);
        to_update = unchanged;
    }

    Ok(to_update)
}

async fn update_collection_methods(
    stripe_client: &Client,
    invoices: Vec<Invoice>,
) -> anyhow::Result<Vec<Invoice>> {
    #[derive(serde::Serialize)]
    struct PostBody {
        collection_method: stripe::CollectionMethod,
        due_date: Option<i64>,
    }
    let pb = ProgressBar::new(invoices.len() as u64);
    pb.set_message("updating collection method");
    pb.set_style(ProgressStyle::with_template(PROGRESS_BAR_TEMPLATE).unwrap());
    let mut updated = Vec::new();
    for inv in invoices {
        let res: Result<stripe::Invoice, _> = stripe_client
            .post_form(
                &format!("/invoices/{}", inv.id()),
                PostBody {
                    collection_method: stripe::CollectionMethod::SendInvoice,
                    due_date: Some((Utc::now() + Duration::days(30)).timestamp()),
                },
            )
            .await;
        match res {
            Ok(mut invoice) => {
                invoice.customer = inv.customer.clone();
                updated.push(Invoice::from(invoice));
            }
            Err(e) => {
                tracing::error!(
                    invoice = %inv.id(),
                    tenant = %inv.tenant(),
                    error = %format!("{e:#}"),
                    "Skipping invoice after collection method update failed"
                );
            }
        }
        pb.inc(1);
    }
    pb.finish_with_message("Collection method update complete");
    Ok(updated)
}

// Missing payment methods are a late collection decision. Incorrect manual
// invoice configuration must be repaired by publish before operator approval.
fn collection_method_needs_update(invoice: &Invoice) -> anyhow::Result<bool> {
    let method = invoice.collection_method()?;
    anyhow::ensure!(
        !invoice.is_manual() || method == stripe::CollectionMethod::SendInvoice,
        "manual invoice must use send_invoice"
    );
    if method == stripe::CollectionMethod::SendInvoice {
        return Ok(false);
    }
    anyhow::ensure!(
        invoice.customer().is_some(),
        "missing expanded Stripe customer"
    );
    Ok(!invoice.has_cc())
}

// Finalizes the invoices and re-fetches them to ensure we have the correct state
// This calls `/invoices/{id}/finalize` to move draft invoices to the `open` state
async fn finalize_invoices(
    stripe_client: &Client,
    to_finalize: Vec<Invoice>,
) -> anyhow::Result<Vec<Invoice>> {
    let pb = ProgressBar::new(to_finalize.len() as u64);
    pb.set_message("finalizing invoices");
    pb.set_style(ProgressStyle::with_template(PROGRESS_BAR_TEMPLATE).unwrap());
    let finalize_futs = to_finalize.into_iter().map(|row| {
        let stripe_client = stripe_client;
        let pb = pb.clone();
        let context = format!("Invoice {} (tenant: {})", row.id(), row.tenant());
        async move {
            // The operator can pause at the prompt. Re-read before enabling collection,
            // and require another send run if the approved collection decision changed.
            let current = Invoice::from(
                StripeInvoice::retrieve(stripe_client, row.id(), &["customer"])
                    .await
                    .with_context(|| {
                        format!("Refreshing invoice {} before finalization", row.id())
                    })?,
            );
            anyhow::ensure!(
                current.status() == Some(stripe::InvoiceStatus::Draft),
                "invoice {} is no longer a draft",
                row.id()
            );
            anyhow::ensure!(
                current.collection_method()? == row.collection_method()?
                    && !collection_method_needs_update(&current).context(
                        "Invalid draft collection configuration; rerun publish-invoices"
                    )?,
                "invoice {} collection decision changed; rerun send-invoices",
                row.id()
            );
            StripeInvoice::finalize(
                stripe_client,
                row.id(),
                FinalizeInvoiceParams {
                    auto_advance: Some(true), // Turn on auto-advance to enable automatic retries
                },
            )
            .await
            .context("Finalizing invoice")?;
            pb.inc(1);

            let invoice = StripeInvoice::retrieve(
                stripe_client,
                row.id(),
                vec!["customer"].as_slice(),
            )
            .await
            .context(
                "Invoice was finalized but could not be read back; check Stripe before retrying",
            )?;
            Ok(Invoice::from(invoice))
        }
        .map_err(move |error: anyhow::Error| error.context(context))
    });
    let finalize_results = stream::iter(finalize_futs)
        .buffer_unordered(10)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .filter(|res: &anyhow::Result<Invoice>| {
            if let Err(e) = res {
                tracing::error!(error = %format!("{e:#}"), "Failed to process invoice");
                return false;
            }
            true
        })
        .map(Result::unwrap)
        .collect::<Vec<_>>();

    pb.finish_with_message("Finished finalizing invoices");

    Ok(finalize_results)
}

async fn check_and_fix_auto_advance(
    stripe_client: &Client,
    invoices: &[Invoice],
) -> anyhow::Result<usize> {
    // Find invoices with auto_advance turned off
    let needs_auto_advance_fix: Vec<Invoice> = invoices
        .iter()
        .filter(|inv| {
            inv.auto_advance.map_or(false, |aa| !aa)
                && matches!(inv.status(), Some(stripe::InvoiceStatus::Open))
        })
        .cloned()
        .collect();

    if needs_auto_advance_fix.is_empty() {
        return Ok(0);
    }

    // Show table of invoices that need auto_advance fixed
    let table_rows: Vec<_> = needs_auto_advance_fix
        .iter()
        .map(|inv| {
            let mut row = inv.to_table_row();
            row.push(
                comfy_table::Cell::new("auto_advance: false => true")
                    .fg(comfy_table::Color::Yellow)
                    .add_attribute(comfy_table::Attribute::Bold),
            );
            row
        })
        .collect();

    if !table_rows.is_empty() {
        let mut table = comfy_table::Table::new();
        table
            .load_preset(comfy_table::presets::UTF8_FULL)
            .apply_modifier(comfy_table::modifiers::UTF8_ROUND_CORNERS)
            .apply_modifier(comfy_table::modifiers::UTF8_SOLID_INNER_BORDERS);

        let mut header = Invoice::table_header();
        header.push("Auto Advance Update");
        table.set_header(header);

        for row in table_rows {
            table.add_row(row);
        }

        println!("\nThe following open invoices have auto_advance turned off and will be updated:");
        println!("{}", table);

        prompt_to_continue(
            "Enter Y to turn on auto_advance for these invoices, or anything else to skip: ",
        )
        .await?;

        // Update auto_advance to true
        return update_auto_advance(stripe_client, needs_auto_advance_fix).await;
    }

    Ok(0)
}

async fn update_auto_advance(
    stripe_client: &Client,
    invoices: Vec<Invoice>,
) -> anyhow::Result<usize> {
    #[derive(serde::Serialize)]
    struct UpdateAutoAdvance {
        auto_advance: bool,
    }

    let pb = ProgressBar::new(invoices.len() as u64);
    pb.set_message("updating auto_advance");
    pb.set_style(ProgressStyle::with_template(PROGRESS_BAR_TEMPLATE).unwrap());

    let mut failures = 0;
    for inv in invoices {
        let res: anyhow::Result<stripe::Invoice> = async {
            let current = Invoice::from(
                StripeInvoice::retrieve(stripe_client, inv.id(), &["customer"]).await?
            );
            anyhow::ensure!(
                current.status() == Some(stripe::InvoiceStatus::Open),
                "invoice is no longer open"
            );
            anyhow::ensure!(
                !collection_method_needs_update(&current)
                    .context("Open invoice requires explicit correction in Stripe")?,
                "open invoice needs collection-method correction in Stripe; leaving auto_advance disabled"
            );
            stripe_client.post_form(
                &format!("/invoices/{}", inv.id()),
                UpdateAutoAdvance { auto_advance: true },
            ).await.context("Enabling invoice collection")
        }.await;

        match res {
            Ok(_) => {
                pb.println(format!(
                    "Updated auto_advance for invoice {} (tenant: {})",
                    inv.id(),
                    inv.tenant()
                ));
            }
            Err(e) => {
                failures += 1;
                tracing::error!(
                    invoice = %inv.id(),
                    tenant = %inv.tenant(),
                    error = %format!("{e:#}"),
                    "Failed to update auto_advance for invoice"
                );
            }
        }
        pb.inc(1);
    }

    pb.finish_with_message("Auto-advance updates complete");
    Ok(failures)
}

fn build_invoice_table<I>(rows: I, subtotal: Option<f64>) -> comfy_table::Table
where
    I: IntoIterator<Item = Vec<comfy_table::Cell>>,
{
    let mut table = comfy_table::Table::new();
    table
        .load_preset(comfy_table::presets::UTF8_FULL)
        .apply_modifier(comfy_table::modifiers::UTF8_ROUND_CORNERS)
        .apply_modifier(comfy_table::modifiers::UTF8_SOLID_INNER_BORDERS);
    table.set_header(Invoice::table_header());
    for row in rows {
        table.add_row(row);
    }
    if let Some(subtotal) = subtotal {
        let subtotal_int = subtotal.trunc() as i64;
        let subtotal_cents = (subtotal.fract() * 100.0).round() as u8;
        let formatted_subtotal = format!(
            "${}.{:02}",
            subtotal_int.to_formatted_string(&Locale::en),
            subtotal_cents
        );
        table.add_row(vec![
            comfy_table::Cell::new("Subtotal").add_attribute(comfy_table::Attribute::Bold),
            comfy_table::Cell::new(formatted_subtotal).add_attribute(comfy_table::Attribute::Bold),
            comfy_table::Cell::new(""),
            comfy_table::Cell::new(""),
            comfy_table::Cell::new(""),
            comfy_table::Cell::new(""),
        ]);
    }
    table
}

fn print_invoice_table(title: &str, rows: &[Invoice]) {
    let subtotal: f64 = rows.iter().map(|r| r.amount()).sum();
    let table = build_invoice_table(
        rows.iter().map(|row| {
            let cells = row.to_table_row();
            if row.collection_method().map_or(false, |cm| {
                cm == stripe::CollectionMethod::ChargeAutomatically
            }) && !row.has_cc()
            {
                let mut red_cells = cells
                    .into_iter()
                    .map(|cell| cell.fg(comfy_table::Color::Red))
                    .collect::<Vec<_>>();

                red_cells[4] = comfy_table::Cell::new("!! Missing default payment method !!")
                    .fg(comfy_table::Color::Red);
                red_cells
            } else {
                cells
            }
        }),
        Some(subtotal),
    );
    println!("\n{title}:");
    println!("{}", table);
}

async fn prompt_to_continue(message: &str) -> anyhow::Result<()> {
    let message = message.to_string();
    let proceed = tokio::task::spawn_blocking(move || {
        println!("\n{}", message);
        let mut buf = String::with_capacity(8);
        match std::io::stdin().read_line(&mut buf) {
            Ok(_) => buf.trim().eq_ignore_ascii_case("y"),
            Err(err) => {
                tracing::error!(error = %err, "failed to read from stdin");
                false
            }
        }
    })
    .await
    .expect("failed to join spawned task");
    if proceed {
        Ok(())
    } else {
        Err(anyhow::anyhow!("Aborted by user."))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn invoice(manual: bool, has_payment_method: bool) -> Invoice {
        Invoice::from(stripe::Invoice {
            id: "in_test".parse().unwrap(),
            status: Some(stripe::InvoiceStatus::Draft),
            collection_method: Some(stripe::CollectionMethod::ChargeAutomatically),
            customer: Some(stripe::Expandable::Object(Box::new(stripe::Customer {
                id: "cus_test".parse().unwrap(),
                invoice_settings: Some(stripe::InvoiceSettingCustomerSetting {
                    default_payment_method: has_payment_method
                        .then(|| stripe::Expandable::Id("pm_test".parse().unwrap())),
                    ..Default::default()
                }),
                ..Default::default()
            }))),
            metadata: Some(
                billing_types::InvoiceMetadata {
                    tenant: "acmeCo/".to_string(),
                    invoice_type: if manual {
                        InvoiceType::Manual
                    } else {
                        InvoiceType::Final
                    },
                    period_start: "2026-08-01".to_string(),
                    period_end: "2026-08-31".to_string(),
                }
                .to_metadata_map(),
            ),
            ..Default::default()
        })
    }

    #[test]
    fn collection_policy() {
        for has_payment_method in [false, true] {
            let mut manual = invoice(true, has_payment_method);
            assert!(
                collection_method_needs_update(&manual)
                    .unwrap_err()
                    .to_string()
                    .contains("manual invoice")
            );
            manual.collection_method = Some(stripe::CollectionMethod::SendInvoice);
            assert!(!collection_method_needs_update(&manual).unwrap());

            let mut usage = invoice(false, has_payment_method);
            assert_eq!(
                collection_method_needs_update(&usage).unwrap(),
                !has_payment_method
            );
            usage.collection_method = Some(stripe::CollectionMethod::SendInvoice);
            assert!(!collection_method_needs_update(&usage).unwrap());
            usage.collection_method = None;
            assert!(collection_method_needs_update(&usage).is_err());
        }
    }

    async fn stripe_stub(
        responses: Vec<(axum::http::StatusCode, serde_json::Value)>,
    ) -> (
        stripe::Client,
        std::sync::Arc<std::sync::Mutex<Vec<String>>>,
    ) {
        let requests = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let recorded = requests.clone();
        let responses = std::sync::Arc::new(std::sync::Mutex::new(responses.into_iter()));
        let app = axum::Router::new().fallback(move |method: axum::http::Method, uri: axum::http::Uri| {
            let recorded = recorded.clone();
            let responses = responses.clone();
            async move {
                recorded.lock().unwrap().push(format!("{method} {}", uri.path()));
                let (status, body) = responses.lock().unwrap().next().unwrap_or((
                    axum::http::StatusCode::BAD_REQUEST,
                    serde_json::json!({"error": {"type": "invalid_request_error", "message": "unexpected request"}}),
                ));
                (status, axum::Json(body))
            }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap());
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (stripe::Client::from_url(url.as_str(), "test"), requests)
    }

    #[tokio::test]
    async fn failed_conversion_is_excluded_from_finalization() {
        let mut successful = invoice(false, false);
        successful.id = "in_success".parse().unwrap();
        let mut converted = successful.clone();
        converted.collection_method = Some(stripe::CollectionMethod::SendInvoice);
        let mut finalized = converted.clone();
        finalized.status = Some(stripe::InvoiceStatus::Open);
        let (client, requests) = stripe_stub(vec![
            (axum::http::StatusCode::BAD_REQUEST, serde_json::json!({"error": {"type": "invalid_request_error", "message": "update failed"}})),
            (axum::http::StatusCode::OK, serde_json::to_value(&*converted).unwrap()),
            (axum::http::StatusCode::OK, serde_json::to_value(&*converted).unwrap()),
            (axum::http::StatusCode::OK, serde_json::to_value(&*finalized).unwrap()),
            (axum::http::StatusCode::OK, serde_json::to_value(&*finalized).unwrap()),
        ]).await;
        let updated = update_collection_methods(&client, vec![invoice(false, false), successful])
            .await
            .unwrap();
        assert_eq!(updated.len(), 1);
        let finalized = finalize_invoices(&client, updated).await.unwrap();
        assert_eq!(finalized.len(), 1);
        assert_eq!(
            *requests.lock().unwrap(),
            [
                "POST /v1/invoices/in_test",
                "POST /v1/invoices/in_success",
                "GET /v1/invoices/in_success",
                "POST /v1/invoices/in_success/finalize",
                "GET /v1/invoices/in_success",
            ]
        );
    }

    #[tokio::test]
    async fn payment_method_removed_after_confirmation_prevents_finalization() {
        let (client, requests) = stripe_stub(vec![(
            axum::http::StatusCode::OK,
            serde_json::to_value(&*invoice(false, false)).unwrap(),
        )])
        .await;
        assert!(
            finalize_invoices(&client, vec![invoice(false, true)])
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(*requests.lock().unwrap(), ["GET /v1/invoices/in_test"]);
    }

    #[tokio::test]
    async fn only_valid_open_invoices_can_enable_collection() {
        for (manual, has_payment_method, valid) in [
            (false, false, false),
            (true, true, false),
            (false, true, true),
        ] {
            let mut current = invoice(manual, has_payment_method);
            current.status = Some(stripe::InvoiceStatus::Open);
            let response = (
                axum::http::StatusCode::OK,
                serde_json::to_value(&*current).unwrap(),
            );
            let (client, requests) = stripe_stub(vec![response.clone(), response]).await;
            // The second response is used only when collection is allowed.
            let expected = if valid {
                vec!["GET /v1/invoices/in_test", "POST /v1/invoices/in_test"]
            } else {
                vec!["GET /v1/invoices/in_test"]
            };
            let failures = update_auto_advance(&client, vec![current]).await.unwrap();
            assert_eq!(failures, usize::from(!valid));
            assert_eq!(*requests.lock().unwrap(), expected);
        }
    }
}
