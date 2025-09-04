use crate::{
    publish::{BILLING_PERIOD_START_KEY, INVOICE_TYPE_KEY},
    stripe_utils::{Invoice, fetch_invoices},
};
use chrono::{Datelike, Duration, NaiveDate, Utc};
use clap::Args;
use futures::stream::{self, StreamExt};
use indicatif::{ProgressBar, ProgressStyle};
use itertools::Itertools;
use num_format::{Locale, ToFormattedString};
use std::collections::HashSet;
use stripe::{Client, FinalizeInvoiceParams, Invoice as StripeInvoice, InvoiceId};

const PROGRESS_BAR_TEMPLATE: &str = "{spinner} [{elapsed_precise}] [{bar:40}] {pos}/{len} {msg}";

#[derive(Debug, Args)]
#[clap(rename_all = "kebab-case")]
/// Process and finalize invoices for a specific billing period. Invoices with auto_advance enabled will be charged automatically by Stripe.
pub struct SendInvoices {
    /// Stripe API key.
    #[clap(long)]
    pub stripe_api_key: String,
    /// The month to send invoices for, in format "YYYY-MM-DD"
    #[clap(long)]
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

    let base_final_metadata = format!(
        "metadata[\"{INVOICE_TYPE_KEY}\"]:'final' AND metadata[\"{BILLING_PERIOD_START_KEY}\"]:'{month_start}'"
    );
    let draft_final_query = format!("status:'draft' AND {base_final_metadata}");
    let open_final_query = format!("status:'open' AND {base_final_metadata}");

    // Separate queries for manual invoices (we'll filter dates client-side)
    let draft_manual_query =
        format!("status:'draft' AND metadata[\"{INVOICE_TYPE_KEY}\"]:'manual'");
    let open_manual_query = format!("status:'open' AND metadata[\"{INVOICE_TYPE_KEY}\"]:'manual'");

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
                .filter(|inv| {
                    if let Some(period_start_str) = inv.period_start() {
                        if let Ok(period_start_date) =
                            NaiveDate::parse_from_str(&period_start_str, "%Y-%m-%d")
                        {
                            return period_start_date >= month_start_date
                                && period_start_date <= month_end_date;
                        }
                    }
                    false
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

    if !draft_invoices.is_empty() {
        // 2a. Update collection methods for any drafts that need it
        draft_invoices = update_draft_collection_methods(&stripe_client, draft_invoices).await?;

        print_invoice_table("Invoices to finalize", &draft_invoices);
        prompt_to_continue("Enter Y to finalize these invoices, or anything else to abort: ")
            .await?;

        // 2b. Move the draft invoices to the `open` state
        finalized_invoices.append(&mut finalize_invoices(&stripe_client, draft_invoices).await?);
    }

    if finalized_invoices.is_empty() {
        tracing::info!("No invoices to send for {month_human_repr}");
        return Ok(());
    }

    // 2c. Check for and fix auto_advance if flag is set
    if cmd.fix_auto_advance {
        finalized_invoices = check_and_fix_auto_advance(&stripe_client, finalized_invoices).await?;
    }

    // 3. Show final status of invoices (auto-advance will handle charging automatically)
    if !finalized_invoices.is_empty() {
        print_invoice_table("Final invoice status", &finalized_invoices);
        tracing::info!(
            "Processed {} invoices for {month_human_repr}. Invoices with auto_advance enabled will be charged automatically by Stripe.",
            finalized_invoices.len()
        );
    }
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
    // Identify invoices that are `charge_automatically` but don't have a default payment method
    let needs_update: HashSet<InvoiceId> = to_update
        .iter()
        .filter(|inv| {
            inv.collection_method().map_or(false, |cm| {
                cm == stripe::CollectionMethod::ChargeAutomatically
            }) && !inv.has_cc()
        })
        .map(|inv| inv.id().clone())
        .collect::<HashSet<_>>();

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
            "\nThe following draft invoices will updated to use the 'send_invoice' collection method:"
        );
        println!("{}", table);

        prompt_to_continue("Enter Y to update collection methods, or anything else to abort: ")
            .await?;

        let to_update = to_update
            .iter_mut()
            .filter(|inv| needs_update.contains(inv.id()))
            .collect::<Vec<_>>();
        update_collection_methods(
            stripe_client,
            to_update,
            stripe::CollectionMethod::SendInvoice,
        )
        .await?;
    }

    Ok(to_update)
}

async fn update_collection_methods(
    stripe_client: &Client,
    invoices: Vec<&mut Invoice>,
    method: stripe::CollectionMethod,
) -> anyhow::Result<()> {
    #[derive(serde::Serialize)]
    struct PostBody {
        collection_method: stripe::CollectionMethod,
        due_date: Option<i64>,
    }
    let pb = ProgressBar::new(invoices.len() as u64);
    pb.set_message("updating collection method");
    pb.set_style(ProgressStyle::with_template(PROGRESS_BAR_TEMPLATE).unwrap());
    for inv in invoices {
        let res: Result<stripe::Invoice, _> = stripe_client
            .post_form(
                &format!("/invoices/{}", inv.id()),
                PostBody {
                    collection_method: method,
                    due_date: Some((Utc::now() + Duration::days(30)).timestamp()),
                },
            )
            .await;
        match res {
            Ok(_) => {
                inv.collection_method = Some(method.clone());
            }
            Err(e) => {
                pb.println(format!(
                    "Failed to update collection method for invoice {}: {}",
                    inv.id(),
                    e
                ));
            }
        }
        pb.inc(1);
    }
    pb.finish_with_message("Collection method update complete");
    Ok(())
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
        async move {
            StripeInvoice::finalize(
                stripe_client,
                row.id(),
                FinalizeInvoiceParams {
                    auto_advance: Some(true), // Turn on auto-advance to enable automatic retries
                },
            )
            .await
            .map_err(|e| {
                pb.println(format!("Error finalizing invoice {}: {}", row.id(), e));
                anyhow::Error::from(e)
            })?;
            pb.inc(1);

            let invoice =
                StripeInvoice::retrieve(stripe_client, row.id(), vec!["customer"].as_slice())
                    .await?;
            Ok(Invoice::from(invoice))
        }
    });
    let finalize_results = stream::iter(finalize_futs)
        .buffer_unordered(10)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .filter(|res: &anyhow::Result<Invoice>| {
            if let Err(e) = res {
                tracing::error!(error = ?e, "Error finalizing invoice");
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
    invoices: Vec<Invoice>,
) -> anyhow::Result<Vec<Invoice>> {
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
        return Ok(invoices);
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
        update_auto_advance(stripe_client, needs_auto_advance_fix).await?;
    }

    Ok(invoices)
}

async fn update_auto_advance(stripe_client: &Client, invoices: Vec<Invoice>) -> anyhow::Result<()> {
    #[derive(serde::Serialize)]
    struct UpdateAutoAdvance {
        auto_advance: bool,
    }

    let pb = ProgressBar::new(invoices.len() as u64);
    pb.set_message("updating auto_advance");
    pb.set_style(ProgressStyle::with_template(PROGRESS_BAR_TEMPLATE).unwrap());

    for inv in invoices {
        let res: Result<stripe::Invoice, _> = stripe_client
            .post_form(
                &format!("/invoices/{}", inv.id()),
                UpdateAutoAdvance { auto_advance: true },
            )
            .await;

        match res {
            Ok(_) => {
                pb.println(format!(
                    "Updated auto_advance for invoice {} (tenant: {})",
                    inv.id(),
                    inv.tenant()
                ));
            }
            Err(e) => {
                pb.println(format!(
                    "Failed to update auto_advance for invoice {} (tenant: {}): {}",
                    inv.id(),
                    inv.tenant(),
                    e
                ));
            }
        }
        pb.inc(1);
    }

    pb.finish_with_message("Auto-advance updates complete");
    Ok(())
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
