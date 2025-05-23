use crate::{
    publish::{BILLING_PERIOD_START_KEY, INVOICE_TYPE_KEY},
    stripe_utils::{fetch_invoices, Invoice},
};
use chrono::{Duration, NaiveDate, Utc};
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
/// Send all invoices for a specific billing period, charging cards or sending for payment.
pub struct SendInvoices {
    /// Stripe API key.
    #[clap(long)]
    pub stripe_api_key: String,
    /// The month to send invoices for, in format "YYYY-MM-DD"
    #[clap(long)]
    pub month: NaiveDate,
}

pub async fn do_send_invoices(cmd: &SendInvoices) -> anyhow::Result<()> {
    let stripe_client = Client::new(cmd.stripe_api_key.to_owned())
        .with_strategy(stripe::RequestStrategy::ExponentialBackoff(4));
    let month_start = cmd.month.format("%Y-%m-%d").to_string();
    let month_human_repr = cmd.month.format("%B %Y");
    tracing::info!("Fetching Stripe invoices to send for {month_human_repr}");

    let base_metadata = format!(
        "metadata[\"{INVOICE_TYPE_KEY}\"]:'final' AND metadata[\"{BILLING_PERIOD_START_KEY}\"]:'{month_start}'"
    );
    let draft_query = format!("status:'draft' AND {base_metadata}");
    let open_query = format!("status:'open' AND {base_metadata}");

    // 1. Fetch any draft invoices. We will endeavor to finalize them so that they can be sent
    let (mut draft_invoices, mut finalized_invoices) = futures::try_join!(
        fetch_invoices(&stripe_client, &draft_query),
        fetch_invoices(&stripe_client, &open_query)
    )?;

    tracing::info!(
        "Fetched {} draft invoices for {month_human_repr}.",
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

    // 3. Send or charge open invoices
    print_invoice_table("Invoices to send", &finalized_invoices);
    prompt_to_continue("Enter Y to send these invoices, or anything else to abort: ").await?;

    // We still could theoretically have `open` invoices that are `charge_automatically`
    // but don't have a default payment method. There's not much we can do about these
    // since `open` invoices are finalized and cannot be updated. So we show them in the table
    // but filter them out before the collection step as they'll throw an error if we try.
    collect_invoices(
        &stripe_client,
        finalized_invoices
            .into_iter()
            .filter(|inv| {
                if inv.collection_method().map_or(false, |cm| {
                    cm == stripe::CollectionMethod::ChargeAutomatically
                }) && !inv.has_cc()
                {
                    return false;
                } else {
                    return true;
                }
            })
            .collect::<Vec<_>>(),
    )
    .await;
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
            StripeInvoice::finalize(stripe_client, row.id(), FinalizeInvoiceParams::default())
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

async fn collect_invoices(stripe_client: &Client, to_send: Vec<Invoice>) {
    let pb = ProgressBar::new(to_send.len() as u64);
    pb.set_message("sending invoices");
    pb.set_style(ProgressStyle::with_template(PROGRESS_BAR_TEMPLATE).unwrap());
    let send_futs = to_send.into_iter().map(|row| {
        let stripe_client = stripe_client;
        let pb = pb.clone();
        async move {
            let res = if row.has_cc() {
                StripeInvoice::pay(stripe_client, row.id()).await
            } else {
                stripe_client
                    .post(&format!("/invoices/{}/send", row.id()))
                    .await
            };
            pb.inc(1);
            match res {
                Ok(_) => {
                    if row.has_cc() {
                        pb.println(format!(
                            "Charged card for tenant {} (invoice {})",
                            row.tenant(),
                            row.id()
                        ));
                    } else {
                        pb.println(format!(
                            "Sent invoice for tenant {} (invoice {})",
                            row.tenant(),
                            row.id()
                        ));
                    }
                }
                Err(e) => {
                    pb.println(format!(
                        "Error sending/paying invoice {}: {:?}",
                        row.id(),
                        e
                    ));
                }
            }
        }
    });
    stream::iter(send_futs)
        .buffer_unordered(10)
        .collect::<Vec<_>>()
        .await;
    pb.finish_with_message("All invoices sent/paid");
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
