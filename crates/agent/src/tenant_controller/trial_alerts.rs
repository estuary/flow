use super::{Outcome, TaskStatus, Tenant, free_trial, retry_backoff};
use crate::alerts::{AlertViewRow, evaluate_alert_actions};
use anyhow::Context;
use control_plane_api::{alerts::ArgsObject, billing};
use models::status::AlertType;

/// `free_trial_ending` fires this many days before the trial ends.
const TRIAL_ENDING_DAYS: i64 = 5;
/// `free_trial_stalled` fires this many days after the trial ends.
const TRIAL_STALLED_DAYS: i64 = 5;

/// Fires and resolves the tenant's alerts to match its state as of `now`.
/// Each evaluation derives every alert from current state, so an evaluation
/// that's early, late or repeated changes nothing.
///
/// `customer` is the tenant's Stripe customer as retrieved by this poll. Without
/// a billing provider, alerts that need payment-method state are left unchanged.
pub async fn evaluate(
    status: &mut TaskStatus,
    pool: &sqlx::PgPool,
    tenant: &Tenant,
    customer: Option<&anyhow::Result<Option<stripe::Customer>>>,
    now: chrono::DateTime<chrono::Utc>,
) -> anyhow::Result<Outcome> {
    match do_evaluate(pool, tenant, customer, now).await {
        Ok(()) => {
            *status = TaskStatus::default();
            Ok(match next_evaluation(tenant, now) {
                Some(at) => Outcome::Sleep((at - now).to_std().unwrap_or_default()),
                None => Outcome::Idle,
            })
        }
        Err(err) => {
            status.failures += 1;
            let backoff = retry_backoff(status.failures);
            status.last_error = Some(format!("{err:#}"));
            tracing::warn!(
                tenant = %tenant.tenant,
                failures = status.failures,
                ?backoff,
                "tenant alert evaluation failed: {err:#}",
            );
            Ok(Outcome::Sleep(backoff))
        }
    }
}

async fn do_evaluate(
    pool: &sqlx::PgPool,
    tenant: &Tenant,
    customer: Option<&anyhow::Result<Option<stripe::Customer>>>,
    now: chrono::DateTime<chrono::Utc>,
) -> anyhow::Result<()> {
    let names = AlertType::all()
        .iter()
        .filter(|alert_type| alert_type.is_tenant_alert())
        .map(|alert_type| catalog_name(&tenant.tenant, *alert_type))
        .collect::<Vec<_>>();
    let names = names.iter().map(String::as_str).collect::<Vec<_>>();
    // A live spec could share one of these names, so its alerts are excluded.
    let open_alerts: Vec<_> =
        control_plane_api::alerts::fetch_open_alerts_by_catalog_name(&names, pool)
            .await
            .context("fetching open alerts")?
            .into_iter()
            .filter(|alert| alert.alert_type.is_tenant_alert())
            .collect();

    // These alerts' emails are written for self-serve tenants that pay by card,
    // and their resolution emails tell the tenant it added one. A tenant that
    // needs no payment method gets none of them, and its open alerts close
    // without notifications.
    if !tenant.needs_payment_method() {
        if open_alerts.is_empty() {
            return Ok(());
        }
        let ids = open_alerts.iter().map(|alert| alert.id).collect::<Vec<_>>();
        let mut conn = pool.acquire().await?;
        control_plane_api::alerts::resolve_alerts_silently(ids, &mut conn)
            .await
            .context("closing alerts of a tenant that needs no payment method")?;
        tracing::info!(tenant = %tenant.tenant, count = open_alerts.len(), "closed alerts of a tenant that needs no payment method");
        return Ok(());
    }

    let has_payment_info = match customer {
        Some(Ok(customer)) => customer
            .as_ref()
            .is_some_and(|customer| billing::default_payment_method_id(customer).is_some()),
        Some(Err(err)) => anyhow::bail!("{err:#}"),
        None => return Ok(()),
    };

    let rows = alert_rows(&tenant.tenant, tenant.trial_start, has_payment_info, now);
    let actions = evaluate_alert_actions(
        now,
        &mut crate::id_generator::with_random_shard(),
        open_alerts,
        rows,
    );
    if actions.is_empty() {
        return Ok(());
    }

    let mut txn = pool.begin().await?;
    control_plane_api::alerts::apply_alert_actions(actions, &mut txn)
        .await
        .context("applying alert actions")?;
    txn.commit().await.context("committing alert actions")?;
    Ok(())
}

fn catalog_name(tenant: &str, alert_type: AlertType) -> String {
    format!("{tenant}alerts/{alert_type}")
}

fn midnight(date: chrono::NaiveDate) -> chrono::DateTime<chrono::Utc> {
    date.and_time(chrono::NaiveTime::MIN).and_utc()
}

/// The tenant's alerts as of `now`. `free_trial` and `missing_payment_method`
/// are included while not firing, so that their resolution notifications render
/// the tenant's state as of the resolution. `free_trial_ending` and
/// `free_trial_stalled` are included only while firing, and resolve with the
/// arguments they fired with.
fn alert_rows(
    tenant: &str,
    trial_start: Option<chrono::NaiveDate>,
    has_payment_info: bool,
    now: chrono::DateTime<chrono::Utc>,
) -> Vec<AlertViewRow> {
    let trial_end = trial_start.map(billing_types::trial_end);

    let (trial, ending, stalled) = match trial_start.zip(trial_end) {
        None => (false, false, false),
        Some((start, end)) => {
            let (start, end) = (midnight(start), midnight(end));
            (
                start <= now && now < end,
                end - chrono::Duration::days(TRIAL_ENDING_DAYS) <= now && now < end,
                end + chrono::Duration::days(TRIAL_STALLED_DAYS) <= now && !has_payment_info,
            )
        }
    };
    let plan_state = match trial_end {
        None => "free_tier",
        Some(end) if now < midnight(end) => "free_trial",
        Some(_) => "paid",
    };

    let arguments: ArgsObject = [
        ("tenant", serde_json::json!(tenant)),
        ("trial_start", serde_json::json!(trial_start)),
        // A timestamp at midnight, which notifications render as is.
        (
            "trial_end",
            serde_json::json!(trial_end.map(|end| end.and_time(chrono::NaiveTime::MIN))),
        ),
        ("has_credit_card", serde_json::json!(has_payment_info)),
        ("plan_state", serde_json::json!(plan_state)),
    ]
    .into_iter()
    .map(|(key, value)| (key.to_string(), value))
    .collect();

    let row = |alert_type, firing| AlertViewRow {
        catalog_name: catalog_name(tenant, alert_type),
        alert_type,
        base_arguments: arguments.clone(),
        firing,
    };
    let mut rows = vec![
        row(AlertType::FreeTrial, trial),
        row(AlertType::MissingPaymentMethod, !has_payment_info),
    ];
    if ending {
        rows.push(row(AlertType::FreeTrialEnding, true));
    }
    if stalled {
        rows.push(row(AlertType::FreeTrialStalled, true));
    }
    rows
}

/// When the tenant's alerts are next evaluated: at the next boundary of its
/// trial, or at its daily check time, since its payment method may be added or
/// removed at any time. A tenant that needs no payment method has no alerts.
fn next_evaluation(
    tenant: &Tenant,
    now: chrono::DateTime<chrono::Utc>,
) -> Option<chrono::DateTime<chrono::Utc>> {
    if !tenant.needs_payment_method() {
        return None;
    }
    let boundaries = tenant.trial_start.into_iter().flat_map(|start| {
        let end = midnight(billing_types::trial_end(start));
        [
            midnight(start),
            end - chrono::Duration::days(TRIAL_ENDING_DAYS),
            end,
            end + chrono::Duration::days(TRIAL_STALLED_DAYS),
        ]
    });
    let daily = free_trial::next_check_after(&tenant.tenant, now);

    boundaries.filter(|at| *at > now).chain([daily]).min()
}

#[cfg(test)]
mod tests {
    use super::super::{CollectionMethod, PaymentProvider, PlanKind};
    use super::*;
    use chrono::TimeZone as _;
    use control_plane_api::billing::{BillingProvider, InMemoryBillingProvider};

    const TENANT: &str = "acmeCo/";

    fn at(month: u32, day: u32, hms: (u32, u32, u32)) -> chrono::DateTime<chrono::Utc> {
        chrono::Utc
            .with_ymd_and_hms(2026, month, day, hms.0, hms.1, hms.2)
            .unwrap()
    }

    fn tenant(plan_kind: PlanKind, trial_start: Option<chrono::NaiveDate>) -> Tenant {
        Tenant {
            tenant: TENANT.to_string(),
            billing_emails: Vec::new(),
            billing_name: None,
            billing_address: None,
            payment_provider: (plan_kind == PlanKind::Contract).then_some(PaymentProvider::Stripe),
            plan_kind,
            collection_method: CollectionMethod::ChargeDefaultPaymentMethod,
            trial_start,
        }
    }

    #[test]
    fn alerts_over_a_trial() {
        // The trial ends one calendar month later, on February 28.
        let start = chrono::NaiveDate::from_ymd_opt(2026, 1, 31).unwrap();
        let end_of_day = (23, 59, 59);
        let cases = [
            ("no trial", None, false, at(1, 31, (12, 0, 0))),
            ("trial starts", Some(start), false, at(1, 31, (0, 0, 0))),
            ("before ending", Some(start), false, at(2, 22, end_of_day)),
            ("ending", Some(start), false, at(2, 23, (0, 0, 0))),
            ("last day", Some(start), true, at(2, 27, end_of_day)),
            ("trial ends", Some(start), false, at(2, 28, (0, 0, 0))),
            ("before stalled", Some(start), false, at(3, 4, end_of_day)),
            ("stalled", Some(start), false, at(3, 5, (0, 0, 0))),
            ("paid", Some(start), true, at(3, 5, (0, 0, 0))),
        ];

        let results = cases
            .into_iter()
            .map(|(case, trial_start, has_payment_info, now)| {
                let rows = alert_rows(TENANT, trial_start, has_payment_info, now);
                let names = |firing: bool| {
                    rows.iter()
                        .filter(|row| row.firing == firing)
                        .map(|row| row.catalog_name.as_str())
                        .collect::<Vec<_>>()
                };
                serde_json::json!({
                    "case": case,
                    "firing": names(true),
                    "notFiring": names(false),
                    "arguments": rows[0].base_arguments,
                })
            })
            .collect::<Vec<_>>();

        insta::assert_json_snapshot!(results);
    }

    #[test]
    fn evaluations_follow_trial_boundaries_and_daily_checks() {
        let start = chrono::NaiveDate::from_ymd_opt(2026, 1, 31).unwrap();
        let mut send_invoice = tenant(PlanKind::SelfServe, Some(start));
        send_invoice.collection_method = CollectionMethod::SendInvoice;
        let mut external = tenant(PlanKind::SelfServe, None);
        external.payment_provider = Some(PaymentProvider::External);

        // A second before each trial boundary, the boundary comes before the
        // daily check.
        let before = |month, day| at(month, day, (0, 0, 0)) - chrono::Duration::seconds(1);
        let in_trial = || tenant(PlanKind::SelfServe, Some(start));
        let cases = [
            (
                "self-serve without a trial",
                tenant(PlanKind::SelfServe, None),
                at(1, 1, (0, 0, 0)),
            ),
            ("before the trial starts", in_trial(), before(1, 31)),
            ("before the trial ends in 5 days", in_trial(), before(2, 23)),
            ("before the trial ends", in_trial(), before(2, 28)),
            ("before the trial stalls", in_trial(), before(3, 5)),
            ("after the trial", in_trial(), at(3, 5, (0, 0, 0))),
            (
                "contract during the trial",
                tenant(PlanKind::Contract, Some(start)),
                before(2, 28),
            ),
            (
                "internal during the trial",
                tenant(PlanKind::Internal, Some(start)),
                before(2, 28),
            ),
            ("external without a trial", external, at(1, 1, (0, 0, 0))),
            ("send_invoice during the trial", send_invoice, before(2, 28)),
        ];

        let results = cases
            .into_iter()
            .map(|(case, tenant, now)| {
                let next = next_evaluation(&tenant, now);
                serde_json::json!({
                    "case": case,
                    "next": next,
                    "isDailyCheck": next == Some(free_trial::next_check_after(TENANT, now)),
                })
            })
            .collect::<Vec<_>>();

        insta::assert_json_snapshot!(results);
    }

    #[sqlx::test(migrations = "../../supabase/migrations")]
    async fn card_state_and_plan_decide_alerts(pool: sqlx::PgPool) {
        // Mirror data must not resolve alerts when live payment-method state is unavailable.
        sqlx::raw_sql(
            r#"
            insert into tenants (tenant, stripe_customer_id) values ('acmeCo/', 'cus_acme');
            insert into stripe.customers (id, "invoice_settings/default_payment_method", flow_document)
            values ('cus_acme', 'pm_mirrored', '{}');
            "#,
        )
        .execute(&pool)
        .await
        .unwrap();

        // Alerts fired before tenant controllers evaluated them, and an alert
        // of a live spec that happens to share a tenant alert's name.
        let mut id_gen = models::IdGenerator::new(1);
        let mut fire = |alert_type| {
            control_plane_api::alerts::AlertAction::Fire(control_plane_api::alerts::FireAlert {
                id: id_gen.next(),
                catalog_name: catalog_name(TENANT, AlertType::MissingPaymentMethod),
                alert_type,
                fired_at: at(1, 1, (0, 0, 0)),
                base_arguments: Default::default(),
            })
        };
        let actions = vec![
            fire(AlertType::MissingPaymentMethod),
            fire(AlertType::ShardFailed),
        ];
        let mut conn = pool.acquire().await.unwrap();
        control_plane_api::alerts::apply_alert_actions(actions, &mut conn)
            .await
            .unwrap();
        drop(conn);

        let provider = InMemoryBillingProvider::new();
        provider.add_customer(TENANT, "cus_acme", Some("pm_live"));
        let live_with_card = provider
            .retrieve_customer(&"cus_acme".parse().unwrap())
            .await
            .map(Some);
        let live_without_card = provider
            .update_customer_default_payment_method(&"cus_acme".parse().unwrap(), None)
            .await
            .map(Some);
        let failed_retrieval = Err(anyhow::anyhow!("Stripe is unavailable"));

        let self_serve = tenant(PlanKind::SelfServe, None);
        let contract = tenant(PlanKind::Contract, None);
        let mut status = TaskStatus::default();
        let now = at(1, 2, (0, 0, 0));
        let mut results = Vec::new();
        for (case, tenant, customer) in [
            (
                "without a billing provider, leaves alerts unchanged",
                &self_serve,
                None,
            ),
            (
                "live customer with a card",
                &self_serve,
                Some(&live_with_card),
            ),
            (
                "live customer without a card",
                &self_serve,
                Some(&live_without_card),
            ),
            (
                "failed retrieval changes nothing",
                &self_serve,
                Some(&failed_retrieval),
            ),
            (
                "successful retrieval clears the failure",
                &self_serve,
                Some(&live_without_card),
            ),
            ("contract closes its alerts silently", &contract, None),
        ] {
            let outcome = evaluate(&mut status, &pool, tenant, customer, now)
                .await
                .unwrap();

            let open: Vec<(String, String)> = sqlx::query_as(
                "select alert_type::text, catalog_name::text from alert_history where resolved_at is null order by alert_type::text",
            )
            .fetch_all(&pool)
            .await
            .unwrap();
            // Each alert's notification task sends its emails.
            let notification_tasks: i64 = sqlx::query_scalar(
                "select count(*) from internal.tasks t join alert_history a on a.id = t.task_id",
            )
            .fetch_one(&pool)
            .await
            .unwrap();
            results.push(serde_json::json!({
                "case": case,
                "status": status,
                "waitSeconds": outcome.wait().map(|duration| duration.as_secs()),
                "open": open,
                "notificationTasks": notification_tasks,
            }));
        }

        insta::assert_json_snapshot!(results);
    }
}
