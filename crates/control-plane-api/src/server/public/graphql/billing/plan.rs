use async_graphql::{Enum, SimpleObject, Union};
use billing_types::{CollectionMethod, PlanKind};
use chrono::NaiveDate;

/// The billing arrangement of a tenant.
#[derive(Debug, Clone, Union)]
pub enum BillingPlan {
    SelfServe(SelfServePlan),
    Contract(ContractPlan),
    Internal(InternalPlan),
}

/// A tenant billed for its usage, which moves from the free tier through a
/// free trial to paid usage.
#[derive(Debug, Clone, SimpleObject)]
pub struct SelfServePlan {
    /// Derived from the trial dates. Adding a payment method during an active
    /// trial keeps the trial.
    pub phase: SelfServePhase,
    /// The tenant's free trial, if one is scheduled or has started.
    pub trial: Option<TrialPeriod>,
    /// Applies to every invoice. SEND_INVOICE doesn't require a payment method.
    pub collection_method: CollectionMethod,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Enum)]
pub enum SelfServePhase {
    /// Usage is credited by the free tier.
    Free,
    /// Usage is credited by the free trial.
    Trial,
    /// Usage is billed.
    Paid,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct TrialPeriod {
    pub start: NaiveDate,
    /// Exclusive end of the trial, one calendar month after its start.
    pub end: NaiveDate,
}

/// A tenant billed under a contract, including fixed plans purchased through
/// a marketplace.
#[derive(Debug, Clone, SimpleObject)]
pub struct ContractPlan {
    pub included_usage: IncludedUsage,
    /// Applies to usage overage invoices. Contract fee invoices are always sent.
    pub collection_method: CollectionMethod,
    pub payment_provider: PaymentProvider,
}

/// Monthly usage included by the contract before overage pricing applies.
#[derive(Debug, Clone, PartialEq, Eq, SimpleObject)]
pub struct IncludedUsage {
    /// Null if all data processing is included.
    pub data_gb: Option<i32>,
    /// Null if all task usage is included.
    pub task_hours: Option<i32>,
}

/// A tenant that Estuary doesn't bill.
#[derive(Debug, Clone, SimpleObject)]
pub struct InternalPlan {
    pub detail: Option<String>,
}

/// Who bills a contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Enum)]
pub enum PaymentProvider {
    Stripe,
    AwsMarketplace,
    GcpMarketplace,
    SnowflakeMarketplace,
}

pub struct PlanRow {
    pub plan_kind: PlanKind,
    pub collection_method: CollectionMethod,
    pub payment_provider: Option<billing_types::PaymentProvider>,
    pub trial_start: Option<NaiveDate>,
    pub trial_end: Option<NaiveDate>,
    pub data_tiers: Vec<i32>,
    pub usage_tiers: Vec<i32>,
}

pub async fn fetch_billing_plan(pool: &sqlx::PgPool, tenant: &str) -> anyhow::Result<BillingPlan> {
    // The trial end uses Postgres date arithmetic so that it matches the trial
    // credit range of `internal.billing_report_202308`.
    let row = sqlx::query_as!(
        PlanRow,
        r#"
        SELECT
            plan_kind AS "plan_kind: PlanKind",
            collection_method AS "collection_method: CollectionMethod",
            payment_provider AS "payment_provider: billing_types::PaymentProvider",
            trial_start,
            (trial_start + INTERVAL '1 month')::date AS trial_end,
            data_tiers,
            usage_tiers
        FROM tenants
        WHERE tenant = $1
        "#,
        tenant,
    )
    .fetch_one(pool)
    .await?;

    Ok(billing_plan(row, chrono::Utc::now().date_naive()))
}

pub fn billing_plan(row: PlanRow, today: NaiveDate) -> BillingPlan {
    match row.plan_kind {
        PlanKind::SelfServe => {
            let trial = row
                .trial_start
                .zip(row.trial_end)
                .map(|(start, end)| TrialPeriod { start, end });
            let phase = match &trial {
                Some(trial) if today >= trial.end => SelfServePhase::Paid,
                Some(trial) if today >= trial.start => SelfServePhase::Trial,
                _ => SelfServePhase::Free,
            };
            BillingPlan::SelfServe(SelfServePlan {
                phase,
                trial,
                collection_method: row.collection_method,
            })
        }
        PlanKind::Contract => {
            let payment_provider = match row.payment_provider {
                Some(billing_types::PaymentProvider::Stripe) => PaymentProvider::Stripe,
                Some(billing_types::PaymentProvider::AwsMarketplace) => {
                    PaymentProvider::AwsMarketplace
                }
                Some(billing_types::PaymentProvider::GcpMarketplace) => {
                    PaymentProvider::GcpMarketplace
                }
                Some(billing_types::PaymentProvider::SnowflakeMarketplace) => {
                    PaymentProvider::SnowflakeMarketplace
                }
                Some(billing_types::PaymentProvider::External) | None => unreachable!(
                    "tenants constraint requires contracts to have a classified payment provider"
                ),
            };
            BillingPlan::Contract(ContractPlan {
                included_usage: IncludedUsage {
                    data_gb: included_quantity(&row.data_tiers),
                    task_hours: included_quantity(&row.usage_tiers),
                },
                collection_method: row.collection_method,
                payment_provider,
            })
        }
        PlanKind::Internal => BillingPlan::Internal(InternalPlan { detail: None }),
    }
}

/// Sums the leading zero-priced tiers of a `[rate, quantity, rate, ..., rate]`
/// tier array, or returns None if the final, unbounded tier is also zero-priced.
fn included_quantity(tiers: &[i32]) -> Option<i32> {
    let mut included = 0;
    for tier in tiers.chunks(2) {
        match tier {
            [0, quantity] => included += quantity,
            [0] => return None,
            _ => break,
        }
    }
    Some(included)
}

#[cfg(test)]
mod tests {
    use super::super::test_util::*;
    use super::*;
    use crate::test_server;
    use serde_json::json;

    fn row(plan_kind: PlanKind) -> PlanRow {
        PlanRow {
            plan_kind,
            collection_method: CollectionMethod::ChargeDefaultPaymentMethod,
            payment_provider: Some(billing_types::PaymentProvider::Stripe),
            trial_start: None,
            trial_end: None,
            data_tiers: vec![50],
            usage_tiers: vec![14, 4464, 7],
        }
    }

    fn date(s: &str) -> NaiveDate {
        s.parse().unwrap()
    }

    #[test]
    fn billing_plan_cases() {
        let today = date("2026-10-14");
        let trial = |start: &str, end: &str| PlanRow {
            trial_start: Some(date(start)),
            trial_end: Some(date(end)),
            ..row(PlanKind::SelfServe)
        };

        let cases = [
            ("self-serve without a trial", row(PlanKind::SelfServe)),
            (
                "self-serve with a future trial",
                trial("2026-11-01", "2026-12-01"),
            ),
            (
                "self-serve trial starting today",
                trial("2026-10-14", "2026-11-14"),
            ),
            (
                "self-serve trial ending today",
                trial("2026-09-14", "2026-10-14"),
            ),
            (
                "self-serve paid by sent invoice",
                PlanRow {
                    collection_method: CollectionMethod::SendInvoice,
                    ..trial("2025-01-31", "2025-02-28")
                },
            ),
            (
                "contract with included usage",
                PlanRow {
                    collection_method: CollectionMethod::SendInvoice,
                    payment_provider: Some(billing_types::PaymentProvider::GcpMarketplace),
                    data_tiers: vec![0, 1000, 50],
                    usage_tiers: vec![0, 720, 0, 720, 14, 4464, 7],
                    ..row(PlanKind::Contract)
                },
            ),
            (
                "contract with unlimited data and a trial_start",
                PlanRow {
                    data_tiers: vec![0],
                    trial_start: Some(date("2026-10-01")),
                    trial_end: Some(date("2026-11-01")),
                    ..row(PlanKind::Contract)
                },
            ),
            ("internal", row(PlanKind::Internal)),
        ];

        let plans: Vec<(&str, BillingPlan)> = cases
            .into_iter()
            .map(|(name, row)| (name, billing_plan(row, today)))
            .collect();
        insta::assert_debug_snapshot!(plans);
    }

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../../../fixtures", scripts("data_planes"))
    )]
    async fn graphql_billing_plan(pool: sqlx::PgPool) {
        let _guard = test_server::init();
        let tenant = "billingplan";
        let user_id = provision_test_tenant(&pool, tenant).await;
        let (server, token) = start_server_and_token(&pool, user_id, tenant, mock_provider()).await;

        let query = json!({
            "query": r#"{
                tenant(name: "billingplan/") {
                    billing {
                        plan {
                            __typename
                            ... on SelfServePlan { phase trial { start end } collectionMethod }
                            ... on ContractPlan {
                                includedUsage { dataGb taskHours }
                                collectionMethod
                                paymentProvider
                            }
                            ... on InternalPlan { detail }
                        }
                    }
                }
            }"#
        });
        // A trial that ended long ago keeps the phase stable over time, and
        // starting it on the 31st exercises end-of-month clamping.
        let steps = [
            "select 'newly provisioned'",
            "update tenants set trial_start = '2024-01-31', collection_method = 'send_invoice' where tenant = 'billingplan/'",
            "update tenants set plan_kind = 'contract', payment_provider = 'aws_marketplace', data_tiers = '{0,1024,50}', usage_tiers = '{0,1440,14}' where tenant = 'billingplan/'",
            "update tenants set plan_kind = 'internal' where tenant = 'billingplan/'",
        ];
        let mut plans = Vec::new();
        for step in steps {
            sqlx::query(step).execute(&pool).await.unwrap();
            let response: serde_json::Value = server.graphql(&query, Some(&token)).await;
            plans.push(json!({"step": step, "response": response}));
        }
        insta::assert_json_snapshot!(plans);
    }
}
