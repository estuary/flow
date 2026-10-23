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

#[derive(Debug, Clone, SimpleObject)]
pub struct SelfServePlan {
    pub phase: SelfServePhase,
    pub trial: Option<TrialPeriod>,
    pub collection_method: CollectionMethod,
    /// Data processing prices, per GB.
    pub data_tiers: Vec<PricingTier>,
    /// Task usage prices, per task hour.
    pub usage_tiers: Vec<PricingTier>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Enum)]
pub enum SelfServePhase {
    /// Before the trial starts, or when there is no trial.
    Free,
    /// During the trial.
    Trial,
    /// After the trial ends.
    Paid,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct TrialPeriod {
    pub start: NaiveDate,
    /// Exclusive end of the trial.
    pub end: NaiveDate,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct ContractPlan {
    pub collection_method: CollectionMethod,
    pub payment_provider: PaymentProvider,
    /// Data processing prices, per GB.
    pub data_tiers: Vec<PricingTier>,
    /// Task usage prices, per task hour.
    pub usage_tiers: Vec<PricingTier>,
}

/// A price for the next `quantity` units of usage after the previous tiers.
#[derive(Debug, Clone, PartialEq, Eq, SimpleObject)]
pub struct PricingTier {
    /// Price per unit, in cents.
    pub rate_cents: i32,
    /// Units priced at this rate. Null for the final tier, which covers all further usage.
    pub quantity: Option<i32>,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct InternalPlan {
    pub detail: Option<String>,
}

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
                data_tiers: pricing_tiers(&row.data_tiers),
                usage_tiers: pricing_tiers(&row.usage_tiers),
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
                // The tenants CHECK constraint rejects these rows on every write, so only a migration that relaxes it can reach this arm.
                Some(billing_types::PaymentProvider::External) | None => unreachable!(
                    "tenants constraint requires contracts to have a payment provider other than external"
                ),
            };
            BillingPlan::Contract(ContractPlan {
                collection_method: row.collection_method,
                payment_provider,
                data_tiers: pricing_tiers(&row.data_tiers),
                usage_tiers: pricing_tiers(&row.usage_tiers),
            })
        }
        PlanKind::Internal => BillingPlan::Internal(InternalPlan { detail: None }),
    }
}

/// Splits a `[rate, quantity, rate, ..., rate]` tier array into tiers. The final rate has no quantity.
fn pricing_tiers(tiers: &[i32]) -> Vec<PricingTier> {
    tiers
        .chunks(2)
        .map(|tier| PricingTier {
            rate_cents: tier[0],
            quantity: tier.get(1).copied(),
        })
        .collect()
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
    fn self_serve_phases() {
        let today = date("2026-10-14");
        let phase = |start: &str, end: &str| {
            let row = PlanRow {
                trial_start: Some(date(start)),
                trial_end: Some(date(end)),
                ..row(PlanKind::SelfServe)
            };
            let BillingPlan::SelfServe(plan) = billing_plan(row, today) else {
                unreachable!()
            };
            plan.phase
        };
        insta::assert_debug_snapshot!([
            ("future trial", phase("2026-11-01", "2026-12-01")),
            ("trial starting today", phase("2026-10-14", "2026-11-14")),
            ("trial ending today", phase("2026-09-14", "2026-10-14")),
        ]);
    }

    #[test]
    fn contract_plans() {
        let cases = [
            ("contract paid through Stripe", row(PlanKind::Contract)),
            (
                "contract on GCP marketplace, paid by sent invoice",
                PlanRow {
                    collection_method: CollectionMethod::SendInvoice,
                    payment_provider: Some(billing_types::PaymentProvider::GcpMarketplace),
                    data_tiers: vec![0, 1000, 50],
                    usage_tiers: vec![0, 720, 14, 4464, 7],
                    ..row(PlanKind::Contract)
                },
            ),
            (
                "contract on Snowflake marketplace",
                PlanRow {
                    payment_provider: Some(billing_types::PaymentProvider::SnowflakeMarketplace),
                    ..row(PlanKind::Contract)
                },
            ),
        ];

        let plans: Vec<(&str, BillingPlan)> = cases
            .into_iter()
            .map(|(name, row)| (name, billing_plan(row, date("2026-10-14"))))
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
        // A second tenant, so that the steps below fail if the plan query ignores `WHERE tenant = $1`.
        provision_test_tenant(&pool, "billingplanother").await;
        let (server, token) = start_server_and_token(&pool, user_id, tenant, mock_provider()).await;

        let query = json!({
            "query": r#"{
                tenant(name: "billingplan/") {
                    billing {
                        plan {
                            __typename
                            ... on SelfServePlan {
                                phase
                                trial { start end }
                                collectionMethod
                                dataTiers { rateCents quantity }
                                usageTiers { rateCents quantity }
                            }
                            ... on ContractPlan {
                                collectionMethod
                                paymentProvider
                                dataTiers { rateCents quantity }
                                usageTiers { rateCents quantity }
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
