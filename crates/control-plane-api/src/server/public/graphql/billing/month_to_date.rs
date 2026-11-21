use async_graphql::SimpleObject;
use chrono::NaiveDate;

/// This UTC month's charges at the end of each day. Days after today are
/// projected at the month's average daily usage so far.
#[derive(Debug, Clone, SimpleObject)]
pub struct BillingMonthToDate {
    /// Charges so far, in US cents. A billing threshold alert compares this
    /// amount with its configured amount.
    pub charges_cents: i32,
    /// This month's invoice subtotal, in US cents, if each remaining day
    /// matches the average daily usage so far.
    pub projected_subtotal_cents: i32,
    /// One entry for each day of the month.
    pub days: Vec<BillingDayCharges>,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct BillingDayCharges {
    pub date: NaiveDate,
    /// Charges through the end of this day, in US cents.
    pub charges_cents: i32,
    /// Whether this day is after today, so that its usage is projected.
    pub projected: bool,
}

impl TryFrom<billing_types::MonthToDate> for BillingMonthToDate {
    type Error = std::num::TryFromIntError;

    fn try_from(month: billing_types::MonthToDate) -> Result<Self, Self::Error> {
        Ok(Self {
            charges_cents: month.charges.try_into()?,
            projected_subtotal_cents: month.projected_subtotal.try_into()?,
            days: month
                .days
                .into_iter()
                .map(|day| {
                    Ok(BillingDayCharges {
                        date: day.date,
                        charges_cents: day.charges.try_into()?,
                        projected: day.projected,
                    })
                })
                .collect::<Result<_, Self::Error>>()?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_util::*;
    use crate::test_server;
    use chrono::Datelike as _;
    use serde_json::json;
    use std::sync::Arc;

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../../../fixtures", scripts("data_planes"))
    )]
    async fn graphql_month_to_date(pool: sqlx::PgPool) {
        let _guard = test_server::init();
        let tenant = "monthtodate";
        let user_id = provision_test_tenant(&pool, tenant).await;
        // A contract has no free tier or trial credits, so its charges are its
        // $10.00 recurring fee plus 20 GB at $0.50 per GB.
        sqlx::query(
            "update tenants set plan_kind = 'contract', recurring_usd_cents = 1000, data_tiers = '{50}' where tenant = 'monthtodate/'",
        )
        .execute(&pool)
        .await
        .unwrap();

        // The resolver reads the current month, so usage is seeded today. The
        // emulator is shared and never truncated, so the tenant's rows are
        // cleared first.
        let now = chrono::Utc::now();
        let stats = Arc::new(catalog_stats::test_util::connect_emulator("estuary-local").await);
        catalog_stats::test_util::delete_rows_with_prefix(&stats, "monthtodate/")
            .await
            .unwrap();
        let usage = ops::catalog_stats::CatalogStats {
            meta: ops::Meta {
                uuid: "00000000-0000-0000-0000-000000000000".to_string(),
            },
            catalog_name: "monthtodate/".to_string(),
            ts: now.date_naive().and_hms_opt(0, 0, 0).unwrap().and_utc(),
            stats_summary: ops::catalog_stats::StatsSummary {
                read_by_me: ops::stats::DocsAndBytes {
                    docs_total: 1,
                    bytes_total: 20_000_000_000,
                },
                ..Default::default()
            },
            task_stats: Default::default(),
        };
        catalog_stats::test_util::seed_rows(&stats, &[(catalog_stats::Grain::Daily, usage)])
            .await
            .unwrap();

        let server = test_server::TestServer::start_with_catalog_stats(
            pool.clone(),
            test_server::snapshot(pool.clone(), true).await,
            stats,
        )
        .await;
        let token = server.make_access_token(user_id, Some("monthtodate@example.test"));
        let response: serde_json::Value = server
            .graphql(
                &json!({
                    "query": r#"{
                        tenant(name: "monthtodate/") {
                            billing {
                                monthToDate {
                                    chargesCents
                                    projectedSubtotalCents
                                    days { date chargesCents projected }
                                }
                            }
                        }
                    }"#
                }),
                Some(&token),
            )
            .await;

        let month = &response["data"]["tenant"]["billing"]["monthToDate"];
        assert_eq!(month["chargesCents"], 2000, "{response:#}");
        let days = month["days"].as_array().unwrap();
        let today = now.date_naive();
        let last_day = today
            .with_day(1)
            .unwrap()
            .checked_add_months(chrono::Months::new(1))
            .unwrap()
            .pred_opt()
            .unwrap();
        assert_eq!(days.len(), last_day.day() as usize);
        assert_eq!(
            days[today.day0() as usize],
            json!({ "date": today.to_string(), "chargesCents": 2000, "projected": false }),
        );
        assert!(
            days[today.day0() as usize + 1..]
                .iter()
                .all(|day| day["projected"] == true)
        );
        assert_eq!(
            days.last().unwrap()["chargesCents"],
            month["projectedSubtotalCents"],
        );
    }
}
