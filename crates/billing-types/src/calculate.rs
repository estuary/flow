use anyhow::Context;

#[derive(Debug, Clone)]
pub(crate) struct BillingTerms {
    pub tenant: String,
    pub data_tiers: Vec<i32>,
    pub usage_tiers: Vec<i32>,
    pub recurring_usd_cents: i32,
    pub trial_start: Option<chrono::NaiveDate>,
}

#[derive(Debug, Clone)]
pub(crate) struct DailyUsage {
    pub date: chrono::NaiveDate,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub usage_seconds: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct Adjustment {
    pub detail: Option<String>,
    pub usd_cents: i32,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct LineItem {
    pub description: Option<String>,
    pub count: i64,
    pub rate: i64,
    pub subtotal: i64,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct InvoiceUsage {
    pub ts: String,
    pub data_gb: f64,
    pub data_subtotal: i64,
    pub task_hours: f64,
    pub task_subtotal: i64,
}

/// A tenant's invoice calculation: usage, line items, credits, and subtotal.
#[derive(Debug, Clone, serde::Serialize)]
pub struct Invoice {
    pub billed_month: String,
    pub billed_prefix: String,
    pub daily_usage: Vec<InvoiceUsage>,
    pub line_items: Vec<LineItem>,
    pub processed_data_gb: f64,
    pub recurring_fee: i64,
    pub subtotal: i64,
    pub task_usage_hours: f64,
    pub trial_credit: i64,
    pub free_tier_credit: i64,
    pub trial_start: Option<chrono::NaiveDate>,
}

struct PricedDay {
    date: chrono::NaiveDate,
    bytes: u128,
    seconds: u128,
    data_subtotal: i64,
    task_subtotal: i64,
}

#[derive(Default)]
struct PricingTotals {
    bytes: u128,
    seconds: u128,
    data_total: i64,
    task_total: i64,
}

/// Calculate a tenant's invoice line items and subtotal for one UTC calendar month.
/// `daily_usage` holds at most one row per day of the month, in date order.
pub(crate) fn calculate(
    year: i32,
    month: chrono::Month,
    terms: &BillingTerms,
    daily_usage: &[DailyUsage],
    adjustments: &[Adjustment],
) -> anyhow::Result<Invoice> {
    let period = month_range(year, month)?;
    let trial = terms.trial_start.map(|start| {
        let end = start
            .checked_add_months(chrono::Months::new(1))
            .expect("stored trial start must allow a representable trial end");
        start..end
    });
    let data_tiers = decode_tiers(&terms.data_tiers, "Data processing")?;
    let usage_tiers = decode_tiers(&terms.usage_tiers, "Task usage")?;

    let mut totals = PricingTotals::default();
    let priced_days = daily_usage
        .iter()
        .scan(&mut totals, |totals, day| {
            Some(
                price_day(totals, day, &data_tiers, &usage_tiers).map(|(next, priced_day)| {
                    **totals = next;
                    priced_day
                }),
            )
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let PricingTotals { bytes, seconds, .. } = totals;

    // Credits can span multiple tiers, so their sum stays in i64 until it
    // offsets the usage charges in the invoice.
    let daily_charge = |day: &PricedDay| day.data_subtotal + day.task_subtotal;
    let trial_credit: i64 = priced_days
        .iter()
        .filter(|day| {
            trial
                .as_ref()
                .is_some_and(|range| range.contains(&day.date))
        })
        .map(daily_charge)
        .sum();
    let free_tier_credit: i64 = priced_days
        .iter()
        .filter(|day| terms.trial_start.is_none_or(|start| day.date < start))
        .map(daily_charge)
        .sum();

    let recurring = (terms.recurring_usd_cents != 0).then(|| {
        single_item(
            Some("Recurring service charge".to_string()),
            i64::from(terms.recurring_usd_cents),
        )
    });
    let data_items = tier_items(
        allocate_tiers(rounded_units(bytes, 1_000_000_000)?, &data_tiers),
        "Data processing",
        "GB",
    )
    .collect::<anyhow::Result<Vec<_>>>()?;
    let task_items = tier_items(
        allocate_tiers(rounded_units(seconds, 3600)?, &usage_tiers),
        "Task usage",
        "hour",
    )
    .collect::<anyhow::Result<Vec<_>>>()?;
    let trial_item = trial
        .as_ref()
        .filter(|range| range.start < period.end && range.end > period.start)
        .map(|range| {
            let last_day = range.end.pred_opt().unwrap();
            single_item(
                Some(format!("Free trial credit ({} - {last_day})", range.start)),
                -trial_credit,
            )
        });
    let free_tier_item = terms
        .trial_start
        .is_none_or(|start| start > period.start)
        .then(|| {
            let description = match terms.trial_start {
                Some(start) => format!("Free tier credit ending {}", start.pred_opt().unwrap()),
                None => "Free tier credit".to_string(),
            };
            single_item(Some(description), -free_tier_credit)
        });
    let line_items: Vec<_> = recurring
        .into_iter()
        .chain(data_items)
        .chain(task_items)
        .chain(
            adjustments
                .iter()
                .map(|a| single_item(a.detail.clone(), i64::from(a.usd_cents))),
        )
        .chain(trial_item)
        .chain(free_tier_item)
        .collect();
    let subtotal: i64 = line_items.iter().map(|item| item.subtotal).sum();
    // GraphQL serves invoice subtotals as a 32-bit `Int`.
    i32::try_from(subtotal).context("invoice subtotal exceeds i32")?;
    Ok(Invoice {
        billed_month: timestamp(period.start),
        billed_prefix: terms.tenant.clone(),
        daily_usage: priced_days
            .into_iter()
            .map(|day| InvoiceUsage {
                ts: timestamp(day.date),
                data_gb: day.bytes as f64 / 1_000_000_000.0,
                data_subtotal: day.data_subtotal,
                task_hours: day.seconds as f64 / 3600.0,
                task_subtotal: day.task_subtotal,
            })
            .collect(),
        line_items,
        processed_data_gb: bytes as f64 / 1_000_000_000.0,
        recurring_fee: i64::from(terms.recurring_usd_cents),
        subtotal,
        task_usage_hours: seconds as f64 / 3600.0,
        trial_credit,
        free_tier_credit,
        trial_start: terms.trial_start,
    })
}

pub(crate) fn month_range(
    year: i32,
    month: chrono::Month,
) -> anyhow::Result<std::ops::Range<chrono::NaiveDate>> {
    let start = chrono::NaiveDate::from_ymd_opt(year, month.number_from_month(), 1)
        .context("billing year is out of range")?;
    let end = start
        .checked_add_months(chrono::Months::new(1))
        .context("billing month is out of range")?;
    Ok(start..end)
}

fn price_day(
    previous: &PricingTotals,
    day: &DailyUsage,
    data_tiers: &[Tier],
    usage_tiers: &[Tier],
) -> anyhow::Result<(PricingTotals, PricedDay)> {
    let bytes = u128::from(day.bytes_read) + u128::from(day.bytes_written);
    let seconds = u128::from(day.usage_seconds);
    let total_bytes = previous.bytes + bytes;
    let total_seconds = previous.seconds + seconds;
    let data_total = allocate_tiers(rounded_units(total_bytes, 1_000_000_000)?, data_tiers)
        .map(|tier| tier.map(|tier| tier.subtotal))
        .sum::<anyhow::Result<i64>>()?;
    let task_total = allocate_tiers(rounded_units(total_seconds, 3600)?, usage_tiers)
        .map(|tier| tier.map(|tier| tier.subtotal))
        .sum::<anyhow::Result<i64>>()?;
    Ok((
        PricingTotals {
            bytes: total_bytes,
            seconds: total_seconds,
            data_total,
            task_total,
        },
        PricedDay {
            date: day.date,
            bytes,
            seconds,
            data_subtotal: data_total - previous.data_total,
            task_subtotal: task_total - previous.task_total,
        },
    ))
}

fn timestamp(date: chrono::NaiveDate) -> String {
    format!("{date}T00:00:00+00:00")
}

// Units and tier subtotals are limited to i32, so the products and sums built
// from them stay within i64.
fn rounded_units(value: u128, unit: u128) -> anyhow::Result<i64> {
    let rounded = i32::try_from(value.div_ceil(unit)).context("billable units exceed i32")?;
    Ok(i64::from(rounded))
}

fn single_item(description: Option<String>, cents: i64) -> LineItem {
    LineItem {
        description,
        count: 1,
        rate: cents,
        subtotal: cents,
    }
}

// The database stores alternating rates and widths, with a final unbounded rate.
struct Tier {
    rate: i64,
    width: Option<i64>,
}

struct TierAllocation {
    rate: i64,
    width: Option<i64>,
    count: i64,
    subtotal: i64,
}

fn decode_tiers(raw: &[i32], name: &str) -> anyhow::Result<Vec<Tier>> {
    anyhow::ensure!(raw.len() % 2 == 1, "invalid {name} tiers");
    Ok(raw
        .chunks(2)
        .map(|tier| Tier {
            rate: i64::from(tier[0]),
            width: tier.get(1).copied().map(i64::from),
        })
        .collect())
}

fn allocate_tiers(
    amount: i64,
    tiers: &[Tier],
) -> impl Iterator<Item = anyhow::Result<TierAllocation>> {
    tiers.iter().scan(amount, |remaining, tier| {
        let count = tier
            .width
            .map_or(*remaining, |width| (*remaining).min(width));
        *remaining -= count;
        // A negative stored width raises `remaining`, so a later count can
        // exceed i32 and the product can overflow i64.
        Some(
            count
                .checked_mul(tier.rate)
                .filter(|subtotal| i32::try_from(*subtotal).is_ok())
                .context("tier subtotal exceeds i32")
                .map(|subtotal| TierAllocation {
                    rate: tier.rate,
                    width: tier.width,
                    count,
                    subtotal,
                }),
        )
    })
}

fn tier_items(
    tiers: impl Iterator<Item = anyhow::Result<TierAllocation>>,
    name: &str,
    unit: &str,
) -> impl Iterator<Item = anyhow::Result<LineItem>> {
    tiers.enumerate().map(move |(idx, tier)| {
        let tier = tier?;
        let rate = tier.rate;
        let sign = if rate < 0 { "-" } else { "" };
        let dollars = format!("{sign}${}.{:02}", rate.abs() / 100, rate.abs() % 100);
        let description = match tier.width {
            None => format!("{name} (at {dollars}/{unit})"),
            Some(pivot) if idx == 0 => {
                format!("{name} (first {pivot} {unit}s at {dollars}/{unit})")
            }
            Some(pivot) => format!("{name} (next {pivot} {unit}s at {dollars}/{unit})"),
        };
        Ok(LineItem {
            description: Some(description),
            count: tier.count,
            rate,
            subtotal: tier.subtotal,
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn date(day: u32) -> chrono::NaiveDate {
        chrono::NaiveDate::from_ymd_opt(2022, 8, day).unwrap()
    }

    fn tenant() -> BillingTerms {
        BillingTerms {
            tenant: "acmeCo/".to_string(),
            data_tiers: vec![50, 5, 20],
            usage_tiers: vec![15],
            recurring_usd_cents: 1000,
            trial_start: Some(date(15)),
        }
    }

    #[test]
    fn prices_cumulative_usage_and_credits_each_period() {
        let usage = [
            DailyUsage {
                date: date(1),
                bytes_read: 18_125_000_000,
                bytes_written: 0,
                usage_seconds: 720 * 3600,
            },
            DailyUsage {
                date: date(30),
                bytes_read: 2_000_000_000,
                bytes_written: 1_000_000_000,
                usage_seconds: 0,
            },
        ];
        let invoice = calculate(
            2022,
            chrono::Month::August,
            &tenant(),
            &usage,
            &[Adjustment {
                detail: Some("Credit".to_string()),
                usd_cents: -250,
            }],
        )
        .unwrap();
        assert_eq!(
            serde_json::to_value(&invoice).unwrap(),
            serde_json::json!({
                "billed_month": "2022-08-01T00:00:00+00:00",
                "billed_prefix": "acmeCo/",
                "daily_usage": [
                    { "ts": "2022-08-01T00:00:00+00:00", "data_gb": 18.125, "data_subtotal": 530, "task_hours": 720.0, "task_subtotal": 10800 },
                    { "ts": "2022-08-30T00:00:00+00:00", "data_gb": 3.0, "data_subtotal": 60, "task_hours": 0.0, "task_subtotal": 0 }
                ],
                "line_items": [
                    { "description": "Recurring service charge", "count": 1, "rate": 1000, "subtotal": 1000 },
                    { "description": "Data processing (first 5 GBs at $0.50/GB)", "count": 5, "rate": 50, "subtotal": 250 },
                    { "description": "Data processing (at $0.20/GB)", "count": 17, "rate": 20, "subtotal": 340 },
                    { "description": "Task usage (at $0.15/hour)", "count": 720, "rate": 15, "subtotal": 10800 },
                    { "description": "Credit", "count": 1, "rate": -250, "subtotal": -250 },
                    { "description": "Free trial credit (2022-08-15 - 2022-09-14)", "count": 1, "rate": -60, "subtotal": -60 },
                    { "description": "Free tier credit ending 2022-08-14", "count": 1, "rate": -11330, "subtotal": -11330 }
                ],
                "processed_data_gb": 21.125, "recurring_fee": 1000, "subtotal": 750,
                "task_usage_hours": 720.0, "trial_credit": 60, "free_tier_credit": 11330,
                "trial_start": "2022-08-15"
            })
        );
    }

    #[test]
    fn empty_usage_keeps_charges_and_nullable_adjustment() {
        let invoice = calculate(
            2022,
            chrono::Month::August,
            &tenant(),
            &[],
            &[Adjustment {
                detail: None,
                usd_cents: 125,
            }],
        )
        .unwrap();
        assert!(invoice.daily_usage.is_empty());
        assert_eq!(invoice.subtotal, 1125);
        assert_eq!(invoice.line_items[4].description, None);
        assert_eq!(
            invoice
                .line_items
                .iter()
                .filter(|item| item.count == 0)
                .count(),
            3
        );
    }

    #[test]
    fn tier_crossing_and_trial_end_in_leap_february() {
        let mut tenant = tenant();
        tenant.trial_start = Some(chrono::NaiveDate::from_ymd_opt(2024, 1, 31).unwrap());
        tenant.data_tiers = vec![100, 5, 50];
        tenant.recurring_usd_cents = 0;
        let usage: Vec<_> = (27..=29)
            .map(|day| DailyUsage {
                date: chrono::NaiveDate::from_ymd_opt(2024, 2, day).unwrap(),
                bytes_read: 3_000_000_000,
                bytes_written: 0,
                usage_seconds: 0,
            })
            .collect();
        let invoice = calculate(2024, chrono::Month::February, &tenant, &usage, &[]).unwrap();
        assert_eq!(
            invoice
                .daily_usage
                .iter()
                .map(|day| day.data_subtotal)
                .collect::<Vec<_>>(),
            vec![300, 250, 150]
        );
        assert_eq!(invoice.subtotal, 150);
        assert_eq!(
            serde_json::to_value(&invoice.line_items).unwrap(),
            serde_json::json!([
                { "description": "Data processing (first 5 GBs at $1.00/GB)", "count": 5, "rate": 100, "subtotal": 500 },
                { "description": "Data processing (at $0.50/GB)", "count": 4, "rate": 50, "subtotal": 200 },
                { "description": "Task usage (at $0.15/hour)", "count": 0, "rate": 15, "subtotal": 0 },
                { "description": "Free trial credit (2024-01-31 - 2024-02-28)", "count": 1, "rate": -550, "subtotal": -550 }
            ])
        );
    }

    #[test]
    fn month_bounds_cross_years_and_reject_unrepresentable_years() {
        use chrono::Datelike;

        let period = month_range(2026, chrono::Month::December).unwrap();
        assert_eq!(period.start.to_string(), "2026-12-01");
        assert_eq!(period.end.to_string(), "2027-01-01");
        assert!(month_range(i32::MAX, chrono::Month::January).is_err());
        assert!(month_range(chrono::NaiveDate::MAX.year(), chrono::Month::December).is_err());
    }

    #[test]
    fn cumulative_rounding_assigns_charges_at_credit_boundaries() {
        let cases = [
            // Free tier ends before August 2, when the trial starts.
            (
                chrono::NaiveDate::from_ymd_opt(2022, 8, 2).unwrap(),
                2400,
                [15, 15, 0],
                15,
                15,
            ),
            // A trial beginning July 3 ends before August 3.
            (
                chrono::NaiveDate::from_ymd_opt(2022, 7, 3).unwrap(),
                1800,
                [15, 0, 15],
                0,
                15,
            ),
        ];
        for (trial_start, seconds, expected_charges, expected_free, expected_trial) in cases {
            let mut terms = tenant();
            terms.trial_start = Some(trial_start);
            let usage: Vec<_> = (1..=3)
                .map(|day| DailyUsage {
                    date: date(day),
                    bytes_read: 0,
                    bytes_written: 0,
                    usage_seconds: seconds,
                })
                .collect();
            let invoice = calculate(2022, chrono::Month::August, &terms, &usage, &[]).unwrap();
            assert_eq!(
                invoice
                    .daily_usage
                    .iter()
                    .map(|day| day.task_subtotal)
                    .collect::<Vec<_>>(),
                expected_charges
            );
            assert_eq!(
                (invoice.free_tier_credit, invoice.trial_credit),
                (expected_free, expected_trial)
            );
        }
    }

    #[test]
    fn billing_numeric_limits_apply_at_units_tiers_and_final_subtotal() {
        let usage = |bytes_read| {
            [DailyUsage {
                date: date(1),
                bytes_read,
                bytes_written: 0,
                usage_seconds: 0,
            }]
        };
        let mut terms = tenant();
        terms.data_tiers = vec![0];
        let error = calculate(
            2022,
            chrono::Month::August,
            &terms,
            &usage(i32::MAX as u64 * 1_000_000_000 + 1),
            &[],
        )
        .unwrap_err();
        assert!(error.to_string().contains("billable units exceed i32"));

        terms.data_tiers = vec![i32::MAX];
        let error = calculate(
            2022,
            chrono::Month::August,
            &terms,
            &usage(2_000_000_000),
            &[],
        )
        .unwrap_err();
        assert!(error.to_string().contains("tier subtotal exceeds i32"));

        terms.data_tiers = vec![0];
        terms.recurring_usd_cents = i32::MAX;
        let adjustment = Adjustment {
            detail: None,
            usd_cents: 1,
        };
        let error = calculate(2022, chrono::Month::August, &terms, &[], &[adjustment]).unwrap_err();
        assert!(error.to_string().contains("invoice subtotal exceeds i32"));
    }

    #[test]
    fn later_tier_errors_propagate_through_totals_and_line_items() {
        let tiers = decode_tiers(&[50, 1, i32::MAX], "Data processing").unwrap();
        let expected = "tier subtotal exceeds i32";

        let error = allocate_tiers(3, &tiers)
            .map(|tier| tier.map(|tier| tier.subtotal))
            .sum::<anyhow::Result<i64>>()
            .unwrap_err();
        assert_eq!(error.to_string(), expected);

        let error = tier_items(allocate_tiers(3, &tiers), "Data processing", "GB")
            .collect::<anyhow::Result<Vec<_>>>()
            .unwrap_err();
        assert_eq!(error.to_string(), expected);
    }

    #[test]
    fn credit_can_offset_multiple_valid_tiers_above_i32_total() {
        let mut terms = tenant();
        terms.data_tiers = vec![1_000_000_000, 2, 1_000_000_000];
        terms.recurring_usd_cents = 0;
        terms.trial_start = Some(date(1));
        let usage = [DailyUsage {
            date: date(1),
            bytes_read: 4_000_000_000,
            bytes_written: 0,
            usage_seconds: 0,
        }];
        let invoice = calculate(2022, chrono::Month::August, &terms, &usage, &[]).unwrap();
        assert_eq!(invoice.trial_credit, 4_000_000_000);
        assert_eq!(invoice.subtotal, 0);
    }
}
