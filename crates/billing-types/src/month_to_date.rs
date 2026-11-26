use crate::calculate::{Adjustment, BillingTerms, DailyUsage, calculate, month_range};
use chrono::Datelike as _;

/// A tenant's charges at the end of each day of a month.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MonthToDate {
    /// `Invoice::threshold_charges` of the usage so far.
    pub charges: i64,
    /// The month's invoice subtotal, if each remaining day matches the
    /// average daily usage so far.
    pub projected_subtotal: i64,
    /// One entry for each day of the month.
    pub days: Vec<DayCharges>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DayCharges {
    pub date: chrono::NaiveDate,
    /// `Invoice::threshold_charges` of the usage through the end of `date`.
    pub charges: i64,
    /// Whether `date` is after today, so that its usage is projected.
    pub projected: bool,
}

/// Calculate the charges at the end of each day of the month containing
/// `now`. Each day after today is given the average daily usage between the
/// start of the month's first day with usage and `now`. The rest of today is
/// not projected.
pub(crate) fn month_to_date(
    terms: &BillingTerms,
    daily_usage: &[DailyUsage],
    adjustments: &[Adjustment],
    now: chrono::DateTime<chrono::Utc>,
) -> anyhow::Result<MonthToDate> {
    let today = now.date_naive();
    let (year, month) = (
        today.year(),
        chrono::Month::try_from(today.month() as u8).expect("month is in 1..=12"),
    );
    let period = month_range(year, month)?;

    let actual = daily_usage.iter().filter(|day| day.date <= today);
    let (bytes_read, bytes_written, usage_seconds) =
        actual
            .clone()
            .fold((0u128, 0u128, 0u128), |(read, written, seconds), day| {
                (
                    read + u128::from(day.bytes_read),
                    written + u128::from(day.bytes_written),
                    seconds + u128::from(day.usage_seconds),
                )
            });
    // Starting from the first day with usage keeps a tenant that began using
    // Estuary mid-month from being averaged over days it didn't exist, and
    // counting partial days keeps the first hours of a day from being averaged
    // as a whole day.
    let elapsed_days = actual.clone().next().map(|first| {
        (now - first.date.and_hms_opt(0, 0, 0).unwrap().and_utc()).num_seconds() as f64 / 86_400.0
    });
    let average = |total: u128| match elapsed_days {
        Some(days) if days > 0.0 => (total as f64 / days) as u64,
        _ => 0,
    };
    let projected = today
        .iter_days()
        .skip(1)
        .take_while(|date| *date < period.end)
        .map(|date| DailyUsage {
            date,
            bytes_read: average(bytes_read),
            bytes_written: average(bytes_written),
            usage_seconds: average(usage_seconds),
        });
    let usage: Vec<DailyUsage> = actual.cloned().chain(projected).collect();

    let mut days = Vec::new();
    let mut projected_subtotal = 0;
    for date in period
        .start
        .iter_days()
        .take_while(|date| *date < period.end)
    {
        let through = usage.partition_point(|day| day.date <= date);
        let invoice = calculate(year, month, terms, &usage[..through], adjustments)?;
        days.push(DayCharges {
            date,
            charges: invoice.threshold_charges(),
            projected: date > today,
        });
        // The last day's invoice covers the whole month.
        projected_subtotal = invoice.subtotal;
    }
    let charges = days[today.day0() as usize].charges;

    Ok(MonthToDate {
        charges,
        projected_subtotal,
        days,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn date(day: u32) -> chrono::NaiveDate {
        chrono::NaiveDate::from_ymd_opt(2026, 11, day).unwrap()
    }

    fn noon(day: u32) -> chrono::DateTime<chrono::Utc> {
        date(day).and_hms_opt(12, 0, 0).unwrap().and_utc()
    }

    fn usage(day: u32, gb: u64, hours: u64) -> DailyUsage {
        DailyUsage {
            date: date(day),
            bytes_read: gb * 1_000_000_000,
            bytes_written: 0,
            usage_seconds: hours * 3600,
        }
    }

    // A self-serve tenant whose trial ended long ago pays for all usage.
    fn terms(trial_start: Option<chrono::NaiveDate>) -> BillingTerms {
        BillingTerms {
            tenant: "acmeCo/".to_string(),
            plan_kind: crate::PlanKind::SelfServe,
            data_tiers: vec![50],
            usage_tiers: vec![14, 100, 7],
            recurring_usd_cents: 0,
            trial_start,
        }
    }

    fn paid() -> BillingTerms {
        terms(Some(date(1).with_year(2025).unwrap()))
    }

    fn charges(month: &MonthToDate) -> Vec<(u32, i64, bool)> {
        month
            .days
            .iter()
            .map(|day| (day.date.day(), day.charges, day.projected))
            .collect()
    }

    #[test]
    fn projects_remaining_days_at_the_average_through_tiers() {
        // 25 GB and 50 hours by noon on the 3rd average 10 GB and 20 hours a
        // day. Day 6 crosses the 100-hour tier, so later days add $5.00 of
        // data and $1.40 of task usage each.
        let month = month_to_date(
            &paid(),
            &[usage(1, 10, 20), usage(2, 10, 20), usage(3, 5, 10)],
            &[Adjustment {
                detail: Some("Credit".to_string()),
                usd_cents: -100,
            }],
            noon(3),
        )
        .unwrap();

        assert_eq!(month.charges, 1250 + 700 - 100);
        assert_eq!(
            charges(&month)[..7],
            [
                (1, 500 + 280 - 100, false),
                (2, 1000 + 560 - 100, false),
                (3, 1250 + 700 - 100, false),
                (4, 1750 + 980 - 100, true),
                (5, 2250 + 1260 - 100, true),
                (6, 2750 + 1470 - 100, true),
                (7, 3250 + 1610 - 100, true),
            ]
        );
        assert_eq!(month.days.len(), 30);
        // 295 GB and 590 hours: $147.50 of data, then $14.00 for the first
        // 100 hours and $34.30 for the other 490.
        assert_eq!(month.projected_subtotal, 14750 + 1400 + 3430 - 100);
        assert_eq!(month.days[29].charges, month.projected_subtotal);
    }

    #[test]
    fn averages_from_the_first_day_with_usage() {
        // A tenant that started on the 20th has moved 150 GB by noon on the
        // 21st, or 100 GB a day, so the other 9 days add 900 GB.
        let month = month_to_date(
            &paid(),
            &[usage(20, 100, 0), usage(21, 50, 0)],
            &[],
            noon(21),
        )
        .unwrap();

        assert_eq!(month.charges, 150 * 50);
        assert_eq!(month.projected_subtotal, 1050 * 50);
    }

    #[test]
    fn credits_projected_days_of_a_trial() {
        // The trial ends after November 10, so only the projected days after
        // it are charged, at 2 GB a day.
        let month = month_to_date(
            &terms(Some(date(11).with_month(10).unwrap())),
            &[usage(1, 2, 0), usage(2, 1, 0)],
            &[],
            noon(2),
        )
        .unwrap();

        assert_eq!(month.charges, 0);
        assert_eq!(
            charges(&month)[9..12],
            [(10, 0, true), (11, 100, true), (12, 200, true)]
        );
        assert_eq!(month.projected_subtotal, 20 * 100);
    }

    #[test]
    fn a_month_without_usage_keeps_its_fixed_charges() {
        let month = month_to_date(
            &BillingTerms {
                plan_kind: crate::PlanKind::Contract,
                recurring_usd_cents: 250_000,
                ..terms(None)
            },
            &[],
            &[],
            noon(29),
        )
        .unwrap();

        assert_eq!(month.charges, 250_000);
        assert_eq!(month.projected_subtotal, 250_000);
        assert!(month.days[29].projected);
    }
}
