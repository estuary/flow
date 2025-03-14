begin;


CREATE or replace VIEW internal.new_free_trial_tenants AS WITH hours_by_day AS (
  SELECT
    t.tenant,
    cs.ts,
    (
      cs.usage_seconds :: numeric / 3600.0
    ) as daily_usage_hours
  FROM
    public.tenants as t
    join public.catalog_stats_daily cs on t.tenant :: text = cs.catalog_name
  WHERE
    -- We run set_new_free_trials daily, so don't bother looking at old data. 7 days is so
    -- we can tolerate up to 7 days of failures.
    cs.ts >= (now() - '7 days'::interval)
    and t.trial_start IS NULL -- Where the tenant has used more than 52.8 hours of task time in a given day.
    and (
      cs.usage_seconds :: numeric / 3600.0
    ) > 52.8
),
hours_by_month AS (
  SELECT
    t.tenant,
    cs.ts,
    cs.usage_seconds :: numeric / 3600.0 as monthly_usage_hours
  FROM
    public.tenants t
    join public.catalog_stats_monthly cs on t.tenant :: text = cs.catalog_name
  WHERE
    cs.ts >= date_trunc('month', now() AT TIME ZONE 'UTC')
    and t.trial_start IS NULL
    and (
      cs.usage_seconds :: numeric / 3600.0
    ) > (24 * 31 * 2):: numeric * 1.1
),
gbs_by_month AS (
  SELECT
    t.tenant,
    cs.ts,
    ceil(
      (
        cs.bytes_written_by_me + cs.bytes_read_by_me
      ):: numeric / (10.0 ^ 9.0)
    ) AS monthly_usage_gbs
  FROM
    public.tenants t
    join public.catalog_stats_monthly cs on t.tenant = cs.catalog_name
  WHERE
    cs.ts >= date_trunc('month', now() AT TIME ZONE 'UTC')
    and t.trial_start IS NULL
    and ceil(
      (
        cs.bytes_written_by_me + cs.bytes_read_by_me
      ):: numeric / (10.0 ^ 9.0)
    ) > 10.0
)
SELECT
  t.tenant,
  max(hours_by_day.daily_usage_hours) AS max_daily_usage_hours,
  max(
    hours_by_month.monthly_usage_hours
  ) AS max_monthly_usage_hours,
  max(gbs_by_month.monthly_usage_gbs) AS max_monthly_gb
FROM
  public.tenants t
  left join hours_by_day on t.tenant = hours_by_day.tenant
  left join hours_by_month on t.tenant = hours_by_month.tenant
  left join gbs_by_month on t.tenant = gbs_by_month.tenant
where t.trial_start is null
group by t.tenant
HAVING ((count(hours_by_month.*) > 0) OR (count(hours_by_day.*) > 0) OR (count(gbs_by_month.*) > 0));

ALTER VIEW internal.new_free_trial_tenants OWNER TO postgres;

commit;
