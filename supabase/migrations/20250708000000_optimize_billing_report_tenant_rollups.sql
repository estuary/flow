-- Optimize billing_report_202308 to use tenant-level rollups instead of aggregating individual tasks
-- This should dramatically improve invoice generation performance

CREATE or replace FUNCTION internal.billing_report_202308(billed_prefix public.catalog_prefix, billed_month timestamp with time zone) RETURNS jsonb
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare
  -- Output variables.
  o_daily_usage       jsonb;
  o_data_gb           numeric;
  o_line_items        jsonb = '[]';
  o_recurring_fee     integer;
  o_subtotal          integer;
  o_task_hours        numeric;
  o_trial_credit      integer;
  o_free_tier_credit  integer;
  o_trial_start       date;
  o_trial_range       daterange;
  o_free_tier_range   daterange;
  o_billed_range      daterange;
begin

  -- Ensure `billed_month` is the truncated start of the billed month.
  billed_month = date_trunc('month', billed_month);

  with vars as (
    select
      t.data_tiers,
      t.trial_start,
      t.usage_tiers,
      tstzrange(billed_month, billed_month  + '1 month', '[)') as billed_range,
      case when t.trial_start is not null
        then daterange(t.trial_start::date, ((t.trial_start::date) + interval '1 month')::date, '[)')
        else 'empty' end as trial_range,
      -- In order to smoothly transition between free tier credit and free trial credit,
      -- the free tier covers all usage up to, but _not including_ the trial start date.
      -- On the trial start date, the free trial credit takes over.
      daterange(NULL, t.trial_start::date, '[)') as free_tier_range,
      -- Reveal contract costs only when computing whole-tenant billing.
      case when t.tenant = billed_prefix then t.recurring_usd_cents else 0 end as recurring_fee
      from tenants t
      where billed_prefix ^@ t.tenant -- Prefix starts with tenant.
  ),
  -- Roll up each day's incremental usage.
  daily_stat_deltas as (
    select
      ts,
      sum(bytes_written_by_me + bytes_read_by_me) / (10.0^9.0) as data_gb,
      sum(usage_seconds) / (60.0 * 60) as task_hours
    from catalog_stats, vars
      where catalog_name = billed_prefix   -- Direct lookup of tenant rollup
      and grain = 'daily'
      and billed_range @> ts
      group by ts
  ),
  -- Map to cumulative daily usage.
  -- Note sum(...) over (order by ts) yields the running sum of its aggregate.
  daily_stats as (
    select
      ts,
      sum(data_gb) over w as data_gb,
      sum(task_hours) over w as task_hours
    from daily_stat_deltas
    window w as (order by ts)
  ),
  -- Extend with line items for each category for the period ending with the given day.
  daily_line_items as (
    select
      daily_stats.*,
      internal.tier_line_items(ceil(data_gb)::integer, data_tiers, 'Data processing', 'GB') as data_line_items,
      internal.tier_line_items(ceil(task_hours)::integer, usage_tiers, 'Task usage', 'hour') as task_line_items
    from daily_stats, vars
  ),
  -- Extend with per-category subtotals for the period ending with the given day.
  daily_totals as (
    select
      daily_line_items.*,
      data_subtotal,
      task_subtotal
    from daily_line_items,
      lateral (select sum((li->>'subtotal')::numeric) as data_subtotal from jsonb_array_elements(data_line_items) li) l1,
      lateral (select sum((li->>'subtotal')::numeric) as task_subtotal from jsonb_array_elements(task_line_items) li) l2
  ),
  -- Map cumulative totals to per-day deltas.
  daily_deltas as (
    select
      ts,
      data_gb       - (coalesce(lag(data_gb,         1) over w, 0)) as data_gb,
      data_subtotal - (coalesce(lag(data_subtotal,   1) over w, 0)) as data_subtotal,
      task_hours    - (coalesce(lag(task_hours,      1) over w, 0)) as task_hours,
      task_subtotal - (coalesce(lag(task_subtotal,   1) over w, 0)) as task_subtotal
      from daily_totals
      window w as (order by ts)
  ),
  -- 1) Group daily_deltas into a JSON array
  -- 2) Sum a trial credit from daily deltas that overlap with the trial period.
  daily_array_and_trial_credits as (
    select
    jsonb_agg(jsonb_build_object(
      'ts', ts,
      'data_gb', data_gb,
      'data_subtotal', data_subtotal,
      'task_hours', task_hours,
      'task_subtotal', task_subtotal
    )) as daily_usage,
    coalesce(sum(data_subtotal + task_subtotal) filter (where trial_range @> (ts::date)),0 ) as trial_credit,
    coalesce(sum(data_subtotal + task_subtotal) filter (where free_tier_range @> (ts::date)),0 ) as free_tier_credit
    from daily_deltas, vars
  ),
  -- The last day captures the cumulative billed period.
  last_day as (
    select * from daily_line_items
    order by ts desc limit 1
  ),
  -- If we're reporting for the whole tenant then gather billing adjustment line-items.
  adjustments as (
    select coalesce(jsonb_agg(
      jsonb_build_object(
        'description', detail,
        'count', 1,
        'rate', usd_cents,
        'subtotal', usd_cents
      )
    ), '[]') as adjustment_line_items
    from internal.billing_adjustments a
    where a.tenant = billed_prefix and a.billed_month = billing_report_202308.billed_month
  )
  select into
    -- Block of variables being selected into.
    o_daily_usage,
    o_data_gb,
    o_line_items,
    o_recurring_fee,
    o_task_hours,
    o_trial_credit,
    o_trial_start,
    o_trial_range,
    o_billed_range,
    o_free_tier_credit,
    o_free_tier_range
    -- The actual selected columns.
    daily_usage,
    data_gb,
    data_line_items || task_line_items || adjustment_line_items,
    recurring_fee,
    task_hours,
    trial_credit,
    trial_start,
    trial_range,
    billed_range,
    free_tier_credit,
    free_tier_range
  from daily_array_and_trial_credits, last_day, adjustments, vars;

  -- Add line items for recurring service fee & free trial credit.
  if o_recurring_fee != 0 then
    o_line_items = jsonb_build_object(
      'description', 'Recurring service charge',
      'count', 1,
      'rate', o_recurring_fee,
      'subtotal', o_recurring_fee
    ) || o_line_items;
  end if;

  -- Display a (possibly zero) free trial credit if the trial range overlaps the billed range
  if o_trial_range && o_billed_range then
    o_line_items = o_line_items || jsonb_build_object(
      'description', format('Free trial credit (%s - %s)', lower(o_trial_range), (upper(o_trial_range) - interval '1 day')::date),
      'count', 1,
      'rate', -o_trial_credit,
      'subtotal', -o_trial_credit
    );
  end if;

  -- Display the free tier credit if the free tier range overlaps the billed range
  if o_free_tier_range && o_billed_range then
    o_line_items = o_line_items || jsonb_build_object(
      'description', case when upper(o_free_tier_range) is not null
        then format('Free tier credit ending %s', (upper(o_free_tier_range) - interval '1 day')::date)
        else 'Free tier credit'
      end,
      'count', 1,
      'rate', -o_free_tier_credit,
      'subtotal', -o_free_tier_credit
    );
  end if;

  -- Roll up the final subtotal.
  select into o_subtotal sum((l->>'subtotal')::numeric)
    from jsonb_array_elements(o_line_items) l;

  return jsonb_build_object(
    'billed_month', billed_month,
    'billed_prefix', billed_prefix,
    'daily_usage', o_daily_usage,
    'line_items', o_line_items,
    'processed_data_gb', o_data_gb,
    'recurring_fee', o_recurring_fee,
    'subtotal', o_subtotal,
    'task_usage_hours', o_task_hours,
    'trial_credit', coalesce(o_trial_credit, 0),
    'free_tier_credit', coalesce(o_free_tier_credit, 0),
    'trial_start', o_trial_start
  );

end
$$;

ALTER FUNCTION internal.billing_report_202308(billed_prefix public.catalog_prefix, billed_month timestamp with time zone) OWNER TO postgres;
