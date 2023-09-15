begin;

-- Compute a JSONB array of line-items detailing usage under a tenant's effective tiers.
create or replace function tier_line_items(
  -- Ammount of usage we're breaking out.
  amount numeric,
  -- Effective tenant tiers as ordered pairs of (quantity, cents), followed
  -- by a final unpaired cents for unbounded usage beyond the final quantity.
  tiers integer[],
  -- Descriptive name of the tiered thing ("Data processing").
  name text,
  -- Unit of the tier ("GB" or "hour").
  unit text
)
returns jsonb as $$
declare
  o_line_items jsonb = '[]'; -- Output variable.
  tier_count   integer;
  tier_pivot   integer;
  tier_rate    integer;
begin

  for idx in 1..array_length(tiers, 1) by 2 loop
    tier_rate = tiers[idx];
    tier_pivot = tiers[idx+1];
    tier_count = ceil(least(amount, tier_pivot));
    amount = amount - tier_count;

    o_line_items = o_line_items || jsonb_build_object(
      'description', format(
        case
          when tier_pivot is null then '%1$s (at %4$s/%2$s)'      -- Data processing (at $0.50/GB)
          when idx = 1 then '%1s (first %3$s %2$ss at %4$s/%2$s)' -- Data processing (first 30 GBs at $0.50/GB)
          else '%1$s (next %3$s %2$ss at %4$s/%2$s)'              -- Data processing (next 6 GBs at $0.25/GB)
        end,
        name,
        unit,
        tier_pivot,
        (tier_rate / 100.0)::money
      ),
      'count', tier_count,
      'rate', tier_rate,
      'subtotal', tier_count * tier_rate
    );
  end loop;

  return o_line_items;

end
$$ language plpgsql;


-- Billing report which is effective August 2023.
create or replace function billing_report_202308(billed_prefix catalog_prefix, billed_month timestamptz)
returns jsonb as $$
declare
  -- Auth checks
  has_admin_grant boolean;
  has_bypassrls boolean;

  -- Output variables.
  o_daily_usage   jsonb;
  o_data_gb       numeric;
  o_line_items    jsonb = '[]';
  o_recurring_fee integer;
  o_subtotal      integer;
  o_task_hours    numeric;
  o_trial_credit  integer;
  o_trial_start   date;
  o_trial_range   daterange;
  o_billed_range  daterange;
begin

  -- Ensure `billed_month` is the truncated start of the billed month.
  billed_month = date_trunc('month', billed_month);

  -- Verify that the user has an admin grant for the requested `billed_prefix`.
  perform 1 from auth_roles('admin') as r where billed_prefix ^@ r.role_prefix;
  has_admin_grant = found;

  -- Check whether the real active role has bypassrls flag set.
  -- Because this function is SECURITY DEFINER, both `current_user` and `current_role`
  -- will be `postgres`, which does have bypassrls set. Instead we want the
  -- role of the caller, which can be accessed like so according to:
  -- https://www.postgresql.org/message-id/13906.1141711109%40sss.pgh.pa.us
  perform * from pg_roles where rolname = current_setting('role') and rolbypassrls = true;
  has_bypassrls = found;

  if not has_admin_grant and not has_bypassrls then
    -- errcode 28000 causes PostgREST to return an HTTP 403
    -- see: https://www.postgresql.org/docs/current/errcodes-appendix.html
    -- and: https://postgrest.org/en/stable/errors.html#status-codes
    raise 'You are not authorized for the billed prefix %', billed_prefix using errcode = 28000;
  end if;

  with vars as (
    select
      t.data_tiers,
      t.trial_start,
      t.usage_tiers,
      tstzrange(billed_month, billed_month  + '1 month', '[)') as billed_range,
      case when t.trial_start is not null
        then tstzrange(t.trial_start, t.trial_start + interval '1 month', '[)')
        else 'empty' end as trial_range,
      -- Reveal contract costs only when computing whole-tenant billing.
      case when t.tenant = billed_prefix then t.recurring_usd_cents else 0 end as recurring_fee
      from tenants t
      where billed_prefix ^@ t.tenant -- Prefix starts with tenant.
  ),
  -- Roll up each day's incremental usage.
  daily_stat_deltas as (
    select
      ts,
      sum(bytes_written_by_me + bytes_read_by_me) / (1024.0 * 1024 * 1024) as data_gb,
      sum(usage_seconds) / (60.0 * 60) as task_hours
    from catalog_stats, vars
      where catalog_name ^@ billed_prefix -- Name starts with prefix.
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
      tier_line_items(data_gb, data_tiers, 'Data processing', 'GB') as data_line_items,
      tier_line_items(task_hours, usage_tiers, 'Task usage', 'hour') as task_line_items
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
  daily_array_and_trial_credit as (
    select
    jsonb_agg(jsonb_build_object(
      'ts', ts,
      'data_gb', data_gb,
      'data_subtotal', data_subtotal,
      'task_hours', task_hours,
      'task_subtotal', task_subtotal
    )) as daily_usage,
    coalesce(sum(data_subtotal + task_subtotal) filter (where trial_range @>ts),0 ) as trial_credit
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
    o_billed_range
    -- The actual selected columns.
    daily_usage,
    data_gb,
    data_line_items || task_line_items || adjustment_line_items,
    recurring_fee,
    task_hours,
    trial_credit,
    trial_start,
    trial_range,
    billed_range
  from daily_array_and_trial_credit, last_day, adjustments, vars;

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
    'trial_start', o_trial_start
  );

end
$$ language plpgsql volatile security definer;

commit;