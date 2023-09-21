begin;

create table manual_bills (
  tenant       catalog_tenant not null references tenants(tenant),
  usd_cents    integer        not null,
  description  text           not null,
  date_start   date           not null,
  date_end     date           not null,
  constraint dates_make_sense check (date_start < date_end),
  primary key (tenant, date_start, date_end)
);

comment on table manual_bills is
  'Manually entered bills that span an arbitrary date range';

-- Move billing report gen to internal

-- Drop the public functions
drop function billing_report_202308(catalog_prefix, timestamptz);
drop function tier_line_items(integer, integer[], text, text);

-- Compute a JSONB array of line-items detailing usage under a tenant's effective tiers.
create or replace function internal.tier_line_items(
  -- Ammount of usage we're breaking out.
  amount integer,
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
    tier_count = least(amount, tier_pivot);
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
-- Removed authorization logic as it's now going to be handled in invoices_ext
create or replace function internal.billing_report_202308(billed_prefix catalog_prefix, billed_month timestamptz)
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

-- Note: have to redefine this to know about internal.billing_report
-- Calculate the specified month's billing report for every tenant
-- and save those reports to billing_historicals.
create or replace function internal.freeze_billing_month(billed_month timestamptz)
returns integer as $$
declare
    tenant_row record;
    tenant_count integer = 0;
begin
    for tenant_row in select tenant as tenant_name from tenants loop
        insert into billing_historicals
        select
            report->>'billed_prefix' as tenant,
            (report->>'billed_month')::timestamptz as billed_month,
            report
        from internal.billing_report_202308(tenant_row.tenant_name, billed_month) as report
        on conflict do nothing;

        -- INSERT statements set FOUND true if at least one row is affected, false if no row is affected.
        if found then
          tenant_count = tenant_count + 1;
        end if;
    end loop;
    return tenant_count;
end
$$ language plpgsql volatile;
-- End internal billing report gen

create or replace view invoices_ext as
with has_bypassrls as (
  select exists(select 1 from pg_roles where rolname = current_role and rolbypassrls = true) as bypass
),
authorized_tenants as (
  select tenants.tenant
  from tenants
  left join has_bypassrls on true
  left join auth_roles('admin') on tenants.tenant ^@ auth_roles.role_prefix
  where has_bypassrls.bypass or auth_roles.role_prefix is not null
),
historical_bills as (
  select
    date_trunc('month', (report->>'billed_month')::timestamptz) as date_start,
    date_trunc('month', (report->>'billed_month')::timestamptz) + interval '1 month' - interval '1 day' as date_end,
    report->>'billed_prefix' as billed_prefix,
    report->'line_items' as line_items,
    report->'subtotal' as subtotal,
    report as extra
  from billing_historicals
  -- inner join should give only rows that match the join condition
  inner join authorized_tenants on billing_historicals.tenant ^@ authorized_tenants.tenant
),
manual_bills as (
  select
    date_start,
    date_end,
    manual_bills.tenant as billed_prefix,
    jsonb_build_array(
      jsonb_build_object(
        'description', manual_bills.description,
        'count', 1,
        'rate', manual_bills.usd_cents,
        'subtotal', manual_bills.usd_cents
      )
    ) as line_items,
    to_jsonb(usd_cents) as subtotal,
    'null'::jsonb as extra
  from manual_bills
  inner join authorized_tenants on manual_bills.tenant ^@ authorized_tenants.tenant
),
current_month as (
  select
    date_trunc('month', (report->>'billed_month')::timestamptz) as date_start,
    date_trunc('month', (report->>'billed_month')::timestamptz) + interval '1 month' - interval '1 day' as date_end,
    report->>'billed_prefix' as billed_prefix,
    report->'line_items' as line_items,
    report->'subtotal' as subtotal,
    report as extra
  from authorized_tenants, internal.billing_report_202308(authorized_tenants.tenant, now()) as report
),
combined as (
  select
    h.date_start, h.date_end, h.billed_prefix, h.line_items, h.subtotal, h.extra, 'usage' as invoice_type
  from historical_bills h
  union all
  select
    m.date_start, m.date_end, m.billed_prefix, m.line_items, m.subtotal, m.extra, 'manual' as invoice_type
  from manual_bills m
  union all
  select
    c.date_start, c.date_end, c.billed_prefix, c.line_items, c.subtotal, c.extra, 'current_month' as invoice_type
  from current_month c
)
select * from combined
order by date_start desc;

grant select on table invoices_ext to authenticated;

commit;