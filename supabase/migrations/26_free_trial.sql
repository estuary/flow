
-- Always use a transaction, y'all.
begin;

alter table tenants add column free_trial_start timestamptz;

create or replace function internal.compute_incremental_line_items(item_name text, item_unit text, item_unit_plural text, single_usage numeric, tiers integer[], running_usage_sum numeric)
returns jsonb as $$
declare
  line_items jsonb = '[]';

  -- Calculating tiered usage.
  tier_rate    integer;
  tier_pivot   integer;
  tier_count   numeric;

  running_tier_pivot integer = 0;
begin
  -- Walk up the tiers
  for tier_idx in 1..array_length(tiers,1) by 2 loop
    tier_rate = tiers[tier_idx];
    tier_pivot = tiers[tier_idx+1];
    if tier_pivot is null then
      -- No limits here, roll all of the remaining usage into this tier
      tier_count = single_usage;
      running_usage_sum = running_usage_sum + tier_count;
      line_items = line_items || jsonb_build_object(
        'description', format(
          '%s (at %s/%s)',
          item_name,
          (tier_rate / 100.0)::money,
          item_unit
        ),
        'count', tier_count,
        'rate', tier_rate,
        'subtotal_frac', tier_count * tier_rate
      );
    elsif tier_pivot > running_usage_sum then
      running_tier_pivot = running_tier_pivot + tier_pivot;
      -- We haven't already surpassed this tier's pivot
      -- Calculate how much more usage we'd need to surpass this tier
      tier_count = least(single_usage, running_tier_pivot - running_usage_sum);
      single_usage = single_usage - tier_count;
      running_usage_sum = running_usage_sum + tier_count;
      line_items = line_items || jsonb_build_object(
        'description', format(
          case
            when tier_idx = 1 then '%s (first %s%s at %s/%s)'
            else '%s (next %s%s at %s/%s)'
          end,
          item_name,
          tier_pivot,
          item_unit_plural,
          (tier_rate / 100.0)::money,
          item_unit
        ),
        'count', tier_count,
        'rate', tier_rate,
        'subtotal_frac', tier_count * tier_rate
      );
    end if;
  end loop;

  return jsonb_build_object(
    'line_items', line_items,
    'running_usage_sum', running_usage_sum
  );
end
$$ language plpgsql;

create or replace function internal.incremental_usage_report(requested_grain text, billed_prefix catalog_prefix, billed_month timestamptz)
returns jsonb as $$
declare
  billed_range tstzrange;
  -- Retrieved from tenants table.
  data_tiers  integer[];
  usage_tiers integer[];

  granules jsonb = '[]';
  returned_data_line_items jsonb = '{}';
  returned_hours_line_items jsonb = '{}';
  combined_line_items jsonb;

  -- We can't round these subtotals yet because we might want to add them up later.
  -- Rounding before adding will result in inconsistent subtotals between daily and monthly
  -- granularities, since the monthly granularity by definition sums up all fractional
  -- values, which we then round later on. In reality the discrepancies are tiny since
  -- the maximum error per grain is 1 ($0.01), but better to be entirely exact when dealing with money.
  subtotal_frac numeric;

  running_gb_sum numeric = 0;
  running_hour_sum numeric = 0;
  line_items jsonb = '[]';
begin
  -- Because usage tiers reset at the beginning of every month, the logic defined here
  -- is only correct when operating on at most a whole month.
  billed_month = date_trunc('month', billed_month);
  billed_range = tstzrange(billed_month, billed_month + '1 month', '[)');

  select into data_tiers, usage_tiers
    t.data_tiers,
    t.usage_tiers
  from tenants t
  where billed_prefix ^@ t.tenant;

  -- Get all stats records for the selected time period at the selected granularity
  select into granules
    (select json_agg(res.obj) from (
        select jsonb_build_object(
          'processed_data_gb', sum((bytes_written_by_me + bytes_read_by_me)) / (1024.0 * 1024 * 1024),
          'task_usage_hours', sum(usage_seconds) / (60.0 * 60),
          'ts', ts
        ) as obj
        from catalog_stats
        where catalog_name ^@ billed_prefix
        and grain = requested_grain
        and billed_range @> ts
        group by ts
    ) as res)
  ;

  if granules is not null then
    for idx in 0..jsonb_array_length(granules)-1 loop
      returned_data_line_items = internal.compute_incremental_line_items('Data processing', 'GB', 'GB', (granules->idx->'processed_data_gb')::numeric, data_tiers, running_gb_sum);
      running_gb_sum = (returned_data_line_items->'running_usage_sum')::numeric;

      returned_hours_line_items = internal.compute_incremental_line_items('Task usage', 'hour', ' hours', (granules->idx->'task_usage_hours')::numeric, usage_tiers, running_hour_sum);
      running_hour_sum = (returned_hours_line_items->'running_usage_sum')::numeric;

      combined_line_items = (returned_data_line_items->'line_items')::jsonb || (returned_hours_line_items->'line_items')::jsonb;

      select into subtotal_frac sum((item->'subtotal_frac')::numeric) from jsonb_array_elements(combined_line_items) as item;

      line_items = line_items || jsonb_build_object(
        'line_items', combined_line_items,
        'subtotal_frac', subtotal_frac,
        'processed_data_gb', (granules->idx->'processed_data_gb')::numeric,
        'task_usage_hours', (granules->idx->'task_usage_hours')::numeric,
        'ts', granules->idx->'ts'
      );
    end loop;
  end if;

  return line_items;
end
$$ language plpgsql;

-- Billing report which is effective August 2023.
create or replace function billing_report_202308(billed_prefix catalog_prefix, billed_month timestamptz)
returns jsonb as $$
#variable_conflict use_variable
declare
  -- Auth checks
  has_admin_grant boolean;
  has_bypassrls boolean;

  -- Computed
  recurring_usd_cents integer;
  free_trial_range tstzrange;
  billed_range tstzrange;
  free_trial_overlap tstzrange;

  incremental_usage jsonb;
  daily_usage jsonb;

  free_trial_credit numeric;

  -- Temporary line items holders for free trial calculations
  task_usage_line_items jsonb = '[]';
  data_usage_line_items jsonb = '[]';

  -- Calculating adjustments.
  adjustment   internal.billing_adjustments;

  -- Aggregated outputs.
  line_items jsonb = '[]';
  subtotal_usd_cents integer;
  processed_data_gb numeric;
  task_usage_hours numeric;

  -- Free trial outputs
  free_trial_gb numeric;
  free_trial_hours numeric;
begin

  -- Ensure `billed_month` is the truncated start of the billed month.
  billed_month = date_trunc('month', billed_month);
  billed_range = tstzrange(billed_month, billed_month + '1 month', '[)');

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

  -- Fetch data & usage tiers for `billed_prefix`'s tenant.
  select into free_trial_range
      case
      	when t.free_trial_start is null then 'empty'::tstzrange
        -- Inclusive start, exclusive end
       	else tstzrange(date_trunc('day', t.free_trial_start), date_trunc('day', t.free_trial_start) + '30 days', '[)')
      end
    from tenants t
    where billed_prefix ^@ t.tenant
  ;
  -- Reveal contract costs only when the computing tenant-level billing.
  select into recurring_usd_cents t.recurring_usd_cents
    from tenants t
    where billed_prefix = t.tenant
  ;

  -- Apply a recurring service cost, if defined.
  if recurring_usd_cents != 0 then
    line_items = line_items || jsonb_build_object(
      'description', 'Recurring service charge',
      'count', 1,
      'rate', recurring_usd_cents,
      'subtotal', recurring_usd_cents
    );
  end if;

  -- Transform from `{"subtotal_frac": 1.98}` into `{"subtotal": 2}`
  -- We can comfortably round here because we're loading the monthly granularity
  -- meaning that summing has already happened.
  select into line_items, processed_data_gb, task_usage_hours
    line_items || (
      select json_agg(
              (item - 'subtotal_frac') ||
              jsonb_build_object(
                'subtotal', round((item->'subtotal_frac')::numeric)
              )
            )::jsonb
      from jsonb_array_elements(report->0->'line_items') as item
    ),
    (report->0->'processed_data_gb')::numeric,
    (report->0->'task_usage_hours')::numeric
  from internal.incremental_usage_report('monthly', billed_prefix, billed_month) as report;

  select into incremental_usage
    report
  from internal.incremental_usage_report('daily', billed_prefix, billed_month) as report;

  -- Does the free trial range overlap the month in question?
  if not isempty(free_trial_range) and (free_trial_range && billed_range) then
    free_trial_overlap = billed_range * free_trial_range;
    -- Sum up the fractional subtotals for each day in the portion of this
    -- month covered by the free trial. Note that we don't want to round yet
    -- since these are exact fractional values. Only after summing do we round.
    select into
      free_trial_credit coalesce(sum((line_item->>'subtotal_frac')::numeric), 0)
    from
      jsonb_array_elements(incremental_usage) as line_item
    where free_trial_overlap @> (line_item->>'ts')::timestamptz;

    line_items = line_items || jsonb_build_object(
      'description', format('Free trial credit (%s to %s)', lower(free_trial_range)::date,upper(free_trial_range)::date),
      'count', 1,
      'rate', round(free_trial_credit) * -1,
      'subtotal', round(free_trial_credit) * -1
    );
  end if;

  -- Apply any billing adjustments.
  for adjustment in select * from internal.billing_adjustments a
    where a.billed_month = billed_month and a.tenant = billed_prefix
  loop
    line_items = line_items || jsonb_build_object(
      'description', adjustment.detail,
      'count', 1,
      'rate', adjustment.usd_cents,
      'subtotal', adjustment.usd_cents
    );
  end loop;

  -- Roll up the final subtotal.
  select into subtotal_usd_cents sum((l->>'subtotal')::numeric)
    from jsonb_array_elements(line_items) l;

  -- Build up a list of days and their usage, with a default of 0
  select into daily_usage json_agg(
    case
      when usage is null then jsonb_build_object(
          'line_items', '[]'::jsonb,
          'subtotal', 0,
          'processed_data_gb', 0,
          'task_usage_hours', 0,
          'ts', date_of_month
        )
      else (usage - 'subtotal_frac') || jsonb_build_object(
          'subtotal', round((usage->'subtotal_frac')::numeric)
      )
    end
  )
  -- Despite the range being exclusive on the upper bound, upper() still returns the upper bound
  -- See this thread https://www.postgresql.org/message-id/20150116152713.2582.10294@wrigleys.postgresql.org
  from generate_series(lower(billed_range)::date, upper(billed_range)::date - interval '1 day', interval '1 day') as date_of_month
  left join jsonb_array_elements(incremental_usage) as usage on (usage->>'ts')::date = date_of_month::date;

  return jsonb_build_object(
    'billed_month', billed_month,
    'billed_prefix', billed_prefix,
    'line_items', line_items,
    'processed_data_gb', processed_data_gb,
    'recurring_fee', coalesce(recurring_usd_cents, 0),
    'subtotal', subtotal_usd_cents,
    'task_usage_hours', task_usage_hours,
    'daily_usage', daily_usage
  );

end
$$ language plpgsql volatile security definer;

commit;

