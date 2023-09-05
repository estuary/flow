begin;

-- Billing report which is effective August 2023.
create or replace function billing_report_202308(billed_prefix catalog_prefix, billed_month timestamptz)
returns jsonb as $$
#variable_conflict use_variable
declare
  -- Auth checks
  has_admin_grant boolean;
  has_bypassrls boolean;
  -- Retrieved from tenants table.
  data_tiers  integer[];
  usage_tiers integer[];
  recurring_usd_cents integer;

  -- Calculating tiered usage.
  tier_rate    integer;
  tier_pivot   integer;
  tier_count   numeric;
  remainder    numeric;

  -- Calculating adjustments.
  adjustment   internal.billing_adjustments;

  -- Aggregated outputs.
  line_items jsonb = '[]';
  processed_data_gb numeric;
  subtotal_usd_cents integer;
  task_usage_hours  numeric;
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

  -- Fetch data & usage tiers for `billed_prefix`'s tenant.
  select into data_tiers, usage_tiers
    t.data_tiers, t.usage_tiers
    from tenants t
    where billed_prefix ^@ t.tenant
  ;
  -- Reveal contract costs only when the computing tenant-level billing.
  select into recurring_usd_cents t.recurring_usd_cents
    from tenants t
    where billed_prefix = t.tenant
  ;

  -- Determine the total amount of data processing and task usage
  -- under `billed_prefix` in the given `billed_month`.
  select into processed_data_gb, task_usage_hours
    coalesce(sum(bytes_written_by_me + bytes_read_by_me) / (1024.0 * 1024 * 1024),0),
    coalesce(sum(usage_seconds) / (60.0 * 60), 0)
    from catalog_stats
    where catalog_name ^@ billed_prefix
    and grain = 'monthly'
    and ts = billed_month
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

  -- Apply each of the data processing tiers.
  remainder = processed_data_gb;

  for idx in 1..array_length(data_tiers, 1) by 2 loop
    tier_rate = data_tiers[idx];
    tier_pivot = data_tiers[idx+1];
    tier_count = least(remainder, tier_pivot);
    remainder = remainder - tier_count;

    line_items = line_items || jsonb_build_object(
      'description', format(
        case
          when tier_pivot is null then 'Data processing (at %2$s/GB)'
          when idx = 1 then 'Data processing (first %sGB at %s/GB)'
          else 'Data processing (next %sGB at %s/GB)'
        end,
        tier_pivot,
        (tier_rate / 100.0)::money
      ),
      'count', tier_count,
      'rate', tier_rate,
      'subtotal', round(tier_count * tier_rate)
    );
  end loop;

  -- Apply each of the task usage tiers.
  remainder = task_usage_hours;

  for idx in 1..array_length(usage_tiers, 1) by 2 loop
    tier_rate = usage_tiers[idx];
    tier_pivot = usage_tiers[idx+1];
    tier_count = least(remainder, tier_pivot);
    remainder = remainder - tier_count;

    line_items = line_items || jsonb_build_object(
      'description', format(
        case
          when tier_pivot is null then 'Task usage (at %2$s/hour)'
          when idx = 1 then 'Task usage (first %s hours at %s/hour)'
          else 'Task usage (next %s hours at %s/hour)'
        end,
        tier_pivot,
        (tier_rate / 100.0)::money
      ),
      'count', tier_count,
      'rate', tier_rate,
      'subtotal', round(tier_count * tier_rate)
    );
  end loop;

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

  return jsonb_build_object(
    'billed_month', billed_month,
    'billed_prefix', billed_prefix,
    'line_items', line_items,
    'processed_data_gb', processed_data_gb,
    'recurring_fee', coalesce(recurring_usd_cents, 0),
    'subtotal', subtotal_usd_cents,
    'task_usage_hours', task_usage_hours
  );

end
$$ language plpgsql volatile security definer;

commit;