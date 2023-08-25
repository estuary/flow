create table "public"."billing_historicals" (
    "tenant" catalog_tenant not null,
    "billed_month" timestamp with time zone not null,
    "line_items" jsonb not null,
    "processed_data_gb" numeric not null,
    "recurring_fee" integer not null,
    "subtotal" integer not null,
    "task_usage_hours" numeric not null
);
alter table "public"."billing_historicals" enable row level security;

CREATE UNIQUE INDEX billing_historicals_tenant_billed_month_key ON public.billing_historicals USING btree (tenant, billed_month);
alter table "public"."billing_historicals" add constraint "billing_historicals_billed_month_check" CHECK ((date_trunc('month'::text, billed_month) = billed_month)) not valid;
alter table "public"."billing_historicals" validate constraint "billing_historicals_billed_month_check";
alter table "public"."billing_historicals" add constraint "billing_historicals_tenant_billed_month_key" UNIQUE using index "billing_historicals_tenant_billed_month_key";

create policy "Users must be authorized to their catalog tenant"
on "public"."billing_historicals"
as permissive
for select
to public
using ((EXISTS ( SELECT 1
   FROM auth_roles('admin'::grant_capability) r(role_prefix, capability)
  WHERE ((billing_historicals.tenant)::text ^@ (r.role_prefix)::text))));

-- set check_function_bodies = off;

CREATE OR REPLACE FUNCTION public.billing_report_202308(billed_prefix catalog_prefix, billed_month timestamp with time zone)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
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

  -- Check whether user has bypassrls flag
  perform 1 from pg_roles where rolname = session_user and rolbypassrls = true;
  has_bypassrls = found;

  if not has_bypassrls and not found then
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
    sum(bytes_written_by_me + bytes_read_by_me) / (1024.0 * 1024 * 1024),
    sum(usage_seconds) / (60.0 * 60)
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
$function$
;

CREATE OR REPLACE FUNCTION internal.freeze_billing_month(billed_month timestamp with time zone)
 RETURNS integer
 LANGUAGE plpgsql
AS $function$
declare
    tenant_row record;
    tenant_count integer = 0;
begin
    for tenant_row in select tenant as tenant_name from tenants loop
        tenant_count = tenant_count + 1;
        insert into billing_historicals
        select
            tenant_row.tenant_name as tenant_name,
            date_trunc('month', billed_month) as billed_month,
            report->'line_items' as line_items,
            (coalesce(nullif(report->'processed_data_gb','null'),'0'))::numeric as processed_data_gb,
            (report->'recurring_fee')::integer as recurring_fee,
            (report->'subtotal')::integer as subtotal,
            (coalesce(nullif(report->'task_usage_hours','null'),'0'))::numeric as task_usage_hours
        from billing_report_202308(tenant_row.tenant_name, date_trunc('month', billed_month)) as report;
    end loop;
    return tenant_count;
end
$function$
;

-- Had to manually add these
comment on table billing_historicals is
    'Historical billing statements frozen from `billing_report_202308()`.';
comment on column billing_historicals.tenant is
    'The tenant for this statement';
comment on column billing_historicals.billed_month is
    'The month for this statement';
comment on column billing_historicals.line_items is
    'A list of line items composing this statement.
    Line items contain `description`, `count`, `rate`, and `subtotal`.';
comment on column billing_historicals.processed_data_gb is
    'The total number of gigabytes of data processed by this tenant this month.';
comment on column billing_historicals.recurring_fee is
    'The fixed portion of this tenant''s bill. 0 if no recurring component.';
comment on column billing_historicals.subtotal is
    'The subtotal in whole USD cents for this statement.';
comment on column billing_historicals.task_usage_hours is
    'The total number of task-hours used by this tenant this month.';

