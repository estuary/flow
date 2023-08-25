
-- Always use a transaction, y'all.
begin;

-- Grain is not actually used anywhere.
-- It was originally intended for `catalog_stats` but isn't used and won't be.
drop type grain;

-- Transition `catalog_stats` to be a partitioned table on grain.
alter table catalog_stats rename to catalog_stats_old;

create table catalog_stats (
    catalog_name        catalog_name not null,
    grain               text         not null,
    ts                  timestamptz  not null,
    bytes_written_by_me bigint       not null default 0,
    docs_written_by_me  bigint       not null default 0,
    bytes_read_by_me    bigint       not null default 0,
    docs_read_by_me     bigint       not null default 0,
    bytes_written_to_me bigint       not null default 0,
    docs_written_to_me  bigint       not null default 0,
    bytes_read_from_me  bigint       not null default 0,
    docs_read_from_me   bigint       not null default 0,
    usage_seconds       integer      not null default 0,
    warnings            integer      not null default 0,
    errors              integer      not null default 0,
    failures            integer      not null default 0,
    flow_document       json         not null,
    primary key (catalog_name, grain, ts)
) partition by list (grain);

create table catalog_stats_monthly partition of catalog_stats for values in ('monthly');
create table catalog_stats_daily partition of catalog_stats for values in ('daily');
create table catalog_stats_hourly partition of catalog_stats for values in ('hourly');

alter table catalog_stats enable row level security;

create policy "Users must be authorized to the catalog name"
  on catalog_stats as permissive for select
  using (exists(
    select 1 from auth_roles('read') r where catalog_name ^@ r.role_prefix
  ));
grant select on catalog_stats to authenticated;

comment on table catalog_stats is
    'Statistics for Flow catalogs';
comment on column catalog_stats.grain is '
Time grain that stats are summed over.

One of "monthly", "daily", or "hourly".
';
comment on column catalog_stats.bytes_written_by_me is
    'Bytes written by this catalog, summed over the time grain.';
comment on column catalog_stats.docs_written_by_me is
    'Documents written by this catalog, summed over the time grain.';
comment on column catalog_stats.bytes_read_by_me is
    'Bytes read by this catalog, summed over the time grain.';
comment on column catalog_stats.docs_read_by_me is
    'Documents read by this catalog, summed over the time grain.';
comment on column catalog_stats.bytes_written_to_me is
    'Bytes written to this catalog, summed over the time grain.';
comment on column catalog_stats.docs_written_to_me is
    'Documents written to this catalog, summed over the time grain.';
comment on column catalog_stats.bytes_read_from_me is
    'Bytes read from this catalog, summed over the time grain.';
comment on column catalog_stats.docs_read_from_me is
    'Documents read from this catalog, summed over the time grain.';
comment on column catalog_stats.usage_seconds is
    'Metered usage of this catalog task.';
comment on column catalog_stats.ts is '
Timestamp indicating the start time of the time grain.

Monthly grains start on day 1 of the month, at hour 0 and minute 0.
Daily grains start on the day, at hour 0 and minute 0.
Hourly grains start on the hour, at minute 0.
';
comment on column catalog_stats.flow_document is
    'Aggregated statistics document for the given catalog name and grain';

-- Populate our rebuilt table.
insert into catalog_stats (
    catalog_name,
    grain,
    ts,
    bytes_written_by_me,
    docs_written_by_me,
    bytes_read_by_me,
    docs_read_by_me,
    bytes_written_to_me,
    docs_written_to_me,
    bytes_read_from_me,
    docs_read_from_me,
    warnings,
    errors,
    failures,
    flow_document
)
select
    catalog_name,
    grain,
    ts,
    bytes_written_by_me,
    docs_written_by_me,
    bytes_read_by_me,
    docs_read_by_me,
    bytes_written_to_me,
    docs_written_to_me,
    bytes_read_from_me,
    docs_read_from_me,
    warnings,
    errors,
    failures,
    flow_document
from catalog_stats_old;

alter table catalog_stats owner to stats_loader;
drop table catalog_stats_old;


-- Internal table used for one-off or negotiated adjustments.
create table internal.billing_adjustments (
  like internal._model including all,

  tenant       catalog_tenant not null references tenants(tenant),
  billed_month timestamptz    not null,
  usd_cents    integer        not null,
  authorizer   text           not null,

  constraint "billed_month must be at a month boundary" check (
    billed_month = date_trunc('month', billed_month)
  )
);

comment on table internal.billing_adjustments is
  'Internal table for authorized adjustments to tenant invoices, such as make-goods or negotiated service fees';
comment on column internal.billing_adjustments.tenant is
  'Tenant which is being credited or debited.';
comment on column internal.billing_adjustments.billed_month is
  'Month to which the adjustment is applied';
comment on column internal.billing_adjustments.usd_cents is
  'Amount of adjustment. Positive values make the bill larger, negative values make it smaller';
comment on column internal.billing_adjustments.authorizer is
  'Estuary employee who authorizes the adjustment';


-- Billing report which is effective August 2023.
create function billing_report_202308(billed_prefix catalog_prefix, billed_month timestamptz)
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
$$ language plpgsql volatile security definer;


-- Add data & usage tiers to tenants, as well as any recurring service charge.
alter table tenants add column data_tiers integer[] not null default '{50, 1024, 20}';
alter table tenants add constraint "data_tiers is odd"  check (array_length(data_tiers, 1) % 2 = 1 );
alter table tenants add column usage_tiers integer[] not null default '{14}';
alter table tenants add constraint "usage_tiers is odd" check (array_length(usage_tiers, 1) % 2 = 1);
alter table tenants add column recurring_usd_cents integer not null default 0;

comment on column tenants.tasks_quota is
  'Maximum number of active tasks that the tenant may have';
comment on column tenants.collections_quota is
  'Maximum number of collections that the tenant may have';
comment on column tenants.data_tiers is '
Tiered data processing volumes and prices.

Structured as an odd-length array of a price (in cents) followed by a volume (in GB).
For example, `{50, 1024, 30, 2048, 20}` is interpreted as:
  * $0.50 per GB for the first TB (1,024 GB).
  * $0.30 per GB for the next two TB (3TB cumulative).
  * $0.20 per GB thereafter.
';
comment on column tenants.usage_tiers is '
Tiered task usage quantities and prices.

Structured as an odd-length array of a price (in cents) followed by a quantity (in hours).
For example, `{30, 1440, 20, 2880, 15}` is interpreted as:
  * $0.30 per hour for the first 1,440 hours.
  * $0.20 per hour for the next 2,880 hours (4,320 hours total).
  * $0.15 per hour thereafter.
';
comment on column tenants.recurring_usd_cents is '
Recurring monthly cost incurred by a tenant under a contracted relationship, in US cents (1/100ths of a USD).
';

commit;