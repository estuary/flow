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

alter table manual_bills enable row level security;

create policy "Users must be authorized to their catalog tenant"
  on manual_bills as permissive for select
  using (exists(
    select 1 from auth_roles('admin') r where tenant ^@ r.role_prefix
  ));
grant select on manual_bills to authenticated;

comment on table manual_bills is
  'Manually entered bills that span an arbitrary date range';


create or replace view invoices_ext as
with admin_roles as (
  select role_prefix from auth_roles('admin')
),
historical_bills as (
  select
    date_trunc('month', (report->>'billed_month')::timestamptz) as date_start,
    date_trunc('month', (report->>'billed_month')::timestamptz) + interval '1 month' - interval '1 day' as date_end,
    report->>'billed_prefix' as billed_prefix,
    report->'line_items' as line_items,
    report->'subtotal' as subtotal
  from billing_historicals
  -- inner join should give only rows that match the join condition
  inner join admin_roles on billing_historicals.tenant ^@ admin_roles.role_prefix
),
manual_bills as (
  select
    date_start,
    date_end,
    tenant as billed_prefix,
    jsonb_build_array(
      jsonb_build_object(
        'description', manual_bills.description,
        'count', 1,
        'rate', manual_bills.usd_cents,
        'subtotal', manual_bills.usd_cents
      )
    ) as line_items,
    to_jsonb(usd_cents) as subtotal
  from manual_bills
  inner join admin_roles on manual_bills.tenant ^@ admin_roles.role_prefix
),
current_month as (
  select
    date_trunc('month', (report->>'billed_month')::timestamptz) as date_start,
    date_trunc('month', (report->>'billed_month')::timestamptz) + interval '1 month' - interval '1 day' as date_end,
    report->>'billed_prefix' as billed_prefix,
    report->'line_items' as line_items,
    report->'subtotal' as subtotal
  from admin_roles, billing_report_202308(admin_roles.role_prefix, now()) as report
),
combined as (
  select
    h.date_start, h.date_end, h.billed_prefix, h.line_items, h.subtotal, 'usage' as invoice_type
  from historical_bills h
  union all
  select
    m.date_start, m.date_end, m.billed_prefix, m.line_items, m.subtotal, 'manual' as invoice_type
  from manual_bills m
  union all
  select
    c.date_start, c.date_end, c.billed_prefix, c.line_items, c.subtotal, 'current_month' as invoice_type
  from current_month c
)
select * from combined
order by date_start desc;

grant select on table invoices_ext to authenticated;

commit;