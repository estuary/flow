begin;

-- Refactor this to avoid use of CTEs, which don't support predicate push-down,
-- which was causing query performance to be very bad.
-- This is essentially the same as the existing invoices_ext, except the three
-- different invoice types are now computed in subqueries instead of CTEs. The other
-- change is to select authorized_tenants.tenant as the billed_prefix, which allows
-- a filter on invoices_ext.billed_prefix to get pushed down properly.
create or replace view invoices_ext as
with has_bypassrls as (
  select exists(select 1 from pg_roles where rolname = current_role and rolbypassrls = true) as bypass
),
authorized_tenants as (
  select tenants.tenant, tenants.created_at
  from tenants
  left join has_bypassrls on true
  left join auth_roles('admin') on tenants.tenant ^@ auth_roles.role_prefix
  where has_bypassrls.bypass or auth_roles.role_prefix is not null
)
select
    date_trunc('month', (report->>'billed_month')::timestamptz)::date as date_start,
    (date_trunc('month', (report->>'billed_month')::timestamptz) + interval '1 month' - interval '1 day')::date as date_end,
    authorized_tenants.tenant::text as billed_prefix,
    coalesce(nullif(report->'line_items', 'null'::jsonb), '[]'::jsonb) as line_items,
    coalesce(nullif(report->'subtotal', 'null'::jsonb), to_jsonb(0))::integer as subtotal,
    report as extra,
    'final' as invoice_type
  from internal.billing_historicals h
  -- inner join should give only rows that match the join condition
  inner join authorized_tenants on h.tenant ^@ authorized_tenants.tenant
union all
select
    date_trunc('month', (report->>'billed_month')::timestamptz)::date as date_start,
    (date_trunc('month', (report->>'billed_month')::timestamptz) + interval '1 month' - interval '1 day')::date as date_end,
    authorized_tenants.tenant::text as billed_prefix,
    coalesce(nullif(report->'line_items', 'null'::jsonb), '[]'::jsonb) as line_items,
    coalesce(nullif(report->'subtotal', 'null'::jsonb), to_jsonb(0))::integer as subtotal,
    report as extra,
    'preview' as invoice_type
  from authorized_tenants
  join generate_series(
    greatest(date '2023-08-01', date_trunc('month', authorized_tenants.created_at)::date),
    date_trunc('month',now()::date),
    '1 month'
  ) as invoice_month on not exists(
    select 1
    from internal.billing_historicals h
    where h.tenant ^@ authorized_tenants.tenant
    and date_trunc('month', (h.report->>'billed_month')::timestamptz)::date = invoice_month
  )
  join internal.billing_report_202308(authorized_tenants.tenant, invoice_month) as report on true
union all
select
    date_start,
    date_end,
    authorized_tenants.tenant::text as billed_prefix,
    jsonb_build_array(
      jsonb_build_object(
        'description', manual_bills.description,
        'count', 1,
        'rate', manual_bills.usd_cents,
        'subtotal', manual_bills.usd_cents
      )
    ) as line_items,
    usd_cents as subtotal,
    'null'::jsonb as extra,
    'manual' as invoice_type
  from internal.manual_bills
  inner join authorized_tenants on manual_bills.tenant ^@ authorized_tenants.tenant;

commit;