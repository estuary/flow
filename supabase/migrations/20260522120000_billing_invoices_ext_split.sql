-- Split invoices_ext into a no-auth data view (internal.invoices_ext) and a
-- thin authorization wrapper (public.invoices_ext). Callers that have already
-- authorized the tenant (e.g. the control-plane-api GraphQL resolver) can query
-- internal.invoices_ext directly, which lets the planner push the
-- `billed_prefix = $1` predicate through UNION ALL into each branch and avoids
-- materializing every tenant via the authorized_tenants CTE.

create view internal.invoices_ext as
  select
      (date_trunc('month'::text, ((billing_historicals.report ->> 'billed_month'::text))::timestamp with time zone))::date as date_start,
      (((date_trunc('month'::text, ((billing_historicals.report ->> 'billed_month'::text))::timestamp with time zone) + '1 mon'::interval) - '1 day'::interval))::date as date_end,
      (billing_historicals.tenant)::text as billed_prefix,
      COALESCE(NULLIF((billing_historicals.report -> 'line_items'::text), 'null'::jsonb), '[]'::jsonb) as line_items,
      (COALESCE(NULLIF((billing_historicals.report -> 'subtotal'::text), 'null'::jsonb), to_jsonb(0)))::integer as subtotal,
      billing_historicals.report as extra,
      'final'::text as invoice_type
    from internal.billing_historicals

  union all

  select
      (date_trunc('month'::text, ((report.report ->> 'billed_month'::text))::timestamp with time zone))::date as date_start,
      (((date_trunc('month'::text, ((report.report ->> 'billed_month'::text))::timestamp with time zone) + '1 mon'::interval) - '1 day'::interval))::date as date_end,
      (tenants.tenant)::text as billed_prefix,
      COALESCE(NULLIF((report.report -> 'line_items'::text), 'null'::jsonb), '[]'::jsonb) as line_items,
      (COALESCE(NULLIF((report.report -> 'subtotal'::text), 'null'::jsonb), to_jsonb(0)))::integer as subtotal,
      report.report as extra,
      'preview'::text as invoice_type
    from public.tenants
    join lateral generate_series(
        (GREATEST('2023-08-01'::date, (date_trunc('month'::text, tenants.created_at))::date))::timestamp with time zone,
        date_trunc('month'::text, ((now())::date)::timestamp with time zone),
        '1 mon'::interval
      ) invoice_month(invoice_month)
      on not exists (
        select 1
        from internal.billing_historicals
        where ((billing_historicals.tenant)::text ^@ (tenants.tenant)::text)
          and ((date_trunc('month'::text, ((billing_historicals.report ->> 'billed_month'::text))::timestamp with time zone))::date = invoice_month.invoice_month)
      )
    join lateral internal.billing_report_202308((tenants.tenant)::public.catalog_prefix, invoice_month.invoice_month) report(report)
      on true

  union all

  select
      manual_bills.date_start,
      manual_bills.date_end,
      (manual_bills.tenant)::text as billed_prefix,
      jsonb_build_array(jsonb_build_object('description', manual_bills.description, 'count', 1, 'rate', manual_bills.usd_cents, 'subtotal', manual_bills.usd_cents)) as line_items,
      manual_bills.usd_cents as subtotal,
      'null'::jsonb as extra,
      'manual'::text as invoice_type
    from internal.manual_bills;

comment on view internal.invoices_ext is
  'Tenant invoices (final + preview + manual) sans authorization.'
  'Use public.invoices_ext for the authorization-checked view.';

grant select on internal.invoices_ext to service_role;

create or replace view public.invoices_ext as
  with has_bypassrls as (
    select (exists (
      select 1 from pg_roles
      where ((pg_roles.rolname = current_role) and (pg_roles.rolbypassrls = true))
    )) as bypass
  ),
  authorized_tenants as (
    select tenants.tenant, tenants.created_at
      from public.tenants
      left join has_bypassrls on (true)
      left join public.auth_roles('admin'::public.grant_capability) auth_roles(role_prefix, capability)
        on (((tenants.tenant)::text ^@ (auth_roles.role_prefix)::text))
     where (has_bypassrls.bypass or (auth_roles.role_prefix is not null))
  )
  select
      invoices_ext.date_start,
      invoices_ext.date_end,
      (authorized_tenants.tenant)::text as billed_prefix,
      invoices_ext.line_items,
      invoices_ext.subtotal,
      invoices_ext.extra,
      invoices_ext.invoice_type
    from internal.invoices_ext
    join authorized_tenants on invoices_ext.billed_prefix = authorized_tenants.tenant::text;
