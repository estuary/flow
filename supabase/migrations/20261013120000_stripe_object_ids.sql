-- Record Stripe customer and invoice IDs for direct retrieval. Customer
-- resolution does not search metadata when stripe_customer_id is NULL; backfill
-- legacy customer IDs before relying on NULL to mean no existing Stripe customer.

begin;

alter table public.tenants
    add column stripe_customer_id text unique
        constraint stripe_customer_id_format check (stripe_customer_id ~ '^cus_');

comment on column public.tenants.stripe_customer_id is
'ID of the Stripe customer that bills this tenant. Recorded when the customer
is created. A NULL ID is not resolved by searching Stripe metadata.';

alter table internal.billing_historicals
    add column stripe_invoice_id text unique
        constraint stripe_invoice_id_format check (stripe_invoice_id ~ '^in_');

comment on column internal.billing_historicals.stripe_invoice_id is
'ID of the Stripe invoice issued for this billing statement.';

alter table internal.manual_bills
    add column stripe_invoice_id text unique
        constraint stripe_invoice_id_format check (stripe_invoice_id ~ '^in_');

comment on column internal.manual_bills.stripe_invoice_id is
'ID of the Stripe invoice issued for this bill. It identifies the invoice
across edits to the bill''s dates, which key the row.';

-- `stripe_invoice_id` is appended so the existing columns keep their positions,
-- as CREATE OR REPLACE VIEW requires. `public.invoices_ext` selects explicit
-- columns and doesn't expose it.
create or replace view internal.invoices_ext as
  select
      (date_trunc('month'::text, ((billing_historicals.report ->> 'billed_month'::text))::timestamp with time zone))::date as date_start,
      (((date_trunc('month'::text, ((billing_historicals.report ->> 'billed_month'::text))::timestamp with time zone) + '1 mon'::interval) - '1 day'::interval))::date as date_end,
      (billing_historicals.tenant)::text as billed_prefix,
      COALESCE(NULLIF((billing_historicals.report -> 'line_items'::text), 'null'::jsonb), '[]'::jsonb) as line_items,
      (COALESCE(NULLIF((billing_historicals.report -> 'subtotal'::text), 'null'::jsonb), to_jsonb(0)))::integer as subtotal,
      billing_historicals.report as extra,
      'final'::text as invoice_type,
      billing_historicals.stripe_invoice_id
    from internal.billing_historicals

  union all

  select
      (date_trunc('month'::text, ((report.report ->> 'billed_month'::text))::timestamp with time zone))::date as date_start,
      (((date_trunc('month'::text, ((report.report ->> 'billed_month'::text))::timestamp with time zone) + '1 mon'::interval) - '1 day'::interval))::date as date_end,
      (tenants.tenant)::text as billed_prefix,
      COALESCE(NULLIF((report.report -> 'line_items'::text), 'null'::jsonb), '[]'::jsonb) as line_items,
      (COALESCE(NULLIF((report.report -> 'subtotal'::text), 'null'::jsonb), to_jsonb(0)))::integer as subtotal,
      report.report as extra,
      'preview'::text as invoice_type,
      null::text as stripe_invoice_id
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
      'manual'::text as invoice_type,
      manual_bills.stripe_invoice_id
    from internal.manual_bills;

commit;
