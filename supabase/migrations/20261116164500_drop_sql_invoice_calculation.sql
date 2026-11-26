begin;

-- Nothing reads these: `billing-types` calculates invoices, and
-- `billing-integrations` reads `internal.billing_historicals` and
-- `internal.manual_bills` directly.
drop view public.invoices_ext;
drop view internal.invoices_ext;
drop function internal.freeze_billing_month(timestamptz);
drop function internal.billing_report_202308(public.catalog_prefix, timestamptz);
drop function public.billing_report_202308(public.catalog_prefix, timestamptz, tstzrange);
drop function internal.test_billing_report(public.catalog_prefix, timestamptz);
drop function internal.incremental_usage_report(text, public.catalog_prefix, tstzrange);
drop function internal.compute_incremental_line_items(text, text, numeric, integer[], numeric);
drop function internal.tier_line_items(integer, integer[], text, text);
drop function public.tier_line_items(numeric, integer[], text, text);

comment on table internal.billing_historicals is
'Frozen monthly invoices, written by `billing-integrations freeze-invoices`.';
comment on column internal.billing_historicals.report is
'The frozen invoice JSON.';

commit;
