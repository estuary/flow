begin;

-- GraphQL previews and `billing-integrations freeze-invoices` use the `billing-types` calculation, and `publish-invoices` reads `internal.billing_historicals` and `internal.manual_bills` directly, so nothing in this repository reads these objects.
drop view public.invoices_ext;
drop view internal.invoices_ext;
drop function internal.freeze_billing_month(timestamptz);
drop function internal.billing_report_202308(public.catalog_prefix, timestamptz);
drop function public.billing_report_202308(public.catalog_prefix, timestamptz, tstzrange);

comment on table internal.billing_historicals is
'Frozen monthly invoices, written by `billing-integrations freeze-invoices`.';
comment on column internal.billing_historicals.report is
'The invoice JSON calculated by `billing-types`.';

commit;
