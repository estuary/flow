
-- Always use a transaction, y'all.
begin;

-- Historical record of tenant billing statements. This structure
-- comes from the return value of `billing_report_202308`.
create table billing_historicals (
    tenant              catalog_tenant  not null,
    billed_month        timestamptz     not null,
    line_items          jsonb           not null,
    processed_data_gb   numeric         not null,
    recurring_fee       integer         not null,
    subtotal            integer         not null,
    task_usage_hours    numeric         not null

    check (date_trunc('month', billed_month) = billed_month),
    unique (tenant, billed_month)
);
alter table billing_historicals enable row level security;
grant all on billing_historicals to postgres;

create policy "Users must be authorized to their catalog tenant"
  on billing_historicals as permissive for select
  using (exists(
    select 1 from auth_roles('admin') r where tenant ^@ r.role_prefix
  ));
grant select on billing_historicals to authenticated;

-- Calculate the specified month's billing report for every tenant
-- and save those reports to billing_historicals.
create function internal.freeze_billing_month(billed_month timestamptz)
returns integer as $$
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
$$ language plpgsql volatile;

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

-- The following enables the regularly scheduled function that creates
-- billing_historical for every tenant at the end of every month.
-- If you want to enable it locally, then just uncomment this
-- or run it manually. More often, it's more convenient during local
-- development to manually trigger this by calling
-- internal.freeze_billing_month() whenever you want to trigger it.

-- create extension pg_cron with schema extensions;
-- select cron.schedule (
--     'month-end billing', -- name of the cron job
--     '0 0 0 2 * ? *', -- run on the second day of every month
--     $$ select internal.freeze_billing_month(now()) $$
-- );

commit;

