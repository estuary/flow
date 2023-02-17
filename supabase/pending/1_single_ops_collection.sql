begin;

-- Rename the current catalog stats table to preserve its historical stats data.
alter table catalog_stats owner to postgres;
alter table catalog_stats rename to old_catalog_stats;

-- Clear out the existing reporting materialization(s) metadata. These tables will be re-created
-- when the new reporting materialization is started.
drop table flow_checkpoints_v1, flow_materializations_v2;

-- Create a publication that will delete all the existing per-tenant ops catalogs. Since the
-- materializations aren't able to load a stored spec (that meta table was dropped above), they
-- won't try to delete the new catalog_stats table.
do $$
declare
    ops_user_id uuid;
    current_tenant tenants;
    new_draft_id flowid := internal.id_generator();
    publication_id flowid := internal.id_generator();
begin

    select id into strict ops_user_id from auth.users where email = 'support@estuary.dev';
    insert into drafts (id, user_id, detail) values
    (new_draft_id, ops_user_id, 'removing per-tenant ops catalogs');
    insert into publications (id, user_id, draft_id) values
    (publication_id, ops_user_id, new_draft_id);

    for current_tenant in
        select * from tenants
    loop
        insert into draft_specs (draft_id, catalog_name, spec_type, spec)
        select new_draft_id, catalog_name, null, null
        from live_specs
        where catalog_name in (
            'ops/' || current_tenant.tenant || 'catalog-stats-view',
            'ops/' || current_tenant.tenant || 'tests/catalog-stats',
            'ops/' || current_tenant.tenant || 'catalog-stats'
        );
    end loop;

end
$$ language plpgsql;

--
-- The rest of this is copied almost verbatim from 11_stats.sql, with the omission of creating the
-- "grain" type since it will already exist. It will re-create the catalog_stats table in its
-- updated non-partitioned form.
--


-- The `catalog_stats` table is _not_ identical to what the connector would have created.
-- They have slightly different column types to make things a little more ergonomic and consistent.

create table catalog_stats (
    catalog_name        catalog_name not null,
    grain               text         not null,
    bytes_written_by_me bigint       not null,
    docs_written_by_me  bigint       not null,
    bytes_read_by_me    bigint       not null,
    docs_read_by_me     bigint       not null,
    bytes_written_to_me bigint       not null,
    docs_written_to_me  bigint       not null,
    bytes_read_from_me  bigint       not null,
    docs_read_from_me   bigint       not null,
    warnings            integer      not null default 0,
    errors              integer      not null default 0,
    failures            integer      not null default 0,
    ts                  timestamptz  not null,
    flow_document       json         not null,
    primary key (catalog_name, grain, ts)
);
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
comment on column catalog_stats.ts is '
Timestamp indicating the start time of the time grain.

Monthly grains start on day 1 of the month, at hour 0 and minute 0.
Daily grains start on the day, at hour 0 and minute 0.
Hourly grains start on the hour, at minute 0.
';
comment on column catalog_stats.flow_document is
    'Aggregated statistics document for the given catalog name and grain';

do $$
begin
    if not exists (select from pg_catalog.pg_roles where rolname = 'stats_loader') then
        create role stats_loader with login password 'stats_loader_password' bypassrls;
   end if;
end
$$;

-- stats_loader loads directly to the catalog_stats table. Postgres routes records to the correct
-- partition based on the catalog name. We make catalog_stats owned by stats_loader instead of
-- postgres to allow for new materializations to be applied for each tenant with catalog_stats as
-- the target table. Materialization application will attempt to add comments to the target table &
-- columns, and this will fail unless the table is owned by the acting user.
alter table catalog_stats owner to stats_loader;

commit;
