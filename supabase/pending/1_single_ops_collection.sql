begin;

-- Rename the existing ops tenant to the new specific ops tenant.
update tenants set tenant = 'ops.us-central1.v1/' where tenant = 'ops/';

-- Rename the current catalog stats table to preserve its historical stats data.
alter table catalog_stats owner to postgres;
alter table catalog_stats rename to old_catalog_stats;

-- Clear out the existing reporting materialization(s) metadata. These tables will be re-created
-- when the new reporting materialization is started.
drop table flow_checkpoints_v1, flow_materializations_v2;

-- Create a publication that will delete all the existing per-tenant ops catalogs. After this
-- publication is complete, run 2_create_stats_table.sql prior to creating the new singular ops
-- reporting tasks.
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

commit;
