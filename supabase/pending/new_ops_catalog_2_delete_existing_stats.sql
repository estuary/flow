-- Run this after deploying the updated agent. At that point, new partitions & ops catalog
-- publications will cease for each new tenant. This migration will also delete all per-tenant
-- reporting tasks.

begin;

drop table ops_catalog_template;
drop function internal.create_ops_publication(tenant_prefix catalog_tenant, ops_user_id uuid);

-- Clear out the existing catalog_stats table and materialization metadata tables. The new
-- ops-catalog materialization will create new metadata tables. The catalog_stats table should be
-- created anew by re-running its migration prior to the new ops-catalog being deployed since the
-- connector would create the table differently than we want.
drop table catalog_stats, flow_checkpoints_v1, flow_materializations_v2;
drop schema catalog_stat_partitions;

-- Create a publication that will delete all the existing per-tenant ops catalogs.
do $$
declare
    ops_user_id uuid;
    current_tenant tenants;
    new_draft_id flowid := internal.id_generator();
    publication_id flowid := internal.id_generator();
begin

    select id into strict ops_user_id from auth.users where email = 'support@estuary.dev';
    insert into drafts (id, user_id, detail) values
    (new_draft_id, ops_user_id, 're-publishing ops catalog');
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

