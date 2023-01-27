#!/bin/bash

# This script should be run after the updated agent has been deployed. There is a second migration
# tiered_stats_2.sql that should be run after this migration & the resulting publications are
# complete to change the owner of the catalog_stats table back to stats_loader.

# Run with:
#   Locally: ./supabase/pending/tiered_stats_1.sh | psql 'postgresql://postgres:postgres@localhost:5432/postgres'
#   In production: ./supabase/pending/tiered_stats_1.sh | psql <prod-postgres-url>

set -o errexit
set -o pipefail
set -o nounset

DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

function psql_input() {
cat <<EOF
begin;

alter table tenants
   add l1_stat_rollup integer not null
   default 0;

-- Prevent the deletion of the catalog_stats table by per-tenant stat materialization deletions
-- by temporarily changing the owner. We'll also manually clear out the materialized values so
-- the new single materialization can start from scratch successfully.
alter table catalog_stats owner to postgres;
truncate catalog_stats, flow_checkpoints_v1, flow_materializations_v2;

-- Create a publication that will delete all the existing per-tenant stat materializations.
do \$\$
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
        where catalog_name like ('ops/' || current_tenant.tenant || 'catalog-stats-view');
    end loop;
end
\$\$ language plpgsql;

-- This table and function will be re-created, along with all of the new functions in
-- 17_ops_catalogs.sql.
drop table ops_catalog_template;
drop function internal.create_ops_publication(tenant_prefix catalog_tenant, ops_user_id uuid);
EOF

cat ${DIR}/../migrations/17_ops_catalogs.sql

cat <<EOF

commit;
EOF
}

psql_input

