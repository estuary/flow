#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

# Prior to beginning, make sure the specs that were created by the original application of
# directives have been deleted. At the time of this writing, this includes these tasks:
#	- ops/ops/stats-by-hour-view
#	- ops/ops/tests/stats-by-hour
#	- ops/ops/stats-by-hour
#	- ops/estuary/tests/stats-by-hour
#	- ops/estuary/stats-by-hour-view
#	- ops/estuary/stats-by-hour

function psql_input() {
    cat <<EOF
begin;

-- Clean up tables that are now unused.
drop table public.task_stats cascade;
drop type public.task_type;
drop schema task_stat_partitions cascade;

-- We added a uniqueness constraint to the storage_mappings table.
alter table public.storage_mappings add constraint storage_mappings_catalog_prefix_key unique (catalog_prefix);

EOF

    cat ${DIR}/../migrations/11_stats.sql

    cat <<EOF

-- Remove tenants applied by original directives. Only ops/ and estuary/ exist at this time.
delete from tenants where tenant in ('ops/', 'estuary/');

-- Re-provision the ops/ tenant owned by the support@estuary.dev user.
-- Note: On production, this is support@estuary.dev. On a local stack it is accounts@estuary.dev.
with accounts_root_user as (
  select (select id from auth.users where email = 'support@estuary.dev' limit 1) as accounts_id
)
insert into applied_directives (directive_id, user_id, user_claims)
  select d.id, a.accounts_id, '{"requestedTenant":"ops"}'
    from directives as d, accounts_root_user as a
    where catalog_prefix = 'ops/' and spec = '{"type":"betaOnboard"}';

commit;
EOF
}

psql_input | psql postgres://postgres:postgres@localhost:5432/postgres

cat <<EOF

The estuary/ tenant should also be re-created now by applying a directive for it.

EOF