#!/bin/bash

# This script migrates a pre-directives local stack (or production) to a post-directives install.
# You must first create an 'accounts@estuary.dev' user or the migration will fail to apply & rollback.
# Then rebuild / restart your control-plane agent to have it automatically provision the 'ops/' tenant.

set -o errexit
set -o pipefail
set -o nounset

DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

function psql_input() {
    cat <<EOF
begin;

drop table public.flow_materializations_v2 cascade;
drop table public.task_stats_by_minute cascade;
drop table public.flow_checkpoints_v1 cascade;
drop table public.task_stats_by_hour cascade;
drop table public.task_stats_by_day cascade;
drop type  public.task_type;

-- Copied verbatim from 03_catalog-types.sql:

create domain catalog_tenant as text
  constraint "Must be a valid catalog tenant"
  check (value ~ '^[[:alpha:][:digit:]\-_.]+/$' and value is nfkc normalized);
comment on domain catalog_tenant is '
catalog_tenant is a prefix within the Flow catalog namespace
having exactly one top-level path component.

Catalog tenants consist of Unicode-normalized (NFKC) letters, numbers,
"-", "_", and "." and ending in a final "/".

For example: "acmeCo/" or "acmeCo.anvils/" or "acmeCo-TNT/",
but not "acmeCo" or "acmeCo/anvils/" or "acmeCo/TNT".
';


EOF

    cat ${DIR}/../migrations/11_stats.sql
    cat ${DIR}/../migrations/15_directives.sql
    cat ${DIR}/../migrations/16_tenants.sql

    cat <<EOF

-- Copied verbatim from seed.sql:

-- Public directive which allows a new user to provision a new tenant.
insert into directives (catalog_prefix, spec, token) values
  ('ops/', '{"type":"clickToAccept"}', 'd4a37dd7-1bf5-40e3-b715-60c4edd0f6dc'),
  ('ops/', '{"type":"betaOnboard"}', '453e00cd-e12a-4ce5-b12d-3837aa385751');

-- Provision the ops/ tenant owned by the accounts@estuary.dev user.
with accounts_root_user as (
  select (select id from auth.users where email = 'accounts@estuary.dev' limit 1) as accounts_id
)
insert into applied_directives (directive_id, user_id, user_claims)
  select d.id, a.accounts_id, '{"requestedTenant":"ops"}'
    from directives d, accounts_root_user a
    where catalog_prefix = 'ops/' and spec = '{"type":"betaOnboard"}';

-- We must clear out old storage mappings of the previous local seed.
-- The production database has 'ops/' (only), which will be deleted
-- and then re-created as the ops/ tenant is provisioned.
delete from storage_mappings where catalog_prefix in ('ops/', 'recovery/');

commit;
EOF
}

psql_input | psql postgres://postgres:postgres@localhost:5432/postgres

cat <<EOF
IMPORTANT: Be sure to update the ops/ storage mapping back to bucket estuary-flow-poc in production!

Commands to get started with directives:

# Auth as a user magic-link user on your local stack.
# You may need to hack up your UI repo to get passed the login wall. Talk to johnny.
cargo run -p flowctl -- auth develop --token TOKEN

# Turn in and "sign" click-to-accept terms.
cargo run -p flowctl -- raw rpc --function exchange_directive_token --body '{"bearer_token":"d4a37dd7-1bf5-40e3-b715-60c4edd0f6dc"}'
cargo run -p flowctl -- raw update --table applied_directives --body '{"user_claims":{"version":"1.2.3"}}' --query user_claims=is.null

# Provision a new tenant for your user.
cargo run -p flowctl -- raw rpc --function exchange_directive_token --body '{"bearer_token":"453e00cd-e12a-4ce5-b12d-3837aa385751"}'
cargo run -p flowctl -- raw update --table applied_directives --body '{"user_claims":{"requestedTenant":"AcmeCo"}}' --query user_claims=is.null

# Create a data-flow, and then spit out stats with:
cargo run -p flowctl -- raw get --table task_stats | jq '.[].flow_document'

EOF