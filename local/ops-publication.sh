#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

# Target this stack's default data plane. FLOW_CLUSTER is ambient inside a mise
# context (mise/tasks/local/stack-env); mise is mandatory.
DP_NAME="${FLOW_CLUSTER:?FLOW_CLUSTER must be set — run via 'mise run' (see local/README.md)}"

bundled_ops_catalog="$(sed "s/'/''/g" "${1}")"

cat << EOF
begin;
do \$\$
declare
    bundled_catalog_arg json := '${bundled_ops_catalog}';
    ops_user_id uuid;
    new_draft_id flowid := internal.id_generator();
    publication_id flowid := internal.id_generator();
begin
    -- Identify user that owns ops specifications.
    select id into strict ops_user_id from auth.users where email = 'support@estuary.dev';

    -- Create a draft of ops changes.
    insert into drafts (id, user_id, detail) values
    (new_draft_id, ops_user_id, 'publishing ops catalog for local development');

    -- Queue a publication of the draft.
    insert into publications (id, user_id, draft_id, data_plane_name) values
    (publication_id, ops_user_id, new_draft_id, 'ops/dp/public/${DP_NAME}');

    insert into draft_specs (draft_id, catalog_name, spec_type, spec)
    select new_draft_id, "key", 'collection'::catalog_spec_type, "value"
    from json_each(json_extract_path(bundled_catalog_arg, 'collections'))
    union all
    select new_draft_id, "key", 'materialization'::catalog_spec_type, "value"
    from json_each(json_extract_path(bundled_catalog_arg, 'materializations'))

	return;
end \$\$
language plpgsql;
commit;
EOF
