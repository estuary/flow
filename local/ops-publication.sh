#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

SOURCE_FILENAME="${1:-"template-local.flow.yaml"}"
bundled_ops_catalog="$(.build/package/bin/flowctl raw bundle --source ops-catalog/$SOURCE_FILENAME | sed "s/'/''/g")"

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

    -- These items must exist for the publication we are creating to succeed. The agent may decide
    -- to process this publication prior to the onboarding directive for the ops tenant, so this makes
    -- sure the publication can succeed in that case.
    insert into user_grants (user_id, object_role, capability) values
    (ops_user_id, 'ops.us-central1.v1/', 'admin')
    on conflict do nothing;
    insert into role_grants (subject_role, object_role, capability) values
    ('ops.us-central1.v1/', 'ops.us-central1.v1/', 'write')
    on conflict do nothing;
    insert into storage_mappings (catalog_prefix, spec) values
    ('ops.us-central1.v1/', '{"stores": [{"provider": "GCS", "bucket": "estuary-trial", "prefix": "collection-data/"}]}'),
    ('recovery/ops.us-central1.v1/', '{"stores": [{"provider": "GCS", "bucket": "estuary-trial"}]}')
    on conflict do nothing;

    -- Create a draft of ops changes.
    insert into drafts (id, user_id, detail) values
    (new_draft_id, ops_user_id, 'publishing ops catalog for local development');

    -- Queue a publication of the draft.
    insert into publications (id, user_id, draft_id, data_plane_name) values
    (publication_id, ops_user_id, new_draft_id, 'ops/dp/public/local-cluster');

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
