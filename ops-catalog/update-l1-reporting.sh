#!/usr/bin/env bash
set -euo pipefail

# One-off script to re-publish the catalog-stats and stats collections
# for all data planes, after updating data-plane-template.bundle.json.
# Based on the mise/tasks/local/stack script.

DB_URL="${DB_URL:-postgresql://postgres:postgres@localhost:5432/postgres}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEMPLATE=$(cat "$SCRIPT_DIR/data-plane-template.bundle.json" | sed "s/'/''/g")

# Only data planes in this list will be updated. All others are skipped.
APPLICABLE_DATA_PLANES=(
  "ops/dp/public/not-a-real-dataplane"
  "ops/dp/public/local-cluster"
)

ALL_DATA_PLANES=$(psql "$DB_URL" -t -A -c \
  "select data_plane_name from data_planes order by data_plane_name")

# Warn about any entries in APPLICABLE_DATA_PLANES that don't exist in the database.
for WANTED in "${APPLICABLE_DATA_PLANES[@]}"; do
  if ! echo "$ALL_DATA_PLANES" | grep -qx "$WANTED"; then
    echo "Warning: '$WANTED' is not a known data plane" >&2
  fi
done

for DATA_PLANE in $ALL_DATA_PLANES; do

  if ! printf '%s\n' "${APPLICABLE_DATA_PLANES[@]}" | grep -qx "$DATA_PLANE"; then
    echo "Skipping data plane: $DATA_PLANE"
    continue
  fi

  BASE_NAME="${DATA_PLANE#ops/dp/}"

  echo "Updating L1 reporting for data plane: $BASE_NAME"

  CATALOG=$(echo "$TEMPLATE" | sed "s|BASE_NAME|$BASE_NAME|g")

  psql "$DB_URL" <<EOF
begin;
do \$\$
declare
    bundled_catalog_arg json := '$CATALOG';
    ops_user_id uuid;
    new_draft_id flowid := internal.id_generator();
    publication_id flowid := internal.id_generator();
begin
    -- Identify user that owns ops specifications.
    select id into strict ops_user_id from auth.users where email = 'support@estuary.dev';

    -- Create a draft of ops changes.
    insert into drafts (id, user_id, detail) values
    (new_draft_id, ops_user_id, 'updating L1 reporting for $BASE_NAME');

    -- Queue a publication of the draft.
    insert into publications (id, user_id, draft_id, detail, data_plane_name) values
    (publication_id, ops_user_id, new_draft_id, 'updating L1 reporting for $BASE_NAME', '$DATA_PLANE');

    insert into draft_specs (draft_id, catalog_name, spec_type, spec)
    select new_draft_id, "key", 'collection'::catalog_spec_type, "value"
    from json_each(json_extract_path(bundled_catalog_arg, 'collections'))
    where "key" like '%/catalog-stats' or "key" like '%/stats';
end \$\$
language plpgsql;
commit;
EOF

  echo "Done: $BASE_NAME. Sleeping 30 seconds..."
  sleep 30
done
