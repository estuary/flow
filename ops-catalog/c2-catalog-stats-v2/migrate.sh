#!/usr/bin/env bash
#
# Migrates c2's catalog-stats L1 derivation to runtime-v2, handing over at time
# T, the script's argument. It:
# - sets `notAfter: T` on the old L1, pins it to runtime-v1 with
#   `enable-runtime-v2: "false"`, and publishes it;
# - creates the new L1, `.../catalog-stats-v2`, with `notBefore: T`, and
#   publishes it;
# - sets c2's `ops_l1_stats_name` in `data_planes` to the new L1;
# - adds a `.v2` suffix to c2's `ops_l2_stats_transform`, because the old L1's
#   transform keeps the original name.

set -euo pipefail

T="${1:?usage: migrate.sh T, with T an RFC 3339 UTC time such as 2026-12-01T18:00:00Z}"

DATA_PLANE=ops/dp/public/gcp-us-central1-c2
BASE_NAME=public/gcp-us-central1-c2
OLD=ops/rollups/L1/public/gcp-us-central1-c2/catalog-stats
NEW=ops/rollups/L1/public/gcp-us-central1-c2/catalog-stats-v2

TEMPLATE="$(cd "$(dirname "$0")/.." && pwd)/data-plane-template.bundle.json"
cd "$(mktemp -d)"

echo "==> Setting the old L1 to stop at $T."
flowctl catalog pull-specs --name "$OLD" --flat --target pulled.flow.json
jq --arg name "$OLD" --arg t "$T" '
    .collections[$name].derive.transforms[].source.notAfter = $t |
    .collections[$name].derive.shards.flags["enable-runtime-v2"] = "false"' \
    pulled.flow.json >old.flow.json
flowctl catalog publish --source old.flow.json

echo "==> Creating the new L1, starting at $T."
sed -e "s#ops/rollups/L1/BASE_NAME/catalog-stats#$NEW#g" -e "s#BASE_NAME#$BASE_NAME#g" "$TEMPLATE" |
    jq --arg name "$NEW" --arg t "$T" \
        '{collections: {($name): (.collections[$name] | .derive.transforms[].source.notBefore = $t)}}' \
        >new.flow.json
flowctl catalog publish --source new.flow.json --init-data-plane "$DATA_PLANE"

echo "==> Pointing c2's row at the new L1, and adding .v2 to its L2 stats transform name."
psql "${DATABASE_URL:?}" -v ON_ERROR_STOP=1 -c "
    update data_planes
    set ops_l1_stats_name = '$NEW',
        ops_l2_stats_transform = ops_l2_stats_transform || '.v2'
    where data_plane_name = '$DATA_PLANE' and ops_l1_stats_name = '$OLD'
    returning ops_l1_stats_name, ops_l2_stats_transform;"
