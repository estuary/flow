# catalog-stats

Read-side client for the BigTable tables that hold rolled-up catalog stats.

## Inspecting a local stack

The local `mise run local:stack` workflow materializes the same docs to
both BigTable (via this crate's tables) and Postgres (`catalog_stats`
table populated by `ops.us-central1.v1/stats-view`). The two are useful
to cross-check during development; the snippets below assume the default
local config.

### BigTable

`cbt` runs out of the `google/cloud-sdk` image and talks to the emulator
exposed by `mise run local:bigtable` on `localhost:8086`:

```sh
cbt() {
    docker run --rm --network host \
        -e BIGTABLE_EMULATOR_HOST=localhost:8086 \
        -e CLOUDSDK_CORE_PROJECT=estuary-local \
        google/cloud-sdk:latest \
        cbt -instance estuary-local "$@"
}

# List the three grain tables.
cbt ls

# Read the first few rows of a grain, restricted to the flow_document column.
cbt read catalog_stats_hourly columns=f:flow_document count=5
```

cbt's `prefix=` / `start=` / `end=` arguments match against the raw row
key, which is FDB tuple-packed and begins with a `0x02` type byte — so a
literal `prefix=bobCo/` will NOT match. For ad-hoc filtering by catalog
name, it's easier to scan the table and post-filter the decoded JSON
(see the `jq` example below).

The `flow_document` cell is printed Go-`%q`-quoted on a 4-space-indented
line; for the ASCII JSON we store, that quoting is also a valid JSON
string, so `jq fromjson` recovers the document:

```sh
cbt read catalog_stats_hourly columns=f:flow_document count=1 \
    | grep -aE '^    "' | sed 's/^    //' | jq 'fromjson'
```

### Postgres

The same docs land in the `catalog_stats` table under the local
`stats_loader` role:

```sh
psql() {
    PGPASSWORD=stats_loader_password command psql \
        -h localhost -U stats_loader -d postgres "$@"
}

# A few recent rows, projected columns.
psql -c "
  SELECT catalog_name, grain, ts, docs_written_by_me, bytes_written_by_me
    FROM catalog_stats
   ORDER BY ts DESC
   LIMIT 10"

# The full flow_document for a specific row.
psql -At -c "
  SELECT jsonb_pretty(flow_document::jsonb)
    FROM catalog_stats
   WHERE catalog_name = 'bobCo/hw1/'
     AND grain        = 'hourly'
     AND ts           = '2026-05-14T18:00:00Z'"
```

### Comparing

The two stores will usually agree (sans small propagation delay) on every
field except `_meta.uuid`, which each materialization assigns
independently. 

**Full dump.** Normalize both sides to one JSON-per-line, sort, and
diff. Identical rows produce byte-identical lines after `del(._meta)`
and key-sorting (`-S`), so `sort` lines them up without an explicit
join:

```sh
for grain in hourly daily monthly; do
    cbt read "catalog_stats_${grain}" columns=f:flow_document
done \
    | grep -aE '^    "' | sed 's/^    //' \
    | jq -cS 'fromjson | del(._meta)' | sort > /tmp/bt.jsonl

psql -At -c "SELECT flow_document::text FROM catalog_stats" \
    | jq -cS 'del(._meta)' | sort > /tmp/pg.jsonl

diff /tmp/bt.jsonl /tmp/pg.jsonl
```
