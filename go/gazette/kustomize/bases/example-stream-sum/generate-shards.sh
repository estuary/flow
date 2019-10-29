#!/bin/bash -e

readonly STORES=(rocksdb sqlite)

cat <<EOF
# Create one shard for each journal & recoverylog hard-coded in test/examples.journalspace.yaml
# Alternate the choice of store implementation with each shard.
# Compare to ShardSpec for field definitions.
common:
  recovery_log_prefix: examples/stream-sum/recovery-logs
  hint_prefix:         /gazette/hints/examples/stream-sum
  hint_backups:        2
  max_txn_duration:    1s
  hot_standbys:        1
shards:
EOF

for i in $(seq -f "%03g" 0 7); do
  cat <<EOF
- id: chunks-part-${i}
  sources: [journal: examples/stream-sum/chunks/part-${i}]
  labels:
    - name:  store
      value: ${STORES[${i}%2]}
EOF
done
