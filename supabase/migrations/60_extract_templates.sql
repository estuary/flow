begin;

alter table live_specs
add column journal_template_name text
generated always as (built_spec->'partitionTemplate'->>'name') stored;

alter table live_specs
add column shard_template_id text
generated always as (coalesce(
    built_spec->'shardTemplate'->>'id',
    built_spec->'derivation'->'shardTemplate'->>'id'
)) stored;

commit;