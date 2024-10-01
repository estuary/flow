# Compaction

To produce a new compaction of the production database, run:

```bash
  pg_dump ${DATABASE} \
    --exclude-table=public.flow_checkpoints_v1 \
    --exclude-table=public.flow_materializations_v2 \
    --schema internal \
    --schema public \
    --schema-only \
  | grep -v "CREATE SCHEMA public;" \
  | grep -v "ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin"
```