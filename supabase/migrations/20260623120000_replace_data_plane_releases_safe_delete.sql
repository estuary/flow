begin;

-- replace_data_plane_releases cleared the table with an unqualified DELETE. Postgres
-- rejects that under sql_safe_updates, which the PostgREST execution context enables,
-- raising "DELETE requires a WHERE clause"; the direct psql path did not enforce it.
-- Add an always-true predicate so the full-table replace still removes every row.
-- CREATE OR REPLACE keeps the existing execute grant to data_plane_releases_ci.
create or replace function public.replace_data_plane_releases(payload jsonb) returns integer
    language plpgsql
    as $$
declare
  inserted integer;
begin
  delete from data_plane_releases where true;

  insert into data_plane_releases (active, data_plane_id, max_tier, next_image, prev_image, step)
    select
      (e->>'active')::boolean,
      (e->>'data_plane_id')::public.flowid,
      (e->>'max_tier')::smallint,
      e->>'next_image',
      e->>'prev_image',
      (e->>'step')::integer
    from jsonb_array_elements(payload) as e;

  get diagnostics inserted = row_count;
  return inserted;
end
$$;

commit;
