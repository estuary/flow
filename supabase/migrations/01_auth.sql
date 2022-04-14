-- We write SQL according to https://www.sqlstyle.guide/
-- It's an arbitrary style guide, but it's important to have one for consistency.
-- We also lower-case SQL keywords, as is common within Supabase documentation.

-- Roles which are created by supabase:
--   create role if not exists anon;
--   create role if not exists authenticated;

-- A new supabase installation grants all in public to anon & authenticated.
-- We elect to NOT do this, instead explicitly granting access to tables and functions
alter default privileges in schema public revoke all on tables from anon, authenticated;
alter default privileges in schema public revoke all on routines from anon, authenticated;
alter default privileges in schema public revoke all on sequences from anon, authenticated;

-- Provide non-browser API clients a way to determine their effective user_id.
create function auth_uid()
returns uuid as $$
begin
  return auth.uid();
end;
$$ language plpgsql stable;
comment on function auth_uid is
  'auth_uid returns the user ID of the authenticated user';