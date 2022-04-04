
do $$
begin
    if not exists (select from pg_catalog.pg_roles where rolname = 'anon') then
        create role anon nologin;
    end if;
end $$;

do $$
begin
    if not exists (select from pg_catalog.pg_roles where rolname = 'authenticated') then
        create role authenticated nologin;
    end if;
end $$;

-- This is a subset of the supabase-installed auth schema.
-- It's trimmed to only the bits we depend upon.
-- See: https://github.com/supabase/supabase/blob/master/docker/volumes/db/init/01-auth-schema.sql
create schema if not exists auth;

create table if not exists auth.users (
	id uuid not null unique,
	constraint users_pkey primary key (id)
);

create or replace function auth.uid() 
returns uuid 
language sql stable
as $$
  select 
  	coalesce(
		current_setting('request.jwt.claim.sub', true),
		(current_setting('request.jwt.claims', true)::jsonb ->> 'sub')
	)::uuid
$$;
