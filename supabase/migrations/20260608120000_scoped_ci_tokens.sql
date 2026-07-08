begin;

-- Lets specific database roles be reached over PostgREST using a revocable refresh
-- token, instead of a direct psql/5432 connection. This is so that CI writes which
-- currently connect directly to the database survive the upcoming network allowlist,
-- which cannot cover GitHub-hosted runner IPs.
--
-- A refresh token may carry an optional `pg_role`. generate_access_token stamps it
-- into the access token's `role` claim, and PostgREST uses that claim for SET ROLE.
-- The credential is revoked by deleting the single refresh_tokens row, which is why
-- this is preferred over a long-lived JWT signed with the (effectively unrotatable)
-- shared JWT secret.

-- Null preserves all existing behavior; only machine credentials set this.
alter table public.refresh_tokens
  add column pg_role text;

comment on column public.refresh_tokens.pg_role is
  'Optional Postgres role stamped into the access token `role` claim by generate_access_token. '
  'Null yields the default ''authenticated'' role. Used for scoped, revocable machine credentials.';

-- Identical to the prior definition except the `role` claim, which now honors a
-- token's pg_role. When pg_role is null the emitted claims are byte-for-byte unchanged,
-- so the shared user-authentication path is unaffected.
create or replace function public.generate_access_token(refresh_token_id public.flowid, secret text) returns json
    language plpgsql security definer
    as $$
declare
  rt refresh_tokens;
  rt_new_secret text;
  access_token text;
begin

  select * into rt from refresh_tokens where
    refresh_tokens.id = refresh_token_id;

  if not found then
    raise 'could not find refresh_token with the given `refresh_token_id`';
  end if;

  if rt.hash <> crypt(secret, rt.hash) then
    raise 'invalid secret provided';
  end if;

  if (rt.updated_at + rt.valid_for) < now() then
    raise 'refresh_token has expired.';
  end if;

  select sign(json_build_object(
    'exp', trunc(extract(epoch from (now() + interval '1 hour'))),
    'iat', trunc(extract(epoch from (now()))),
    'sub', rt.user_id,
    'aud', 'authenticated',
    'role', coalesce(rt.pg_role, 'authenticated')
  ), internal.access_token_jwt_secret()) into access_token
  limit 1;

  if rt.multi_use = false then
    rt_new_secret = gen_random_uuid();
    update refresh_tokens
      set
        hash = crypt(rt_new_secret, gen_salt('bf')),
        uses = (uses + 1),
        updated_at = clock_timestamp()
      where refresh_tokens.id = rt.id;
  else
    -- re-set the updated_at timer so the token's validity is refreshed
    update refresh_tokens
      set
        uses = (uses + 1),
        updated_at = clock_timestamp()
      where refresh_tokens.id = rt.id;
  end if;

  if rt_new_secret is null then
    return json_build_object(
      'access_token', access_token
    );
  else
    return json_build_object(
      'access_token', access_token,
      'refresh_token', json_build_object(
        'id', rt.id,
        'secret', rt_new_secret
        )
    );
  end if;
commit;
end
$$;

-- A dedicated, grant-less identity to own CI refresh tokens. Because it holds no
-- user_grants or role_grants, an access token minted for it is inert if presented to
-- agent-api (it authenticates as a user with no capabilities); its only use is the
-- PostgREST role assumption granted below. The refresh_tokens.user_id foreign key
-- requires a real auth.users row, mirroring the support@estuary.dev fixture in seed.sql.
insert into auth.users (id, email)
  values ('c100b07b-0000-0000-0000-000000000001', 'ci-bot@estuary.dev')
  on conflict (id) do nothing;

-- Mint a revocable, multi-use refresh token bound to a scoped role, for CI use over
-- PostgREST. Admin-gated and restricted to an explicit allowlist; without the allowlist
-- this would be a privilege-escalation path (any caller could mint a service_role token).
-- Returns {id, secret}; revoke by deleting the refresh_tokens row.
create function public.create_scoped_refresh_token(target_role text, valid_for interval, detail text default null)
    returns json
    language plpgsql security definer
    as $$
declare
  new_secret text;
  rt refresh_tokens;
  service_user_id constant uuid = 'c100b07b-0000-0000-0000-000000000001';
begin
  if not exists (
    select 1 from internal.user_roles(auth_uid(), 'admin')
    where role_prefix = 'ops/'
  ) then
    raise 'must be an admin of the ops/ tenant to create a scoped refresh token';
  end if;

  if target_role not in ('github_action_connector_refresh', 'data_plane_releases_ci') then
    raise 'target_role % is not an allowlisted scoped role', target_role;
  end if;

  new_secret = gen_random_uuid();

  insert into refresh_tokens (detail, user_id, multi_use, valid_for, hash, pg_role)
  values (
    detail,
    service_user_id,
    true,
    valid_for,
    crypt(new_secret, gen_salt('bf')),
    target_role
  ) returning * into rt;

  return json_build_object('id', rt.id, 'secret', new_secret);
end
$$;

alter function public.create_scoped_refresh_token(text, interval, text) owner to postgres;

comment on function public.create_scoped_refresh_token(text, interval, text) is
  'Admin-only. Create a revocable, multi-use refresh token bound to an allowlisted scoped '
  'Postgres role, for machine (CI) use over PostgREST. Revoke by deleting the refresh_tokens row.';

revoke execute on function public.create_scoped_refresh_token(text, interval, text) from public;
grant execute on function public.create_scoped_refresh_token(text, interval, text) to authenticated;

-- Allow PostgREST (which logs in as `authenticator`) to SET ROLE to these scoped roles.
-- This is the wiring that makes the scoped access token usable; it mirrors how `dekaf`
-- is already a member of authenticator. SET ROLE evaluates privileges and RLS exactly as
-- a direct login as the role would, so behavior matches the current psql path.
grant github_action_connector_refresh to authenticator;
grant data_plane_releases_ci to authenticator;

-- Re-queue connector tag publishing. SECURITY INVOKER: runs as the assumed role
-- (github_action_connector_refresh), which already holds UPDATE on connector_tags and
-- SELECT on connectors. The AFTER UPDATE trigger create_connector_tag_task then enqueues
-- the agent task, exactly as the prior direct UPDATE did. Returns the number of rows requeued.
create function public.requeue_connector_tag(image_name text, image_tags text[]) returns integer
    language plpgsql
    as $$
declare
  requeued integer;
begin
  update connector_tags ct
    set job_status = '{"type": "queued"}', updated_at = clock_timestamp()
    where ct.connector_id in (
            select c.id from connectors c where c.image_name = requeue_connector_tag.image_name
          )
      and ct.image_tag = any(image_tags);
  get diagnostics requeued = row_count;
  return requeued;
end
$$;

comment on function public.requeue_connector_tag(text, text[]) is
  'Re-queue publishing of the given connector image tags by setting job_status to queued. '
  'Replaces the direct UPDATE that connector CI ran over psql.';

revoke execute on function public.requeue_connector_tag(text, text[]) from public;
grant execute on function public.requeue_connector_tag(text, text[]) to github_action_connector_refresh;

-- Replace the full data_plane_releases set in one transaction. SECURITY INVOKER: runs as
-- data_plane_releases_ci, which holds DELETE and INSERT on the table. PostgREST has no COPY,
-- so the ops workflow's CSV becomes a JSON array of release rows. Returns the number inserted.
create function public.replace_data_plane_releases(payload jsonb) returns integer
    language plpgsql
    as $$
declare
  inserted integer;
begin
  delete from data_plane_releases;

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

comment on function public.replace_data_plane_releases(jsonb) is
  'Atomically replace all data_plane_releases rows from a JSON array. '
  'Replaces the DELETE + COPY that the ops release-data-planes workflow ran over psql.';

revoke execute on function public.replace_data_plane_releases(jsonb) from public;
grant execute on function public.replace_data_plane_releases(jsonb) to data_plane_releases_ci;

commit;
