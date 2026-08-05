begin;

-- Lets a refresh token be confined to a catalog prefix. generate_access_token stamps
-- the prefix into the access token's `scope_prefix` claim, and control-plane-api
-- resolves that claim into a `tables::AuthScope` which intersects every authorization
-- answer with the authority reachable from the prefix through role_grants.
--
-- The scope lives on the token row rather than being requested at exchange time so
-- that whoever holds the credential cannot re-scope it. This matters for the case the
-- feature exists for: a credential handed to an agent should be confined by whoever
-- minted it, not by whoever presents it.
--
-- The claim carries only the prefix, never a materialized list of authorized prefixes.
-- Authority is still derived from the grant tables per request, so revoking a grant
-- takes effect on the next authorization Snapshot regardless of the token's remaining
-- lifetime. Freezing the scope for the token's lifetime is safe because a scope can
-- only narrow: a stale scope cannot authorize anything the user could not do unscoped.
alter table public.refresh_tokens
  add column scope_prefix public.catalog_prefix;

comment on column public.refresh_tokens.scope_prefix is
  'Optional catalog prefix stamped into the access token `scope_prefix` claim by '
  'generate_access_token. Null yields an unscoped token. When set, every authorization '
  'decision made with the token is intersected with the authority reachable from this '
  'prefix through role_grants, so the token can only ever do less than its owner could.';

-- Identical to the prior definition except that a non-null scope_prefix is added to the
-- claims. Unscoped tokens (scope_prefix null) emit exactly the claims they did before.
create or replace function public.generate_access_token(refresh_token_id public.flowid, secret text) returns json
    language plpgsql security definer
    as $$
declare
  rt refresh_tokens;
  rt_new_secret text;
  claims jsonb;
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

  claims = jsonb_build_object(
    'exp', trunc(extract(epoch from (now() + interval '1 hour'))),
    'iat', trunc(extract(epoch from (now()))),
    'sub', rt.user_id,
    'aud', 'authenticated',
    'role', coalesce(rt.pg_role, 'authenticated')
  );

  if rt.scope_prefix is not null then
    claims = claims || jsonb_build_object('scope_prefix', rt.scope_prefix);
  end if;

  select sign(claims::json, internal.access_token_jwt_secret()) into access_token
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

commit;
