-- Refresh tokens used to generate access tokens
create table refresh_tokens (
  like internal._model including all,
  user_id    uuid references auth.users(id) not null,
  multi_use  boolean default false,
  valid_for  interval not null,
  uses       int default 0,
  hash       text not null
);

create policy "Users can access only their own refreshed_tokens"
  on refresh_tokens as permissive
  using (user_id = auth.uid());

grant select(id, created_at, detail, updated_at, user_id, multi_use, valid_for, uses) on refresh_tokens to authenticated;
grant update(detail) on refresh_tokens to authenticated;
grant delete on refresh_tokens to authenticated;

-- Create a new refresh_token
create function create_refresh_token(multi_use boolean, valid_for interval, detail text default null)
returns json as $$
declare
  secret text;
  refresh_token_row refresh_tokens;
begin
  secret = gen_random_uuid();

  insert into refresh_tokens (detail, user_id, multi_use, valid_for, hash)
  values (
    detail,
    auth_uid(),
    multi_use,
    valid_for,
    crypt(secret, gen_salt('bf'))
  ) returning * into refresh_token_row;

  return json_build_object(
    'secret', secret,
    'id', refresh_token_row.id
  );
commit;
end
$$ language plpgsql volatile security definer;

-- Returns the secret used for signing JWT tokens, with a default value for
-- local env, taken from https://github.com/supabase/supabase-js/issues/25#issuecomment-1019935888
create function internal.access_token_jwt_secret()
returns text as $$

  select coalesce(current_setting('app.settings.jwt_secret', true), 'super-secret-jwt-token-with-at-least-32-characters-long') limit 1

$$ language sql stable security definer;

-- Given a refresh_token, generates a new access_token
-- if the refresh_token is not multi-use, it is deleted and a new
-- refresh_token is also created. If the refresh_token is multi-use, we reset
-- its validity period by updating its `updated_at` column
create function generate_access_token(id flowid, secret text)
returns json as $$
declare
  rt refresh_tokens;
  rt_new_secret text;
  access_token text;
begin

  select * into rt from refresh_tokens where
    refresh_tokens.id = generate_access_token.id and
    hash = crypt(secret, hash) and
    (updated_at + valid_for) > now();
  if not found then
    raise 'invalid refresh token';
  end if;

  select sign(json_build_object(
    'exp', trunc(extract(epoch from (now() + interval '1 hour'))),
    'iat', trunc(extract(epoch from (now()))),
    'sub', rt.user_id,
    'role', 'authenticated'
  ), internal.access_token_jwt_secret()) into access_token
  limit 1;

  if rt.multi_use = false then
    rt_new_secret = gen_random_uuid();
    update refresh_tokens
      set
        hash = crypt(rt_new_secret, gen_salt('bf')),
        uses = (uses + 1)
      where refresh_tokens.id = rt.id;
  else
    -- re-set the updated_at timer so the token's validity is refreshed
    update refresh_tokens set uses = (uses + 1) where refresh_tokens.id = rt.id;
  end if;

  return json_build_object(
    'access_token', access_token,
    'refresh_token', json_build_object(
      'id', rt.id,
      'secret', rt_new_secret
      )
  );
commit;
end
$$ language plpgsql volatile security definer;
