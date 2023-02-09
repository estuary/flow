-- Refresh tokens used to generate access tokens
create table refresh_tokens (
  like internal._model including all,
  user_id    uuid references auth.users(id) not null,
  multi_use  boolean default false,
  valid_for  interval not null,
  hash       text not null
);

create policy "Users can access only their own refreshed_tokens"
  on refresh_tokens as permissive
  using (user_id = auth.uid());

grant select(id, user_id, multi_use, valid_for) on refresh_tokens to authenticated;

create type refresh_token_response as (
  refresh_token text
);

-- Generate a new refresh_token
create function gen_refresh_token(multi_use boolean, valid_for interval)
returns refresh_token_response as $$
declare
  token text;
  res refresh_token_response;
begin
  token = internal.id_generator();

  insert into refresh_tokens (user_id, multi_use, valid_for, hash)
  values (
    auth_uid(),
    multi_use,
    valid_for,
    crypt(token, gen_salt('md5'))
  );

  res.refresh_token = token;
  return res;
commit;
end
$$ language plpgsql volatile security definer;

-- Revoke a refresh_token given the token itself
create function revoke_refresh_token(token text)
returns void as $$
begin
  delete from refresh_tokens where hash = crypt(token, hash) and user_id = auth_uid();
end
$$ language plpgsql volatile security definer;

-- Revoke a refresh_token given the token_id
create function revoke_refresh_token(token_id flowid)
returns void as $$
begin
  delete from refresh_tokens where refresh_tokens.id = token_id and user_id = auth_uid();
end
$$ language plpgsql volatile security definer;

-- Returns the secret used for signing JWT tokens, with a default value for
-- local env, taken from https://github.com/supabase/supabase-js/issues/25#issuecomment-1019935888
create function internal.refresh_token_jwt_secret()
returns text as $$

  select coalesce(current_setting('app.settings.jwt_secret', true), 'super-secret-jwt-token-with-at-least-32-characters-long') limit 1

$$ language sql stable security definer;

-- When generating an access_token, if the refresh_token used is not multi-use,
-- then we will delete it and create a new one instead
create type access_token_response as (
  access_token text,
  refresh_token text
);

-- Given a refresh_token, generates a new access_token
-- if the refresh_token is not multi-use, it is deleted and a new
-- refresh_token is also created. If the refresh_token is multi-use, we reset
-- its validity period by updating its `updated_at` column
create function gen_access_token(refresh_token text)
returns access_token_response as $$
declare
  rt refresh_tokens;
  rt_new_id flowid;
  access_token text;
begin

  select * into rt from refresh_tokens where
    hash = crypt(refresh_token, hash) and
    (updated_at + valid_for) > now();
  if not found then
    raise 'invalid refresh token';
  end if;

  select sign(json_build_object(
    'exp', trunc(extract(epoch from (now() + interval '1 hour'))),
    'iat', trunc(extract(epoch from (now()))),
    'sub', rt.user_id,
    'role', 'authenticated'
  ), internal.refresh_token_jwt_secret()) into access_token
  limit 1;

  if rt.multi_use = false then
    delete from refresh_tokens where id = rt.id;
    rt_new_id = internal.id_generator();
    insert into refresh_tokens (user_id, multi_use, valid_for, hash) values (
      rt.user_id,
      rt.multi_use,
      rt.valid_for,
      crypt(rt_new_id::text, gen_salt('md5'))
    );
  else
    -- re-set the updated_at timer so the token's validity is refreshed
    update refresh_tokens set updated_at = now() where id = rt.id;
  end if;

  return (access_token, rt_new_id::text);
commit;
end
$$ language plpgsql volatile security definer;
