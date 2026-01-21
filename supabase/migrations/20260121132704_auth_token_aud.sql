begin;

-- Updates the generate_access_token function to include the `aud` field in the token claims.

CREATE OR REPLACE FUNCTION public.generate_access_token(refresh_token_id public.flowid, secret text) RETURNS json
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
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
    'role', 'authenticated'
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


ALTER FUNCTION public.generate_access_token(refresh_token_id public.flowid, secret text) OWNER TO postgres;

COMMENT ON FUNCTION public.generate_access_token(refresh_token_id public.flowid, secret text) IS '
Given a refresh_token, generates a new access_token.
If the refresh_token is not multi-use, the token''s secret is rotated.
If the refresh_token is multi-use, we reset its validity period by updating its `updated_at` column
';


commit;
