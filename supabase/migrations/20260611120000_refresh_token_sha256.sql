begin;

-- Migrate refresh-token secret hashing from bcrypt to SHA-256.
--
-- Refresh-token secrets are high-entropy random values (random UUIDs at ~122
-- bits historically; 256-bit random hex going forward), so bcrypt's slow
-- hashing adds no protection against offline brute force — that only matters
-- for low-entropy passwords — while costing ~1ms of CPU on every
-- verification. The bearer-credential path validates on every request (e.g.
-- OpenMetrics scrapers), so verification cost matters. This matches how API
-- key secrets are hashed.
--
-- Existing bcrypt hashes cannot be rewritten in place (we don't hold the
-- plaintexts), so validators dual-read by hash format: bcrypt hashes start
-- with '$2', hex SHA-256 digests are 64 hex characters. A successful bcrypt
-- verification rewrites the row to SHA-256 while the plaintext is in hand
-- ("rehash on use"). This rewrite is load-bearing for the migration's
-- completion: multi-use tokens re-arm their validity window on every use, so
-- an actively-used bcrypt token would otherwise never expire and the bcrypt
-- branch could never be removed. With it, bcrypt rows drain via rehash or
-- expiry, after which the bcrypt branches can be dropped — planned alongside
-- retiring this function in favor of /api/v1/auth/token.
--
-- The Rust bearer-authentication path (control-plane-api
-- authenticate_refresh_token) implements the same dual-read and rehash.

CREATE OR REPLACE FUNCTION public.create_refresh_token(multi_use boolean, valid_for interval, detail text DEFAULT NULL::text) RETURNS json
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare
  secret text;
  refresh_token_row refresh_tokens;
begin
  -- 256 bits from pgcrypto's CSPRNG, up from a UUID's 122. The fast-hash
  -- rationale above rests on secrets being high-entropy; hex also reads as
  -- an opaque secret rather than a UUID-shaped identifier.
  secret = encode(gen_random_bytes(32), 'hex');

  insert into refresh_tokens (detail, user_id, multi_use, valid_for, hash)
  values (
    detail,
    auth_uid(),
    multi_use,
    valid_for,
    encode(digest(secret, 'sha256'), 'hex')
  ) returning * into refresh_token_row;

  return json_build_object(
    'id', refresh_token_row.id,
    'secret', secret
  );
commit;
end
$$;

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

  if rt.hash like '$2%' then
    -- Legacy bcrypt hash; rewritten to SHA-256 on successful use below.
    if rt.hash <> crypt(secret, rt.hash) then
      raise 'invalid secret provided';
    end if;
  else
    if rt.hash <> encode(digest(secret, 'sha256'), 'hex') then
      raise 'invalid secret provided';
    end if;
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
    rt_new_secret = encode(gen_random_bytes(32), 'hex');
    update refresh_tokens
      set
        hash = encode(digest(rt_new_secret, 'sha256'), 'hex'),
        uses = (uses + 1),
        updated_at = clock_timestamp()
      where refresh_tokens.id = rt.id;
  else
    -- Re-set the updated_at timer so the token's validity is refreshed,
    -- and rehash a legacy bcrypt secret while the plaintext is in hand.
    update refresh_tokens
      set
        hash = case when hash like '$2%'
                    then encode(digest(secret, 'sha256'), 'hex')
                    else hash end,
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
