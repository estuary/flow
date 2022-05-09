-- Keys used to sign/verify gateway auth tokens.
create table internal.gateway_auth_keys (
  like internal._model including all,
  -- Key used to sign JWTs
  secret_key text
);

insert into internal.gateway_auth_keys (secret_key, detail) values (
  'supersecret', 'Used for development only. This value will be changed manually when deployed to production.'
);



-- Addresses of deployed data plane gateways. As we deploy into multiple
-- AZs/Regions, we can direct a caller to the appropriate Gateway for accessing
-- data in a region-aware way.
create table internal.gateway_endpoints (
  like internal._model including all,
  name text,
  url text
);

insert into internal.gateway_endpoints (name, url, detail) values (
  'local', 'http://localhost:28318/', 'Used for development only. This value will be changed manually when deployed to production.'
);



-- Returns the most appropriate gateway url. For now, there should only be one.
create function internal.gateway_endpoint_url()
returns text as $$

  select url
  from internal.gateway_endpoints
  limit 1

$$ language sql stable security definer;



-- Grabs the secret signing key and signs the object.
create function internal.sign_jwt(obj json)
returns text as $$

  select sign(obj, secret_key::text)
  from internal.gateway_auth_keys
  limit 1

$$ language sql stable security definer;



create function gateway_auth_token(variadic prefixes text[])
returns table (token text, gateway_url text) as $$

  with authorized_prefixes as (
    select p as prefix
    from auth_roles() as r, unnest(prefixes) as p
    where starts_with(p, r.role_prefix)
  )

  select internal.sign_jwt(
    json_build_object(
      'exp', trunc(extract(epoch from (now() + interval '1 hour'))),
      'iat', trunc(extract(epoch from (now()))),
      'operation', 'read',
      'prefixes', array_agg(distinct ap.prefix),
      'sub', auth_uid()
    )
  ) as token, internal.gateway_endpoint_url() as gateway_url
  from authorized_prefixes as ap

$$ language sql stable security definer;

comment on function gateway_auth_token is
  'gateway_auth_token returns a jwt that can be used with the Data Plane Gateway to interact directly with Gazette RPCs.';
