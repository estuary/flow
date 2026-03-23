-- Sets up SSO-enforced tenants with users whose identities do/don't match.
-- Alice has an SSO identity matching acmeCo's provider — her grant should be included.
-- Bob has an SSO identity for a *different* provider — his grant should be excluded.
-- Carol has no SSO identity at all — her grant should be excluded.
-- All three have grants on openCo (no SSO enforcement) — those should be included.
do $$
declare
  provider_acme uuid := 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa';
  provider_other uuid := 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb';

  alice_uid uuid := '11111111-1111-1111-1111-111111111111';
  bob_uid uuid := '22222222-2222-2222-2222-222222222222';
  carol_uid uuid := '33333333-3333-3333-3333-333333333333';
begin

  insert into auth.sso_providers (id) values (provider_acme), (provider_other);

  insert into auth.users (id, email, is_sso_user) values
    (alice_uid, 'alice@acme.co', true),
    (bob_uid, 'bob@other.co', true),
    (carol_uid, 'carol@example.com', false)
  ;

  -- Alice has the correct SSO identity for acmeCo's provider.
  insert into auth.identities (user_id, provider, provider_id, identity_data) values
    (alice_uid, 'sso', provider_acme::text, '{}'::jsonb),
    (bob_uid, 'sso', provider_other::text, '{}'::jsonb)
  ;

  insert into public.tenants (tenant, sso_provider_id, enforce_sso) values
    ('acmeCo/', provider_acme, true),
    ('openCo/', null, false)
  ;

  insert into public.user_grants (user_id, object_role, capability) values
    (alice_uid, 'acmeCo/', 'admin'),
    (bob_uid, 'acmeCo/', 'admin'),
    (carol_uid, 'acmeCo/', 'admin'),
    (alice_uid, 'openCo/', 'read'),
    (bob_uid, 'openCo/', 'read'),
    (carol_uid, 'openCo/', 'read')
  ;

end
$$;
