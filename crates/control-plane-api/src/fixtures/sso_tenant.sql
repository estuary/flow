do $$
declare
  data_plane_one_id flowid := '111111111111';

  alice_uid uuid := '11111111-1111-1111-1111-111111111111';
  bob_uid uuid := '22222222-2222-2222-2222-222222222222';
  carol_uid uuid := '33333333-3333-3333-3333-333333333333';

  acme_provider_id uuid := 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa';
  other_provider_id uuid := 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb';

  last_pub_id flowid := '000000000002';

begin

  -- SSO providers
  insert into auth.sso_providers (id) values
    (acme_provider_id),
    (other_provider_id)
  ;

  -- Users
  insert into auth.users (id, email) values
    (alice_uid, 'alice@acme.co'),
    (bob_uid, 'bob@other.co'),
    (carol_uid, 'carol@example.com')
  ;

  -- Alice has an SSO identity matching acme's provider.
  -- GoTrue stores SSO identities with provider = 'sso:<provider_id>'.
  insert into auth.identities (user_id, provider, provider_id) values
    (alice_uid, 'sso:' || acme_provider_id::text, acme_provider_id::text)
  ;

  -- Bob has an SSO identity, but for a different provider.
  insert into auth.identities (user_id, provider, provider_id) values
    (bob_uid, 'sso:' || other_provider_id::text, other_provider_id::text)
  ;

  -- Carol has no SSO identity (social login only).

  -- Tenants: acmeCo/ with SSO, openCo/ without SSO.
  insert into public.tenants (id, tenant, sso_provider_id) values
    (internal.id_generator(), 'acmeCo/', acme_provider_id),
    (internal.id_generator(), 'openCo/', null)
  ;

  -- Alice is admin on acmeCo/ and openCo/.
  insert into public.user_grants (user_id, object_role, capability) values
    (alice_uid, 'acmeCo/', 'admin'),
    (alice_uid, 'openCo/', 'admin')
  ;

  -- Give alice read on data planes so specs can resolve.
  insert into public.role_grants (subject_role, object_role, capability) values
    ('acmeCo/', 'ops/dp/public/', 'read')
  ;

end
$$;
