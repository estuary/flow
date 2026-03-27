-- SSO enforcement: per-tenant provider checks in user_roles().
create function tests.test_sso_enforcement()
returns setof text as $$
declare
  provider_acme uuid = 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa';
  provider_bigcorp uuid = 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb';
begin

  -- Create two SSO providers.
  insert into auth.sso_providers (id) values (provider_acme), (provider_bigcorp);

  -- Create tenants: acmeCo enforces SSO, bigcorpCo enforces SSO, openCo does not.
  delete from tenants;
  insert into tenants (tenant, sso_provider_id, enforce_sso) values
    ('acmeCo/', provider_acme, true),
    ('bigcorpCo/', provider_bigcorp, true),
    ('openCo/', null, false);

  -- Alice has grants on all three tenants.
  delete from user_grants;
  insert into user_grants (user_id, object_role, capability) values
    ('11111111-1111-1111-1111-111111111111', 'acmeCo/', 'admin'),
    ('11111111-1111-1111-1111-111111111111', 'bigcorpCo/', 'read'),
    ('11111111-1111-1111-1111-111111111111', 'openCo/', 'admin');

  -- Give Alice an SSO identity for Acme only.
  delete from auth.identities where user_id = '11111111-1111-1111-1111-111111111111';
  insert into auth.identities (user_id, provider, provider_id, identity_data) values
    ('11111111-1111-1111-1111-111111111111', 'sso:' || provider_acme::text, provider_acme::text, '{}'::jsonb);

  -- Alice sees acmeCo (matching SSO) and openCo (no SSO enforced),
  -- but NOT bigcorpCo (enforces SSO with a different provider).
  return next results_eq(
    $i$ select role_prefix::text, capability::text
        from internal.user_roles('11111111-1111-1111-1111-111111111111') $i$,
    $i$ values ('acmeCo/', 'admin'), ('openCo/', 'admin') $i$,
    'SSO user sees matching SSO tenant + open tenant, not mismatched SSO tenant'
  );

  -- Bob has a grant on acmeCo but no SSO identity at all.
  insert into user_grants (user_id, object_role, capability) values
    ('22222222-2222-2222-2222-222222222222', 'acmeCo/', 'read'),
    ('22222222-2222-2222-2222-222222222222', 'openCo/', 'read');

  delete from auth.identities where user_id = '22222222-2222-2222-2222-222222222222';

  -- Bob only sees openCo.
  return next results_eq(
    $i$ select role_prefix::text, capability::text
        from internal.user_roles('22222222-2222-2222-2222-222222222222') $i$,
    $i$ values ('openCo/', 'read') $i$,
    'non-SSO user excluded from SSO-enforced tenant'
  );

  -- Hypothetical: GoTrue doesn't support multiple SSO identities per user
  -- today, but verify user_roles() behaves correctly if that ever changes.
  insert into auth.identities (user_id, provider, provider_id, identity_data) values
    ('11111111-1111-1111-1111-111111111111', 'sso:' || provider_bigcorp::text, provider_bigcorp::text, '{}'::jsonb);

  return next results_eq(
    $i$ select role_prefix::text, capability::text
        from internal.user_roles('11111111-1111-1111-1111-111111111111') $i$,
    $i$ values ('acmeCo/', 'admin'), ('bigcorpCo/', 'read'), ('openCo/', 'admin') $i$,
    'user with both SSO identities sees both SSO-enforced tenants'
  );

  delete from auth.identities
    where user_id = '11111111-1111-1111-1111-111111111111'
      and provider = 'sso:' || provider_bigcorp::text;

  -- Transitive grant: Alice has admin on bigcorpCo which projects through
  -- a role_grant to acmeCo/shared/. Since Alice lacks BigCorp SSO, the base
  -- grant is excluded and the transitive grant is unreachable.
  delete from user_grants where user_id = '11111111-1111-1111-1111-111111111111';
  insert into user_grants (user_id, object_role, capability) values
    ('11111111-1111-1111-1111-111111111111', 'bigcorpCo/', 'admin');

  delete from role_grants;
  insert into role_grants (subject_role, object_role, capability) values
    ('bigcorpCo/', 'acmeCo/shared/', 'read');

  return next is_empty(
    $i$ select role_prefix::text, capability::text
        from internal.user_roles('11111111-1111-1111-1111-111111111111') $i$,
    'transitive grants through SSO-enforced tenant are excluded when provider mismatches'
  );

  -- Tenant with sso_provider_id set but enforce_sso = false should NOT filter.
  update tenants set enforce_sso = false where tenant = 'bigcorpCo/';

  return next results_eq(
    $i$ select role_prefix::text, capability::text
        from internal.user_roles('11111111-1111-1111-1111-111111111111') $i$,
    $i$ values ('acmeCo/shared/', 'read'), ('bigcorpCo/', 'admin') $i$,
    'enforce_sso=false does not filter even when sso_provider_id is set'
  );

  return;
end
$$ language plpgsql;
