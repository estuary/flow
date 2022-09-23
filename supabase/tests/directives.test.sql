
create function tests.test_directives()
returns setof text as $$
begin

  -- Replace seed grants with fixtures for this test.
  delete from user_grants;
  insert into user_grants (user_id, object_role, capability) values
    ('11111111-1111-1111-1111-111111111111', 'aliceCo/', 'admin'),
    ('22222222-2222-2222-2222-222222222222', 'bobCo/', 'admin')
  ;
  delete from role_grants;
  insert into role_grants (subject_role, object_role, capability) values
    ('aliceCo/', 'otherCo/', 'write'),
    ('aliceCo/','bobCo/',  'read');

  insert into directives (catalog_prefix, spec, token, single_use) values
    ('aliceCo/', '{"type": "alice"}', 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', true),
    ('bobCo/', '{"type": "bob"}',   'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb', false);

  -- We're authorized as Alice.
  set role authenticated;
  set request.jwt.claim.sub to '11111111-1111-1111-1111-111111111111';

  -- We see only Alice's directive (which we admin), and not Bob's (despite our read grant).
  return query select results_eq(
    $i$ select catalog_prefix::text, spec::text from directives order by catalog_prefix $i$,
    $i$ values  ('aliceCo/','{"type": "alice"}') $i$,
    'alice directive'
  );

  -- Turn in the Alice directive bearer token to apply it.
  perform exchange_directive_token('aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa');

  return query select results_eq(
    $i$
    select d.catalog_prefix::text, d.token::text, a.user_id, a.user_claims::text
    from directives d join applied_directives a on a.directive_id = d.id
    order by d.catalog_prefix;
    $i$,
    $i$ values ('aliceCo/', null, auth.uid(), null) $i$,
    'alice directive is applied and its token is reset'
  );

  -- Turn in the Bob bearer token to apply it.
  perform exchange_directive_token('bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb');

  return query select results_eq(
    $i$ select catalog_prefix::text, spec::text from directives order by catalog_prefix $i$,
    $i$ values  ('aliceCo/','{"type": "alice"}'), ('bobCo/','{"type": "bob"}') $i$,
    'bob directive is now visible'
  );

  return query select results_eq(
    $i$
    select d.catalog_prefix::text, d.token::text, a.user_id, a.user_claims::text
    from directives d join applied_directives a on a.directive_id = d.id
    order by d.catalog_prefix;
    $i$,
    $i$
    values ('aliceCo/', null, auth.uid(), null),
      ('bobCo/', 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb', auth.uid(), null);
    $i$,
    'bob token was not reset (unlike alice it is not single use)'
  );

  -- Switch to the Bob user, and also turn a token on their behalf.
  set request.jwt.claim.sub to '22222222-2222-2222-2222-222222222222';

  -- Bob turns in a token and updates their user claims.
  perform exchange_directive_token('bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb');
  update applied_directives set user_claims = '{"hello":"bob"}';

  return query select results_eq(
    $i$
    select d.catalog_prefix::text, d.token::text, a.user_id, a.user_claims::text
    from directives d join applied_directives a on a.directive_id = d.id
    order by d.catalog_prefix;
    $i$,
    $i$
    values ('bobCo/', 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb', auth.uid(), '{"hello":"bob"}');
    $i$,
    'bob sees only their applied bob directive and not those of alice'
  );

  -- Switch back to Alice.
  set request.jwt.claim.sub to '11111111-1111-1111-1111-111111111111';
  -- Alice can update directives, but it affects only Alice's directive and not Bob's.
  update directives set catalog_prefix = 'aliceCo/dir/', spec = '{"type": "alice.v2"}';

  return query select results_eq(
    $i$ select catalog_prefix::text, spec::text from directives order by catalog_prefix $i$,
    $i$ values  ('aliceCo/dir/','{"type": "alice.v2"}'), ('bobCo/','{"type": "bob"}') $i$,
    'alice directive is updated and not bob'
  );

  -- Alice can't change the catalog_prefix to a namespace they don't admin.
  return query select throws_like(
    $i$ update directives set catalog_prefix = 'otherCo/'; $i$,
    'new row violates row-level security policy for table "directives"',
    'attempted change to otherCo fails'
  );

  -- Alice can update their user claims.
  update applied_directives set user_claims = '{"hello":"alice"}' where user_claims is null;

  return query select results_eq(
    $i$
    select d.catalog_prefix::text, d.token::text, a.user_id, a.user_claims::text
    from directives d join applied_directives a on a.directive_id = d.id
    order by d.catalog_prefix;
    $i$,
    $i$
    values ('aliceCo/dir/', null, auth.uid(), '{"hello":"alice"}'),
      ('bobCo/', 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb', auth.uid(), '{"hello":"alice"}');
    $i$,
    'alice user claims are updated but not bob'
  );

  -- And delete them if they have yet to be applied.
  delete from applied_directives where id in
    (select id from applied_directives order by id desc limit 1);

  return query select results_eq(
    $i$
    select d.catalog_prefix::text, d.token::text, a.user_id, a.user_claims::text, a.job_status->>'type'
    from directives d join applied_directives a on a.directive_id = d.id
    order by d.catalog_prefix;
    $i$,
    $i$
    values ('aliceCo/dir/', null, auth.uid(), '{"hello":"alice"}', 'queued');
    $i$,
    'alice deletes an applied directive'
  );

  -- We can also update it if our job is another non-success status.
  -- On doing so it's re-queued for re-evaluation by the agent.
  set role postgres;
  update applied_directives set job_status = '{"type":"whoopsie"}';
  set role authenticated;

  update applied_directives set user_claims = '{"try":"again"}';

  return query select results_eq(
    $i$
    select d.catalog_prefix::text, d.token::text, a.user_id, a.user_claims::text, a.job_status->>'type'
    from directives d join applied_directives a on a.directive_id = d.id
    order by d.catalog_prefix;
    $i$,
    $i$
    values ('aliceCo/dir/', null, auth.uid(), '{"try":"again"}', 'queued');
    $i$,
    'alice updates claims to try again and the job is re-queued'
  );

  -- Once an applied directive has completed, though, Alice can no longer change or delete it.
  set role postgres;
  update applied_directives set job_status = '{"type":"success"}';
  set role authenticated;

  return query select throws_like(
    $i$ update applied_directives set user_claims = '{"not":"allowed"}'; $i$,
    'Cannot modify an applied directive which has completed',
    'attempted change of applied directive fails'
  );

  return query select throws_like(
    $i$ delete from applied_directives; $i$,
    'Cannot delete an applied directive which has completed',
    'attempted deletion of applied directive fails'
  );

end
$$ language plpgsql;