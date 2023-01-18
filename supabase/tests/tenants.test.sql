
create function tests.test_tenants()
returns setof text as $$
begin

  delete from user_grants;
  delete from role_grants;

  insert into user_grants (user_id, object_role, capability) values
    ('11111111-1111-1111-1111-111111111111', 'aliceCo/', 'admin'),
    ('22222222-2222-2222-2222-222222222222', 'bobCo/', 'admin')
  ;
  insert into tenants (tenant) values ('aliceCo/'), ('bobCo/');

  -- Drop priviledge to `authenticated` and authorize as Alice.
  set role authenticated;
  set request.jwt.claim.sub to '11111111-1111-1111-1111-111111111111';

  return query select results_eq(
    $i$ select tenant::text from tenants $i$,
    $i$ values ('aliceCo/') $i$,
    'alice can read alice tenant only'
  );

end;
$$ language plpgsql;