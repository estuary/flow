
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

  return query select lives_ok(
    $i$ update tenants set plan_kind = 'internal', payment_provider = null where tenant = 'aliceCo/' $i$,
    'internal tenants need no payment provider'
  );
  return query select throws_ok(
    $i$ update tenants set plan_kind = 'contract', payment_provider = 'external' where tenant = 'bobCo/' $i$,
    '23514', null, 'contracts cannot use the external payment provider'
  );
  return query select throws_ok(
    $i$ update tenants set plan_kind = 'contract', payment_provider = null where tenant = 'bobCo/' $i$,
    '23514', null, 'contracts require a payment provider'
  );
  return query select lives_ok(
    format($i$ update tenants set plan_kind = 'contract', payment_provider = %L where tenant = 'bobCo/' $i$, provider),
    format('contracts accept %s', provider)
  ) from unnest(array['aws_marketplace', 'gcp_marketplace', 'snowflake_marketplace']) as provider;

  -- Drop priviledge to `authenticated` and authorize as Alice.
  perform set_authenticated_context('11111111-1111-1111-1111-111111111111');

  return query select results_eq(
    $i$ select tenant::text from tenants $i$,
    $i$ values ('aliceCo/') $i$,
    'alice can read alice tenant only'
  );

end;
$$ language plpgsql;