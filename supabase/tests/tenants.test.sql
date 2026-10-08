
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

  return query select results_eq(
    $i$ select tenant::text, invoice_overages from tenants order by tenant $i$,
    $i$ values ('aliceCo/', false), ('bobCo/', false) $i$,
    'overage invoice preference defaults to false'
  );

  update tenants set invoice_overages = true where tenant = 'aliceCo/';
  return query select results_eq(
    $i$ select tenant::text, invoice_overages, payment_provider::text from tenants order by tenant $i$,
    $i$ values ('aliceCo/', true, 'stripe'), ('bobCo/', false, 'stripe') $i$,
    'overage preference is independent of provider and other tenants'
  );

  return query select throws_ok(
    $i$ update tenants set invoice_overages = null where tenant = 'aliceCo/' $i$,
    '23502', null, 'overage invoice preference cannot be null'
  );

  -- Drop priviledge to `authenticated` and authorize as Alice.
  perform set_authenticated_context('11111111-1111-1111-1111-111111111111');

  return query select results_eq(
    $i$ select tenant::text from tenants $i$,
    $i$ values ('aliceCo/') $i$,
    'alice can read alice tenant only'
  );

end;
$$ language plpgsql;
