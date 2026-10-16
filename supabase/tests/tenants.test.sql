
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
    $i$ select plan_kind::text, collection_method::text, payment_provider::text from tenants where tenant = 'aliceCo/' $i$,
    $i$ values ('self_serve', 'charge_default_payment_method', 'stripe') $i$,
    'tenants default to a self-serve plan charged to the default payment method'
  );
  return query select lives_ok(
    $i$ update tenants set payment_provider = 'external' where tenant = 'bobCo/' $i$,
    'self-serve tenants may remain externally paid'
  );
  return query select throws_ok(
    $i$ update tenants set plan_kind = 'contract' where tenant = 'bobCo/' $i$,
    '23514', null, 'contracts require a classified payment provider'
  );
  return query select throws_ok(
    $i$ update tenants set plan_kind = 'internal', payment_provider = null where tenant = 'aliceCo/' $i$,
    '23514', null, 'internal plans require a payment provider'
  );
  return query select lives_ok(
    $i$ update tenants set plan_kind = 'contract', payment_provider = 'snowflake_marketplace', collection_method = 'send_invoice' where tenant = 'bobCo/' $i$,
    'contracts accept a marketplace provider'
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