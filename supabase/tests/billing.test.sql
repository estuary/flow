
create function tests.test_billing()
returns setof text as $$
declare
  response json;
begin

  -- Replace seed grants with fixtures for this test.
  delete from user_grants;
  insert into user_grants (user_id, object_role, capability) values
    ('11111111-1111-1111-1111-111111111111', 'aliceCo/', 'admin'),
    ('22222222-2222-2222-2222-222222222222', 'bobCo/', 'admin')
  ;

  insert into catalog_stats (
    catalog_name, grain, ts, flow_document, bytes_written_by_me, bytes_read_by_me
  ) values
    ('aliceCo/hello', 'hourly', '2022-08-29T13:00:00Z', '{}', 1, 0),
    ('aliceCo/big',   'hourly', '2022-08-29T13:00:00Z', '{}', 0, 1),
    ('aliceCo/world', 'hourly', '2022-08-29T13:00:00Z', '{}', 0, 1),
    ('aliceCo/hello', 'monthly', '2022-08-01T00:00:00Z', '{}', 5.125 * 1024 * 1024 * 1024, 0),
    ('aliceCo/big',   'monthly', '2022-08-01T00:00:00Z', '{}', 7::bigint * 1024 * 1024 * 1024, 9::bigint * 1024 * 1024 * 1024),
    ('aliceCo/world', 'monthly', '2022-08-01T00:00:00Z', '{}', 0, 22::bigint * 1024 * 1024 * 1024)
  ;

  -- We're authorized as Alice.
  perform set_authenticated_context('11111111-1111-1111-1111-111111111111');

  return query select is(billing_report('aliceCo/', '2022-08-29T13:00:00Z'), '{
    "subtotal": 2484,
    "line_items": [
      {
        "rate": 0,
        "count": 2,
        "subtotal": 0,
        "description": "Included task shards (up to 2)"
      },
      {
        "rate": 2000,
        "count": 1,
        "subtotal": 2000,
        "description": "Additional task shards minimum (assessed at 2022-08-29 13:00:00+00)"
      },
      {
        "rate": 0,
        "count": 10,
        "subtotal": 0,
        "description": "Included data processing (in GB, up to 10.0GB)"
      },
      {
        "rate": 75,
        "count": 33.125,
        "subtotal": 2484,
        "description": "Additional data processing (in GB)"
      },
      {
        "description": "Subtotal is greater of task shards minimum, or data processing volume"
      }
    ],
    "billed_month": "2022-08-01T00:00:00+00:00",
    "billed_prefix": "aliceCo/"
  }'::jsonb);

  -- We're authorized as Bob.
  perform set_authenticated_context('22222222-2222-2222-2222-222222222222');

  return query select throws_like(
    $i$ select * from billing_report('aliceCo/', '2022-08-29T13:00:00Z'); $i$,
    'You are not authorized for the billed prefix aliceCo/',
    'Attempting to fetch a report for aliceCo/ as Bob fails'
  );

end
$$ language plpgsql;