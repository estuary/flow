
create function tests.test_billing_v0()
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
    "billed_prefix": "aliceCo/",
    "total_processed_data_gb": 43.125,
    "max_concurrent_tasks": 3,
    "max_concurrent_tasks_at": "2022-08-29T13:00:00+00:00"
  }'::jsonb);

  set role postgres;
  delete from catalog_stats;

  insert into catalog_stats (
    catalog_name, grain, ts, flow_document, bytes_written_by_me, bytes_read_by_me
  ) values
    ('aliceCo/hello', 'hourly', '2022-08-29T13:00:00Z', '{}', 1, 0),
    ('aliceCo/big',   'hourly', '2022-08-29T13:00:00Z', '{}', 0, 1),
    ('aliceCo/world', 'hourly', '2022-08-29T13:00:00Z', '{}', 0, 1),

    ('aliceCo/hello', 'hourly', '2022-08-29T14:00:00Z', '{}', 1, 0),
    ('aliceCo/big',   'hourly', '2022-08-29T14:00:00Z', '{}', 0, 1),
    ('aliceCo/world', 'hourly', '2022-08-29T14:00:00Z', '{}', 0, 1),
    ('aliceCo/of',    'hourly', '2022-08-29T14:00:00Z', '{}', 0, 1),
    ('aliceCo/data',  'hourly', '2022-08-29T14:00:00Z', '{}', 0, 1),

    ('aliceCo/hello', 'monthly', '2022-08-01T00:00:00Z', '{}', 5.125 * 1024 * 1024 * 1024, 0),
    ('aliceCo/big',   'monthly', '2022-08-01T00:00:00Z', '{}', 7::bigint * 1024 * 1024 * 1024, 9::bigint * 1024 * 1024 * 1024),
    ('aliceCo/world', 'monthly', '2022-08-01T00:00:00Z', '{}', 0, 22::bigint * 1024 * 1024 * 1024)
  ;

  -- We're authorized as Alice.
  perform set_authenticated_context('11111111-1111-1111-1111-111111111111');

  return query select is(billing_report('aliceCo/', '2022-08-29T13:00:00Z'), '{
    "subtotal": 6000,
    "line_items": [
      {
        "rate": 0,
        "count": 2,
        "subtotal": 0,
        "description": "Included task shards (up to 2)"
      },
      {
        "rate": 2000,
        "count": 3,
        "subtotal": 6000,
        "description": "Additional task shards minimum (assessed at 2022-08-29 14:00:00+00)"
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
    "billed_prefix": "aliceCo/",
    "total_processed_data_gb": 43.125,
    "max_concurrent_tasks": 5,
    "max_concurrent_tasks_at": "2022-08-29T14:00:00+00:00"
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

create function tests.test_billing_202308()
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

  insert into tenants (tenant, data_tiers, usage_tiers, recurring_usd_cents) values
    ('aliceCo/', '{30, 4, 25, 6, 20, 20, 15}', '{18, 40, 17, 60, 16, 200, 14}', 10000),
    ('bobCo/', default, default, default);

  insert into catalog_stats (
    catalog_name, grain, ts, flow_document, bytes_written_by_me, bytes_read_by_me, usage_seconds
  ) values
    ('aliceCo/aa/hello', 'monthly', '2022-08-01T00:00:00Z', '{}', 5.125 * 1024 * 1024 * 1024, 0, 3600 * 720),
    ('aliceCo/aa/big',   'monthly', '2022-08-01T00:00:00Z', '{}', 7::bigint * 1024 * 1024 * 1024, 9::bigint * 1024 * 1024 * 1024, 0),
    ('aliceCo/bb/world', 'monthly', '2022-08-01T00:00:00Z', '{}', 0, 22::bigint * 1024 * 1024 * 1024, 3600 * 18.375)
  ;

  insert into internal.billing_adjustments (
    tenant, billed_month, usd_cents, authorizer, detail
  ) values
    ('aliceCo/', '2022-08-01T00:00:00Z', -250, 'john@estuary.dev', 'A make good from a whoops'),
    ('aliceCo/', '2022-08-01T00:00:00Z', 350, 'sue@estuary.dev', 'An extra charge for some reason'),
    ('aliceCo/', '2022-07-01T00:00:00Z', 100, 'jane@estuary.dev', 'different month'),
    ('bobCo/', '2022-08-01T00:00:00Z', 200, 'frank@estuary.dev', 'different tenant')
  ;

  -- We're authorized as Alice.
  perform set_authenticated_context('11111111-1111-1111-1111-111111111111');

  return query select is(billing_report_202308('aliceCo/', '2022-08-29T13:00:00Z'), '{
    "billed_month": "2022-08-01T00:00:00+00:00",
    "billed_prefix": "aliceCo/",
    "line_items": [
      {
        "rate": 10000,
        "count": 1,
        "subtotal": 10000,
        "description": "Recurring service charge"
      },
      {
        "rate": 30,
        "count": 4,
        "subtotal": 120,
        "description": "Data processing (first 4GB at $0.30/GB)"
      },
      {
        "rate": 25,
        "count": 6,
        "subtotal": 150,
        "description": "Data processing (next 6GB at $0.25/GB)"
      },
      {
        "rate": 20,
        "count": 20,
        "subtotal": 400,
        "description": "Data processing (next 20GB at $0.20/GB)"
      },
      {
        "rate": 15,
        "count": 13.125,
        "subtotal": 197,
        "description": "Data processing (at $0.15/GB)"
      },
      {
        "rate": 18,
        "count": 40,
        "subtotal": 720,
        "description": "Task usage (first 40 hours at $0.18/hour)"
      },
      {
        "rate": 17,
        "count": 60,
        "subtotal": 1020,
        "description": "Task usage (next 60 hours at $0.17/hour)"
      },
      {
        "rate": 16,
        "count": 200,
        "subtotal": 3200,
        "description": "Task usage (next 200 hours at $0.16/hour)"
      },
      {
        "rate": 14,
        "count": 438.375,
        "subtotal": 6137,
        "description": "Task usage (at $0.14/hour)"
      },
      {
        "rate": -250,
        "count": 1,
        "subtotal": -250,
        "description": "A make good from a whoops"
      },
      {
        "rate": 350,
        "count": 1,
        "subtotal": 350,
        "description": "An extra charge for some reason"
      }
    ],
    "processed_data_gb": 43.125,
    "recurring_fee": 10000,
    "subtotal": 22044,
    "task_usage_hours": 738.375
  }'::jsonb);

  set role postgres;

  -- Use a simpler tier structure.
  update tenants set
    data_tiers = '{50, 30, 20}',
    usage_tiers = '{15}'
    where tenant = 'aliceCo/';

  perform set_authenticated_context('11111111-1111-1111-1111-111111111111');

  -- Again, but now look at a narrower billed prefix.
  -- Note that we don't see fixed cost or adjustment,
  -- just rolled-up usage of the prefixed catalog tasks.
  return query select is(billing_report_202308('aliceCo/aa/', '2022-08-29T13:00:00Z'), '{
    "billed_month": "2022-08-01T00:00:00+00:00",
    "billed_prefix": "aliceCo/aa/",
    "line_items": [
      {
        "rate": 50,
        "count": 21.125,
        "subtotal": 1056,
        "description": "Data processing (first 30GB at $0.50/GB)"
      },
      {
        "rate": 20,
        "count": 0,
        "subtotal": 0,
        "description": "Data processing (at $0.20/GB)"
      },
      {
        "rate": 15,
        "count": 720,
        "subtotal": 10800,
        "description": "Task usage (at $0.15/hour)"
      }
    ],
    "processed_data_gb": 21.125,
    "recurring_fee": 0,
    "subtotal": 11856,
    "task_usage_hours": 720
  }'::jsonb);

  -- We're authorized as Bob.
  perform set_authenticated_context('22222222-2222-2222-2222-222222222222');

  return query select throws_like(
    $i$ select * from billing_report_202308('aliceCo/', '2022-08-29T13:00:00Z'); $i$,
    'You are not authorized for the billed prefix aliceCo/',
    'Attempting to fetch a report for aliceCo/ as Bob fails'
  );

end
$$ language plpgsql;