
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
    ('aliceCo/aa/hello', 'daily', '2022-08-01T00:00:00Z', '{}', 5.125 * 1024 * 1024 * 1024, 0, 3600 * 720),
    ('aliceCo/aa/big',   'daily', '2022-08-01T00:00:00Z', '{}', 6::bigint * 1024 * 1024 * 1024, 7::bigint * 1024 * 1024 * 1024, 0),
    ('aliceCo/aa/big',   'daily', '2022-08-30T00:00:00Z', '{}', 1::bigint * 1024 * 1024 * 1024, 2::bigint * 1024 * 1024 * 1024, 0),
    ('aliceCo/bb/world', 'daily', '2022-08-01T00:00:00Z', '{}', 0, 22::bigint * 1024 * 1024 * 1024, 3600 * 18.375),
    ('aliceCo/cc/round', 'daily', '2022-07-01T00:00:00Z', '{}', 0, 0.1 * 1024 * 1024 * 1024, 0),
    ('aliceCo/cc/round', 'daily', '2022-07-02T00:00:00Z', '{}', 0, 0.1 * 1024 * 1024 * 1024, 0),
    ('aliceCo/cc/round', 'daily', '2022-07-03T00:00:00Z', '{}', 0, 0.1 * 1024 * 1024 * 1024, 0),
    ('aliceCo/cc/round', 'daily', '2022-07-04T00:00:00Z', '{}', 0, 0.1 * 1024 * 1024 * 1024, 0),
    ('aliceCo/cc/round', 'daily', '2022-07-05T00:00:00Z', '{}', 0, 0.1 * 1024 * 1024 * 1024, 0),
    ('aliceCo/cc/round', 'daily', '2022-07-06T00:00:00Z', '{}', 0, 0.1 * 1024 * 1024 * 1024, 0),
    ('aliceCo/cc/round', 'daily', '2022-07-07T00:00:00Z', '{}', 0, 0.1 * 1024 * 1024 * 1024, 0),
    ('aliceCo/cc/round', 'daily', '2022-07-08T00:00:00Z', '{}', 0, 0.1 * 1024 * 1024 * 1024, 0),
    ('aliceCo/cc/round', 'daily', '2022-07-09T00:00:00Z', '{}', 0, 0.1 * 1024 * 1024 * 1024, 0),
    ('aliceCo/cc/round', 'daily', '2022-07-10T00:00:00Z', '{}', 0, 0.1 * 1024 * 1024 * 1024, 0),
    ('aliceCo/cc/round', 'daily', '2022-07-11T00:00:00Z', '{}', 0, 0.1 * 1024 * 1024 * 1024, 0),
    ('aliceCo/cc/round', 'daily', '2022-07-12T00:00:00Z', '{}', 0, 0.1 * 1024 * 1024 * 1024, 0),
    ('aliceCo/cc/round', 'daily', '2022-07-13T00:00:00Z', '{}', 0, 0.1 * 1024 * 1024 * 1024, 0),
    ('aliceCo/cc/round', 'daily', '2022-07-14T00:00:00Z', '{}', 0, 0.1 * 1024 * 1024 * 1024, 0),
    ('aliceCo/cc/round', 'daily', '2022-07-15T00:00:00Z', '{}', 0, 0.1 * 1024 * 1024 * 1024, 0),
    ('aliceCo/cc/round', 'daily', '2022-07-16T00:00:00Z', '{}', 0, 0.1 * 1024 * 1024 * 1024, 0),
    ('aliceCo/cc/round', 'daily', '2022-07-17T00:00:00Z', '{}', 0, 0.1 * 1024 * 1024 * 1024, 0),
    ('aliceCo/cc/round', 'daily', '2022-07-18T00:00:00Z', '{}', 0, 0.1 * 1024 * 1024 * 1024, 0)
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

  return query select is(billing_report_202308('aliceCo/', '2022-08-29T13:00:00Z') - 'daily_usage', '{
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
        "description": "Data processing (first 4 GBs at $0.30/GB)"
      },
      {
        "rate": 25,
        "count": 6,
        "subtotal": 150,
        "description": "Data processing (next 6 GBs at $0.25/GB)"
      },
      {
        "rate": 20,
        "count": 20,
        "subtotal": 400,
        "description": "Data processing (next 20 GBs at $0.20/GB)"
      },
      {
        "rate": 15,
        "count": 14,
        "subtotal": 210,
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
        "count": 439,
        "subtotal": 6146,
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
    "processed_data_gb": 44,
    "recurring_fee": 10000,
    "subtotal": 22066,
    "task_usage_hours": 739,
    "trial_credit": 0,
    "trial_start": null
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
  return query select is((billing_report_202308('aliceCo/aa/', '2022-08-29T13:00:00Z') - 'daily_usage'), '{
    "billed_month": "2022-08-01T00:00:00+00:00",
    "billed_prefix": "aliceCo/aa/",
    "line_items": [
      {
        "rate": 50,
        "count": 22,
        "subtotal": 1100,
        "description": "Data processing (first 30 GBs at $0.50/GB)"
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
    "processed_data_gb": 22,
    "recurring_fee": 0,
    "subtotal": 11900,
    "task_usage_hours": 720,
    "trial_credit": 0,
    "trial_start": null
  }'::jsonb);

  -- We're authorized as Bob.
  perform set_authenticated_context('22222222-2222-2222-2222-222222222222');

  return query select throws_like(
    $i$ select * from billing_report_202308('aliceCo/', '2022-08-29T13:00:00Z'); $i$,
    'You are not authorized for the billed prefix aliceCo/',
    'Attempting to fetch a report for aliceCo/ as Bob fails'
  );

  set role postgres;
  -- Switch tiers so usage spills over
  -- and set trial so half of August is covered
  update tenants set
    trial_start='2022-08-15',
    data_tiers = '{50, 5, 20}'
    where tenant = 'aliceCo/';

  perform set_authenticated_context('11111111-1111-1111-1111-111111111111');

  -- aliceCo has a free trial set, and has free trial usage, so let's check that
  return query select is(billing_report_202308('aliceCo/aa/', '2022-08-29T13:00:00Z'), '{
    "billed_month": "2022-08-01T00:00:00+00:00",
    "billed_prefix": "aliceCo/aa/",
    "daily_usage": [
        {
            "data_gb": 19,
            "data_subtotal": 530,
            "task_hours": 720,
            "task_subtotal": 10800,
            "ts": "2022-08-01T00:00:00+00:00"
        },
        {
            "data_gb": 3,
            "data_subtotal": 60,
            "task_hours": 0,
            "task_subtotal": 0,
            "ts": "2022-08-30T00:00:00+00:00"
        }
    ],
    "line_items": [
        {
            "count": 5,
            "description": "Data processing (first 5 GBs at $0.50/GB)",
            "rate": 50,
            "subtotal": 250
        },
        {
            "count": 17,
            "description": "Data processing (at $0.20/GB)",
            "rate": 20,
            "subtotal": 340
        },
        {
            "count": 720,
            "description": "Task usage (at $0.15/hour)",
            "rate": 15,
            "subtotal": 10800
        },
        {
            "count": 1,
            "description": "Free trial credit (2022-08-15 - 2022-09-14)",
            "rate": -60,
            "subtotal": -60
        }
    ],
    "processed_data_gb": 22,
    "recurring_fee": 0,
    "subtotal": 11330,
    "task_usage_hours": 720,
    "trial_credit": 60,
    "trial_start": "2022-08-15"
  }'::jsonb);

  -- aliceCo/bb has a free trial set, but has no usage in the trial period
  -- this should result in a free trial credit line item of $0
  return query select is(billing_report_202308('aliceCo/bb/', '2022-08-29T13:00:00Z'), '{
    "billed_month": "2022-08-01T00:00:00+00:00",
    "billed_prefix": "aliceCo/bb/",
    "daily_usage": [
      {
        "ts": "2022-08-01T00:00:00+00:00",
        "data_gb": 22,
        "task_hours": 19,
        "data_subtotal": 590,
        "task_subtotal": 285
      }
    ],
    "line_items": [
        {
            "count": 5,
            "description": "Data processing (first 5 GBs at $0.50/GB)",
            "rate": 50,
            "subtotal": 250
        },
        {
            "count": 17,
            "description": "Data processing (at $0.20/GB)",
            "rate": 20,
            "subtotal": 340
        },
        {
            "count": 19,
            "description": "Task usage (at $0.15/hour)",
            "rate": 15,
            "subtotal": 285
        },
        {
            "count": 1,
            "description": "Free trial credit (2022-08-15 - 2022-09-14)",
            "rate": 0,
            "subtotal": 0
        }
    ],
    "processed_data_gb": 22,
    "recurring_fee": 0,
    "subtotal": 875,
    "task_usage_hours": 19,
    "trial_start": "2022-08-15",
    "trial_credit": 0
  }'::jsonb);

  -- aliceCo/cc has a free trial set, but has no usage in the trial period
  -- and multiple days of fractional usage. Let's see if they round correctly
  return query select is(billing_report_202308('aliceCo/cc/', '2022-07-20T13:00:00Z'), '{
    "billed_month": "2022-07-01T00:00:00+00:00",
    "billed_prefix": "aliceCo/cc/",
    "daily_usage": [
        {
            "data_gb": 1,
            "data_subtotal": 50,
            "task_hours": 0,
            "task_subtotal": 0,
            "ts": "2022-07-01T00:00:00+00:00"
        },
        {
            "data_gb": 0,
            "data_subtotal": 0,
            "task_hours": 0,
            "task_subtotal": 0,
            "ts": "2022-07-02T00:00:00+00:00"
        },
        {
            "data_gb": 0,
            "data_subtotal": 0,
            "task_hours": 0,
            "task_subtotal": 0,
            "ts": "2022-07-03T00:00:00+00:00"
        },
        {
            "data_gb": 0,
            "data_subtotal": 0,
            "task_hours": 0,
            "task_subtotal": 0,
            "ts": "2022-07-04T00:00:00+00:00"
        },
        {
            "data_gb": 0,
            "data_subtotal": 0,
            "task_hours": 0,
            "task_subtotal": 0,
            "ts": "2022-07-05T00:00:00+00:00"
        },
        {
            "data_gb": 0,
            "data_subtotal": 0,
            "task_hours": 0,
            "task_subtotal": 0,
            "ts": "2022-07-06T00:00:00+00:00"
        },
        {
            "data_gb": 0,
            "data_subtotal": 0,
            "task_hours": 0,
            "task_subtotal": 0,
            "ts": "2022-07-07T00:00:00+00:00"
        },
        {
            "data_gb": 0,
            "data_subtotal": 0,
            "task_hours": 0,
            "task_subtotal": 0,
            "ts": "2022-07-08T00:00:00+00:00"
        },
        {
            "data_gb": 0,
            "data_subtotal": 0,
            "task_hours": 0,
            "task_subtotal": 0,
            "ts": "2022-07-09T00:00:00+00:00"
        },
        {
            "data_gb": 0,
            "data_subtotal": 0,
            "task_hours": 0,
            "task_subtotal": 0,
            "ts": "2022-07-10T00:00:00+00:00"
        },
        {
            "data_gb": 1,
            "data_subtotal": 50,
            "task_hours": 0,
            "task_subtotal": 0,
            "ts": "2022-07-11T00:00:00+00:00"
        },
        {
            "data_gb": 0,
            "data_subtotal": 0,
            "task_hours": 0,
            "task_subtotal": 0,
            "ts": "2022-07-12T00:00:00+00:00"
        },
        {
            "data_gb": 0,
            "data_subtotal": 0,
            "task_hours": 0,
            "task_subtotal": 0,
            "ts": "2022-07-13T00:00:00+00:00"
        },
        {
            "data_gb": 0,
            "data_subtotal": 0,
            "task_hours": 0,
            "task_subtotal": 0,
            "ts": "2022-07-14T00:00:00+00:00"
        },
        {
            "data_gb": 0,
            "data_subtotal": 0,
            "task_hours": 0,
            "task_subtotal": 0,
            "ts": "2022-07-15T00:00:00+00:00"
        },
        {
            "data_gb": 0,
            "data_subtotal": 0,
            "task_hours": 0,
            "task_subtotal": 0,
            "ts": "2022-07-16T00:00:00+00:00"
        },
        {
            "data_gb": 0,
            "data_subtotal": 0,
            "task_hours": 0,
            "task_subtotal": 0,
            "ts": "2022-07-17T00:00:00+00:00"
        },
        {
            "data_gb": 0,
            "data_subtotal": 0,
            "task_hours": 0,
            "task_subtotal": 0,
            "ts": "2022-07-18T00:00:00+00:00"
        }
    ],
    "line_items": [
        {
            "count": 2,
            "description": "Data processing (first 5 GBs at $0.50/GB)",
            "rate": 50,
            "subtotal": 100
        },
        {
            "count": 0,
            "description": "Data processing (at $0.20/GB)",
            "rate": 20,
            "subtotal": 0
        },
        {
            "count": 0,
            "description": "Task usage (at $0.15/hour)",
            "rate": 15,
            "subtotal": 0
        }
    ],
    "processed_data_gb": 2,
    "recurring_fee": 0,
    "subtotal": 100,
    "task_usage_hours": 0,
    "trial_credit": 0,
    "trial_start": "2022-08-15"
  }'::jsonb);



end
$$ language plpgsql;