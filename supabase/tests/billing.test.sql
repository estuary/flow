create function tests.test_billing_202308()
returns setof text as $$
declare
  response json;
begin

  -- Replace seed grants with fixtures for this test.
  delete from user_grants;
  insert into user_grants (user_id, object_role, capability) values
    ('11111111-1111-1111-1111-111111111111', 'aliceCo/', 'admin'),
    ('11111111-1111-1111-1111-111111111111', 'aliceCo_aa/', 'admin'),
    ('11111111-1111-1111-1111-111111111111', 'aliceCo_bb/', 'admin'),
    ('11111111-1111-1111-1111-111111111111', 'aliceCo_cc/', 'admin'),
    ('22222222-2222-2222-2222-222222222222', 'bobCo/', 'admin')
  ;

  insert into tenants (tenant, data_tiers, usage_tiers, recurring_usd_cents) values
    ('aliceCo/', '{30, 4, 25, 6, 20, 20, 15}', '{18, 40, 17, 60, 16, 200, 14}', 10000),
    ('aliceCo_aa/', '{50, 30, 20}', '{15}', 0),
    ('aliceCo_bb/', '{50, 30, 20}', '{15}', 0),
    ('aliceCo_cc/', '{50, 30, 20}', '{15}', 0),
    ('bobCo/', default, default, default);

  insert into catalog_stats (
    catalog_name, grain, ts, flow_document, bytes_written_by_me, bytes_read_by_me, usage_seconds
  ) values
    ('aliceCo/',         'daily', '2022-08-01T00:00:00Z', '{}', 43.125 * 10^9, 0, 3600 * 738.375),
    ('aliceCo_aa/',      'daily', '2022-08-01T00:00:00Z', '{}', 11.125 * 10^9, 7 * 10^9, 3600 * 720),
    ('aliceCo_aa/',      'daily', '2022-08-30T00:00:00Z', '{}', 1 * 10^9, 2 * 10^9, 0),
    ('aliceCo_bb/',      'daily', '2022-08-01T00:00:00Z', '{}', 0, 22 * 10^9, 3600 * 18.375),
    ('aliceCo_cc/',      'daily', '2022-07-01T00:00:00Z', '{}', 0, 0.1 * 10^9, 0),
    ('aliceCo_cc/',      'daily', '2022-07-02T00:00:00Z', '{}', 0, 0.1 * 10^9, 0),
    ('aliceCo_cc/',      'daily', '2022-07-03T00:00:00Z', '{}', 0, 0.1 * 10^9, 0),
    ('aliceCo_cc/',      'daily', '2022-07-04T00:00:00Z', '{}', 0, 0.1 * 10^9, 0),
    ('aliceCo_cc/',      'daily', '2022-07-05T00:00:00Z', '{}', 0, 0.1 * 10^9, 0),
    ('aliceCo_cc/',      'daily', '2022-07-06T00:00:00Z', '{}', 0, 0.1 * 10^9, 0),
    ('aliceCo_cc/',      'daily', '2022-07-07T00:00:00Z', '{}', 0, 0.1 * 10^9, 0),
    ('aliceCo_cc/',      'daily', '2022-07-08T00:00:00Z', '{}', 0, 0.1 * 10^9, 0),
    ('aliceCo_cc/',      'daily', '2022-07-09T00:00:00Z', '{}', 0, 0.1 * 10^9, 0),
    ('aliceCo_cc/',      'daily', '2022-07-10T00:00:00Z', '{}', 0, 0.1 * 10^9, 0),
    ('aliceCo_cc/',      'daily', '2022-07-11T00:00:00Z', '{}', 0, 0.1 * 10^9, 0),
    ('aliceCo_cc/',      'daily', '2022-07-12T00:00:00Z', '{}', 0, 0.1 * 10^9, 0),
    ('aliceCo_cc/',      'daily', '2022-07-13T00:00:00Z', '{}', 0, 0.1 * 10^9, 0),
    ('aliceCo_cc/',      'daily', '2022-07-14T00:00:00Z', '{}', 0, 0.1 * 10^9, 0),
    ('aliceCo_cc/',      'daily', '2022-07-15T00:00:00Z', '{}', 0, 0.1 * 10^9, 0),
    ('aliceCo_cc/',      'daily', '2022-07-16T00:00:00Z', '{}', 0, 0.1 * 10^9, 0),
    ('aliceCo_cc/',      'daily', '2022-07-17T00:00:00Z', '{}', 0, 0.1 * 10^9, 0),
    ('aliceCo_cc/',      'daily', '2022-07-18T00:00:00Z', '{}', 0, 0.1 * 10^9, 0)
  ;

  insert into internal.billing_adjustments (
    tenant, billed_month, usd_cents, authorizer, detail
  ) values
    ('aliceCo/', '2022-08-01T00:00:00Z', -250, 'john@estuary.dev', 'A make good from a whoops'),
    ('aliceCo/', '2022-08-01T00:00:00Z', 350, 'sue@estuary.dev', 'An extra charge for some reason'),
    ('aliceCo/', '2022-07-01T00:00:00Z', 100, 'jane@estuary.dev', 'different month'),
    ('bobCo/', '2022-08-01T00:00:00Z', 200, 'frank@estuary.dev', 'different tenant')
  ;

  perform internal.freeze_billing_month('2022-08-01');
  -- We're authorized as Alice.
  perform set_authenticated_context('11111111-1111-1111-1111-111111111111');

  return query select is((select jsonb_agg(invoices.extra-'daily_usage') from (
    select extra
    from invoices_ext
    where date_end < '2022-09-01'
    and billed_prefix = 'aliceCo/'
  ) as invoices),
  jsonb_build_array(
    '{
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
      },
      {
        "count": 1,
        "description": "Free tier credit",
        "rate": -11966,
        "subtotal": -11966
      }
    ],
    "free_tier_credit": 11966,
    "processed_data_gb": 43.125,
    "recurring_fee": 10000,
    "subtotal": 10100,
    "task_usage_hours": 738.375,
    "trial_credit": 0,
    "trial_start": null
  }'::jsonb
  ));

  -- We're authorized as Bob.
  perform set_authenticated_context('22222222-2222-2222-2222-222222222222');

  return query select is((select jsonb_agg(distinct invoices.billed_prefix) from (
    select billed_prefix
    from invoices_ext
  ) as invoices),
  jsonb_build_array('bobCo/'));


  set role postgres;
  -- Switch tiers so usage spills over
  -- and set trial so half of August is covered
  update tenants set
    trial_start='2022-08-15',
    data_tiers = '{50, 5, 20}'
    where tenant ^@ 'aliceCo_';

  truncate table internal.billing_historicals;
  perform internal.freeze_billing_month('2022-08-01');
  perform set_authenticated_context('11111111-1111-1111-1111-111111111111');

  -- aliceCo has a free trial set, and has free trial usage, so let's check that
  return query select is((select jsonb_agg(invoices.extra) from (
    select extra
    from invoices_ext
    where date_end < '2022-09-01'
    and billed_prefix = 'aliceCo_aa/'
  ) as invoices),
  jsonb_build_array('{
    "billed_month": "2022-08-01T00:00:00+00:00",
    "billed_prefix": "aliceCo_aa/",
    "daily_usage": [
        {
            "data_gb": 18.125,
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
        },
        {
            "count": 1,
            "description": "Free tier credit ending 2022-08-14",
            "rate": -11330,
            "subtotal": -11330
        }
    ],
    "free_tier_credit": 11330,
    "processed_data_gb": 21.125,
    "recurring_fee": 0,
    "subtotal": 0,
    "task_usage_hours": 720,
    "trial_credit": 60,
    "trial_start": "2022-08-15"
  }'::jsonb));

  -- aliceCo/bb has a free trial set, but has no usage in the trial period
  -- this should result in a free trial credit line item of $0
  return query select is((select jsonb_agg(invoices.extra) from (
    select extra
    from invoices_ext
    where date_end < '2022-09-01'
    and billed_prefix = 'aliceCo_bb/'
  ) as invoices),
  jsonb_build_array('{
    "billed_month": "2022-08-01T00:00:00+00:00",
    "billed_prefix": "aliceCo_bb/",
    "daily_usage": [
      {
        "ts": "2022-08-01T00:00:00+00:00",
        "data_gb": 22,
        "task_hours": 18.375,
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
        },
        {
            "count": 1,
            "description": "Free tier credit ending 2022-08-14",
            "rate": -875,
            "subtotal": -875
        }
    ],
    "processed_data_gb": 22,
    "recurring_fee": 0,
    "free_tier_credit": 875,
    "subtotal": 0,
    "task_usage_hours": 18.375,
    "trial_start": "2022-08-15",
    "trial_credit": 0
  }'::jsonb));

  set role postgres;
  perform internal.freeze_billing_month('2022-07-01');
  perform set_authenticated_context('11111111-1111-1111-1111-111111111111');

  -- aliceCo/cc has a free trial set, but has no usage in the trial period
  -- and multiple days of fractional usage. Let's see if they round correctly
  return query select is((select jsonb_agg(invoices.extra) from (
    select extra
    from invoices_ext
    where date_end < '2022-08-01'
    and billed_prefix = 'aliceCo_cc/'
  ) as invoices),
  jsonb_build_array('{
   "line_items":[
      {
         "rate":50,
         "count":2,
         "subtotal":100,
         "description":"Data processing (first 5 GBs at $0.50/GB)"
      },
      {
         "rate":20,
         "count":0,
         "subtotal":0,
         "description":"Data processing (at $0.20/GB)"
      },
      {
         "rate":15,
         "count":0,
         "subtotal":0,
         "description":"Task usage (at $0.15/hour)"
      },
      {
          "count": 1,
          "description": "Free tier credit ending 2022-08-14",
          "rate": -100,
          "subtotal": -100
      }
   ],
   "daily_usage":[
      {
         "ts":"2022-07-01T00:00:00+00:00",
         "data_gb": 0.1,
         "task_hours":0.00000000000000000000,
         "data_subtotal":50,
         "task_subtotal":0
      },
      {
         "ts":"2022-07-02T00:00:00+00:00",
         "data_gb": 0.1,
         "task_hours":0.00000000000000000000,
         "data_subtotal":0,
         "task_subtotal":0
      },
      {
         "ts":"2022-07-03T00:00:00+00:00",
         "data_gb": 0.1,
         "task_hours":0.00000000000000000000,
         "data_subtotal":0,
         "task_subtotal":0
      },
      {
         "ts":"2022-07-04T00:00:00+00:00",
         "data_gb": 0.1,
         "task_hours":0.00000000000000000000,
         "data_subtotal":0,
         "task_subtotal":0
      },
      {
         "ts":"2022-07-05T00:00:00+00:00",
         "data_gb": 0.1,
         "task_hours":0.00000000000000000000,
         "data_subtotal":0,
         "task_subtotal":0
      },
      {
         "ts":"2022-07-06T00:00:00+00:00",
         "data_gb": 0.1,
         "task_hours":0.00000000000000000000,
         "data_subtotal":0,
         "task_subtotal":0
      },
      {
         "ts":"2022-07-07T00:00:00+00:00",
         "data_gb": 0.1,
         "task_hours":0.00000000000000000000,
         "data_subtotal":0,
         "task_subtotal":0
      },
      {
         "ts":"2022-07-08T00:00:00+00:00",
         "data_gb": 0.1,
         "task_hours":0.00000000000000000000,
         "data_subtotal":0,
         "task_subtotal":0
      },
      {
         "ts":"2022-07-09T00:00:00+00:00",
         "data_gb": 0.1,
         "task_hours":0.00000000000000000000,
         "data_subtotal":0,
         "task_subtotal":0
      },
      {
         "ts":"2022-07-10T00:00:00+00:00",
         "data_gb": 0.1,
         "task_hours":0.00000000000000000000,
         "data_subtotal":0,
         "task_subtotal":0
      },
      {
         "ts":"2022-07-11T00:00:00+00:00",
         "data_gb": 0.1,
         "task_hours":0.00000000000000000000,
         "data_subtotal":50,
         "task_subtotal":0
      },
      {
         "ts":"2022-07-12T00:00:00+00:00",
         "data_gb": 0.1,
         "task_hours":0.00000000000000000000,
         "data_subtotal":0,
         "task_subtotal":0
      },
      {
         "ts":"2022-07-13T00:00:00+00:00",
         "data_gb": 0.1,
         "task_hours":0.00000000000000000000,
         "data_subtotal":0,
         "task_subtotal":0
      },
      {
         "ts":"2022-07-14T00:00:00+00:00",
         "data_gb": 0.1,
         "task_hours":0.00000000000000000000,
         "data_subtotal":0,
         "task_subtotal":0
      },
      {
         "ts":"2022-07-15T00:00:00+00:00",
         "data_gb": 0.1,
         "task_hours":0.00000000000000000000,
         "data_subtotal":0,
         "task_subtotal":0
      },
      {
         "ts":"2022-07-16T00:00:00+00:00",
         "data_gb": 0.1,
         "task_hours":0.00000000000000000000,
         "data_subtotal":0,
         "task_subtotal":0
      },
      {
         "ts":"2022-07-17T00:00:00+00:00",
         "data_gb": 0.1,
         "task_hours":0.00000000000000000000,
         "data_subtotal":0,
         "task_subtotal":0
      },
      {
         "ts":"2022-07-18T00:00:00+00:00",
         "data_gb": 0.1,
         "task_hours":0.00000000000000000000,
         "data_subtotal":0,
         "task_subtotal":0
      }
   ],
   "free_tier_credit": 100,
   "subtotal":0,
   "trial_start":"2022-08-15",
   "billed_month":"2022-07-01T00:00:00+00:00",
   "trial_credit":0,
   "billed_prefix":"aliceCo_cc/",
   "recurring_fee":0,
   "task_usage_hours":0.00000000000000000000,
   "processed_data_gb":1.8
  }'::jsonb));

  set role postgres;

  insert into internal.manual_bills (tenant, usd_cents, description, date_start, date_end)
  values ('aliceCo/',12356,'Test manually entered bill','2022-10-12','2022-11-25');

  truncate table internal.billing_historicals;
  perform internal.freeze_billing_month('2022-08-01');

  perform set_authenticated_context('11111111-1111-1111-1111-111111111111');
  return query select is((select jsonb_agg(invoices.jsonified::jsonb-'extra') from (
    select row_to_json(invoices_ext) as jsonified
    from invoices_ext
    where billed_prefix='aliceCo/'
    order by date_start desc
  ) as invoices), jsonb_build_array(
    jsonb_build_object(
      'date_start', date_trunc('month', now())::date,
      'date_end', (date_trunc('month', now()) + interval '1 month' - interval '1 day')::date,
      'subtotal', 0,
      'line_items', '[]'::jsonb,
      'invoice_type', 'preview',
      'billed_prefix', 'aliceCo/'
    ),
    '{
        "billed_prefix": "aliceCo/",
        "date_end": "2022-11-25",
        "date_start": "2022-10-12",
        "invoice_type": "manual",
        "line_items": [
            {
                "count": 1,
                "description": "Test manually entered bill",
                "rate": 12356,
                "subtotal": 12356
            }
        ],
        "subtotal": 12356
    }'::jsonb,
    '{
        "billed_prefix": "aliceCo/",
        "date_end": "2022-08-31",
        "date_start": "2022-08-01",
        "invoice_type": "final",
        "line_items": [
            {
                "count": 1,
                "description": "Recurring service charge",
                "rate": 10000,
                "subtotal": 10000
            },
            {
                "count": 4,
                "description": "Data processing (first 4 GBs at $0.30/GB)",
                "rate": 30,
                "subtotal": 120
            },
            {
                "count": 6,
                "description": "Data processing (next 6 GBs at $0.25/GB)",
                "rate": 25,
                "subtotal": 150
            },
            {
                "count": 20,
                "description": "Data processing (next 20 GBs at $0.20/GB)",
                "rate": 20,
                "subtotal": 400
            },
            {
                "count": 14,
                "description": "Data processing (at $0.15/GB)",
                "rate": 15,
                "subtotal": 210
            },
            {
                "count": 40,
                "description": "Task usage (first 40 hours at $0.18/hour)",
                "rate": 18,
                "subtotal": 720
            },
            {
                "count": 60,
                "description": "Task usage (next 60 hours at $0.17/hour)",
                "rate": 17,
                "subtotal": 1020
            },
            {
                "count": 200,
                "description": "Task usage (next 200 hours at $0.16/hour)",
                "rate": 16,
                "subtotal": 3200
            },
            {
                "count": 439,
                "description": "Task usage (at $0.14/hour)",
                "rate": 14,
                "subtotal": 6146
            },
            {
                "count": 1,
                "description": "A make good from a whoops",
                "rate": -250,
                "subtotal": -250
            },
            {
                "count": 1,
                "description": "An extra charge for some reason",
                "rate": 350,
                "subtotal": 350
            },
            {
                "count": 1,
                "description": "Free tier credit",
                "rate": -11966,
                "subtotal": -11966
            }
        ],
        "subtotal": 10100
    }'::jsonb
  ));

end
$$ language plpgsql;