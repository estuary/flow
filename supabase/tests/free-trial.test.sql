
create function tests.test_new_free_trial_tenants()
returns setof text as $$
begin

  delete from tenants;
  insert into tenants (tenant, trial_start) values
    ('freebie/', null), -- stays just under the limits
    ('ghost/', null), -- no usage at all
    ('alreadyPay/', now() - '45 days'::interval),
    ('overHoursByDay/', null),
    ('overHoursByMonth/', null),
    ('overGBs/', null),
    ('overAll/', null);

  insert into catalog_stats(grain, catalog_name, ts, usage_seconds, bytes_written_by_me, bytes_read_by_me, flow_document)
  values
    -- freebie used 48 hours per day, and just under 10GBs in a month
    ('daily', 'freebie/', date_trunc('day', now() - '24 h'::interval), 48 * 3600, 0, 0, '{}'),
    ('monthly', 'freebie/', date_trunc('month', now() at time zone 'UTC'), 24 * 31 * 2 * 3600, 4000000000, 5000000000, '{}'),
    -- alreadyPay is using way above the free tier limits, but already has a trial_start
    ('daily', 'alreadyPay/', date_trunc('day', now() - '24 h'::interval), 300 * 3600, 0, 0, '{}'),
    ('monthly', 'alreadyPay/', date_trunc('month', now() at time zone 'UTC'), 24 * 31 * 6 * 3600, 99000000000, 99000000000, '{}'),
    ('daily', 'overHoursByDay/', date_trunc('day', now() - '24 h'::interval), 60 * 3600, 0, 0, '{}'),
    ('monthly', 'overHoursByMonth/', date_trunc('month', now()), 24 * 31 * 4 * 3600, 0, 0, '{}'),
    ('monthly', 'overGBs/', date_trunc('month', now()), 55, 6000000000, 6000000000, '{}'),
    ('daily', 'overAll/', date_trunc('day', now() - '24 h'::interval), 60 * 3600, 0, 0, '{}'),
    ('monthly', 'overAll/', date_trunc('month', now()), 24 * 31 * 6 * 3600, 6000000000, 6000000000, '{}');



  return query select results_eq(
    $i$ select tenant::text from internal.new_free_trial_tenants order by tenant $i$,
    $i$ values ('overAll/'), ('overGBs/'), ('overHoursByDay/'), ('overHoursByMonth/') $i$,
    'expect correct tenants returned'
  );

end;
$$ language plpgsql;
