
create function tests.test_tenant_controller_billing_contact()
returns setof text as $$
declare
  v_task_id public.flowid;
  v_task_count integer;
begin

  -- Inserting a tenant auto-creates a controller task and records its id.
  insert into tenants (tenant) values ('acmeCo/');
  select controller_task_id into v_task_id from tenants where tenant = 'acmeCo/';

  return query select ok(
    v_task_id is not null,
    'inserting a tenant auto-creates a controller task'
  );
  return query select is(
    (select task_type from internal.tasks where task_id = v_task_id),
    12::smallint,
    'the controller task has the tenant-controller task type'
  );
  return query select ok(
    (select wake_at is null from internal.tasks where task_id = v_task_id),
    'a newly created controller task is not yet woken'
  );

  -- Changing a billing field wakes the controller (IS DISTINCT FROM fires).
  update tenants set billing_email = 'billing@acme.co' where tenant = 'acmeCo/';
  return query select ok(
    (select wake_at is not null from internal.tasks where task_id = v_task_id),
    'a billing-field change wakes the controller'
  );

  -- A non-billing update must not wake the controller. Clear wake_at and confirm
  -- a change to an unrelated column leaves it untouched.
  update internal.tasks set wake_at = null where task_id = v_task_id;
  update tenants set detail = 'unrelated' where tenant = 'acmeCo/';
  return query select ok(
    (select wake_at is null from internal.tasks where task_id = v_task_id),
    'a non-billing update does not wake the controller'
  );

  -- Lazy create: a tenant with no controller (e.g. a backfilled row) gets one
  -- created exactly once on its first billing-field change.
  update tenants set controller_task_id = null where tenant = 'acmeCo/';
  select count(*) into v_task_count from internal.tasks where task_type = 12;

  update tenants set billing_name = 'Acme Billing' where tenant = 'acmeCo/';
  return query select ok(
    (select controller_task_id is not null from tenants where tenant = 'acmeCo/'),
    'a billing change lazily creates a controller when none exists'
  );
  return query select is(
    (select count(*)::integer from internal.tasks where task_type = 12),
    v_task_count + 1,
    'exactly one controller task is lazily created'
  );

end;
$$ language plpgsql;

create function tests.test_tenant_controller_payment_provider()
returns setof text as $$
declare
  v_task_id public.flowid;
begin

  -- Inserting a tenant auto-creates a controller task; it starts un-woken and
  -- defaults to the 'stripe' payment provider.
  insert into tenants (tenant) values ('acmeCo/');
  select controller_task_id into v_task_id from tenants where tenant = 'acmeCo/';

  -- Setting the provider to 'external' wakes the controller and enqueues a wake
  -- message on its task inbox.
  update tenants set payment_provider = 'external' where tenant = 'acmeCo/';
  return query select ok(
    (select wake_at is not null from internal.tasks where task_id = v_task_id),
    'setting payment_provider to external wakes the controller'
  );
  return query select ok(
    (select exists(
       select 1 from internal.tasks, unnest(inbox) as msg
       where task_id = v_task_id and (msg -> 1)::jsonb = '{"type":"wake"}'::jsonb
     )),
    'a wake message is enqueued on the controller task inbox'
  );

  -- A repeated no-op update (external -> external) must not re-wake the
  -- controller: the IS DISTINCT FROM guard suppresses it.
  update internal.tasks set wake_at = null where task_id = v_task_id;
  update tenants set payment_provider = 'external' where tenant = 'acmeCo/';
  return query select ok(
    (select wake_at is null from internal.tasks where task_id = v_task_id),
    'a no-op payment_provider update does not re-wake the controller'
  );

  -- Transitioning away from external (external -> stripe) must not wake the
  -- controller: the trigger only fires when the new value is 'external'.
  update tenants set payment_provider = 'stripe' where tenant = 'acmeCo/';
  return query select ok(
    (select wake_at is null from internal.tasks where task_id = v_task_id),
    'changing payment_provider away from external does not wake the controller'
  );

end;
$$ language plpgsql;
