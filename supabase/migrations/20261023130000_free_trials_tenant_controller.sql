-- Tenant controllers start free trials: each checks its tenant's usage daily,
-- at a time of day fixed per tenant, and sets `trial_start` once usage exceeds
-- the free tier. See crates/agent/src/tenant_controller/free_trial.rs.

begin;

-- pg_cron exists only on the Supabase database, and not in sqlx::test databases.
do $$
begin
    if to_regclass('cron.job') is not null then
        perform cron.unschedule(jobid) from cron.job where jobname = 'free-trials';
    end if;
end
$$;

drop function internal.set_new_free_trials();
drop view internal.new_free_trial_tenants;

-- Create controllers for tenants which lack one.
select internal.create_tenant_controller(tenant)
from public.tenants
where controller_task_id is null;

-- Wake suspended controllers so they begin daily checks. Waking without a
-- message runs only scheduled checks, and doesn't reconcile billing with
-- Stripe. Wakes spread across a day so that controllers aren't all polled at
-- once, and each then settles on its tenant's own check time.
update internal.tasks
set wake_at = now() + random() * interval '1 day'
where task_type = 12 and wake_at is null;

-- New controllers wake upon creation, to begin their daily checks.
create or replace function internal.create_tenant_controller(p_tenant public.catalog_tenant)
returns public.flowid
language plpgsql
security definer
set search_path to ''
as $$
declare
    new_task_id public.flowid;
    -- Must match automations::task_types::TENANT_CONTROLLER = TaskType(12)
    tenant_controller_task_type constant smallint := 12;
begin
    new_task_id := internal.id_generator();

    perform internal.create_task(new_task_id, tenant_controller_task_type, null);

    -- Wake without a message, so the first poll runs only scheduled checks.
    update internal.tasks
    set wake_at = now()
    where task_id = new_task_id;

    update public.tenants
    set controller_task_id = new_task_id
    where tenant = p_tenant;

    return new_task_id;
end;
$$;

create function internal.on_tenant_trial_reset()
returns trigger
language plpgsql
security definer
set search_path to ''
as $$
begin
    perform internal.wake_tenant_controller(new.tenant);
    return new;
end;
$$;

create trigger tenant_trial_reset
    after update of trial_start on public.tenants
    for each row
    when (old.trial_start is not null and new.trial_start is null)
    execute function internal.on_tenant_trial_reset();

commit;
