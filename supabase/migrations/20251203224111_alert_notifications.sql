begin;

-- explicit default null is necessary because the flowid domain is declared as
-- having a default of a new random id.
alter table public.alert_history add column notification_task_id flowid default null;
comment on column public.alert_history.notification_task_id is
'The id of the automations task that sends alert emails. This will
be null after all resolution notifications have been sent.';

with add_task_ids as (
    update public.alert_history
    set notification_task_id = internal.id_generator()
    where resolved_at is null and notification_task_id is null
    returning notification_task_id
)
select internal.create_task(notification_task_id, 9::smallint, '0000000000000000'::flowid)
from add_task_ids;

create unique index notification_task_id_uniq on alert_history (notification_task_id);


-- This is the function that's executed by the trigger on insert to
-- `alert_history`. We're changing it to bypass this old notification mechanism
-- when the `notification_task_id` is set. This allows the existing alerts to
-- continue functioning while we roll out the new and improved alerts. Also skipping
-- the post request when the token is not defined.
create or replace function internal.send_alerts() returns trigger
    language plpgsql
    as $$
declare
  token text;
begin
  select decrypted_secret into token from vault.decrypted_secrets where name = 'alert-email-fn-shared-secret' limit 1;
  if new.notification_task_id is null and token is not null then
        perform
        net.http_post(
            'https://alerts-1084703453822.us-central1.run.app/',
            to_jsonb(new.*),
            headers:=format('{"Content-Type": "application/json", "Authorization": "Basic %s"}', token)::jsonb,
            timeout_milliseconds:=90000
        );
  end if;
  return null;
end;
$$;

drop function internal.evaluate_alert_events;

drop view internal.alert_all;


commit;
