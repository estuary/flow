begin;

create or replace function internal.send_alerts()
returns trigger as $trigger$
declare
  token text;
begin

select decrypted_secret into token from vault.decrypted_secrets where name = 'alert-email-fn-shared-secret' limit 1;

if new.alert_type = 'data_not_processed_in_interval' then
  perform
    net.http_post(
      'https://eyrcnmuzzyriypdajwdk.supabase.co/functions/v1/alert-data-processing',
      to_jsonb(new.*),
      headers:=format('{"Content-Type": "application/json", "Authorization": "Basic %s"}', token)::jsonb
    );
end if;

return null;
end;
$trigger$ LANGUAGE plpgsql;

drop trigger "Send alerts" on alert_history;

create trigger "Send email after alert fired" after insert on alert_history
  for each row execute procedure internal.send_alerts();

create trigger "Send email after alert resolved" after update on alert_history
  for each row when (old.resolved_at is null and new.resolved_at is not null) execute procedure internal.send_alerts();

commit;