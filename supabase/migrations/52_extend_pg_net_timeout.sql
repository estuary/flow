begin;

create or replace function internal.send_alerts()
returns trigger as $trigger$
declare
  token text;
begin
  select decrypted_secret into token from vault.decrypted_secrets where name = 'alert-email-fn-shared-secret' limit 1;
    perform
      net.http_post(
        -- 'http://host.docker.internal:5431/functions/v1/alerts',
        'https://eyrcnmuzzyriypdajwdk.supabase.co/functions/v1/alerts',
        to_jsonb(new.*),
        headers:=format('{"Content-Type": "application/json", "Authorization": "Basic %s"}', token)::jsonb,
        timeout_milliseconds:=90000
      );
  return null;
end;
$trigger$ LANGUAGE plpgsql;

commit;