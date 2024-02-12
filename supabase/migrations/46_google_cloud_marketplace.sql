begin;

create table gcm_accounts(
  id uuid not null primary key,
  obfuscated_id text,
  entitlement_id uuid,

  job_status  jsonb_obj not null default '{"type":"queued"}',
  logs_token  uuid not null default gen_random_uuid()
);

comment on column gcm_accounts.id is
  'Google marketplace user ID, received in the first ACCOUNT_ACTIVE pub/sub event and as the subject of the JWT token during signup';
  
comment on column gcm_accounts.obfuscated_id is
  'Google GAIA ID, received in JWT during sign-up, can be used to sign the user in using OAuth2';

comment on column gcm_accounts.job_status is
  'Server-side job executation status of the record';
comment on column gcm_accounts.logs_token is
  'Bearer token for accessing logs of the server-side operation';

create unique index idx_gcm_accounts_id_where_queued on gcm_accounts(id)
  where job_status->>'type' = 'queued';

alter table tenants add column if not exists gcm_account_id uuid references gcm_accounts(id);

alter type payment_provider_type add value if not exists 'gcm';

commit;
