begin;

create table internal.gcm_accounts(
  id uuid not null primary key,
  obfuscated_id text,
  entitlement_id uuid,

  approved boolean default false
);

comment on column internal.gcm_accounts.id is
  'Google marketplace user ID, received in the first ACCOUNT_ACTIVE pub/sub event and as the subject of the JWT token during signup';
  
comment on column internal.gcm_accounts.obfuscated_id is
  'Google GAIA ID, received in JWT during sign-up, can be used to sign the user in using OAuth2';

comment on column internal.gcm_accounts.approved is
  'Has the account been approved with Google';

create unique index idx_gcm_accounts_id_where_approved on internal.gcm_accounts(id)
  where approved=true;

alter table tenants add column if not exists gcm_account_id uuid references internal.gcm_accounts(id);

commit;
