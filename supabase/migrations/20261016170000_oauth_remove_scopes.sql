-- OAuth always authorizes full access within the user's current grants.
begin;
alter table internal.oauth_requests drop column scope;
alter table internal.oauth_grants drop column scope;
commit;
