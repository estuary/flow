-- Prevent tenants from enabling enforce_sso without an sso_provider_id.

alter table public.tenants
  add constraint tenants_enforce_sso_requires_provider
  check (not (enforce_sso and sso_provider_id is null));

