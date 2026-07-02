-- Reserve privileged / role-sounding tenant names so they cannot be provisioned by users.
-- Names like "admin", "root", or "support" are easily confused with platform roles or with
-- the `admin` grant capability, and make poor (and potentially misleading) tenant prefixes.
-- Reserving them also ensures a user cannot provision the name and thereby inherit any
-- role_grants that were previously created with that name as the subject.
--
-- The onboarding existence check (control_plane_api::directives::beta_onboard::tenant_exists)
-- compares case-insensitively, so a single lowercase entry covers all case variants.
-- Idempotent: safe to re-run and coexists with any names already inserted directly.

insert into internal.illegal_tenant_names (name) values
  ('admin/'),
  ('admin1/'),
  ('administrator/'),
  ('root/'),
  ('superuser/'),
  ('support/'),
  ('security/'),
  ('compliance/'),
  ('developers/'),
  ('everyone/'),
  ('internal/'),
  ('system/'),
  ('billing/')
on conflict (name) do nothing;
