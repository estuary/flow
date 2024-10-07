begin;

-- Ensures that grants can only give capabilities that we actually use.
-- See #1675

alter table user_grants add constraint valid_capability
check (capability = any(array['read'::grant_capability, 'write'::grant_capability, 'admin'::grant_capability]));

alter table role_grants add constraint valid_capability
check (capability = any(array['read'::grant_capability, 'write'::grant_capability, 'admin'::grant_capability]));

commit;
