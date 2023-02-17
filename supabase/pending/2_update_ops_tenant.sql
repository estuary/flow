-- This should be run after 1_single_ops_collection.sql is complete, because removing ops/ tasks
-- requires the grants for ops/ to remain active for those builds to succeed.

begin;

-- Rename the existing ops tenant to the new specific ops tenant. This needs to happen _after_ the
-- above publications are created so that the original ops/ tenant tasks are included in those too.
update tenants set tenant = 'ops.us-central1.v1/' where tenant = 'ops/';

-- Update the user grant for the support@estuary.dev account to have access to the renamed ops tenant.
update user_grants set object_role = 'ops.us-central1.v1/' where object_role = 'ops/';

-- Update the role grants for the renamed ops tenant. This will effectively update two grants: The
-- write capability for ops -> ops, and the read capability for ops -> estuary/public. There will
-- remain a now defunct ops/ -> ops/ops read grant for all tenants.
update role_grants set subject_role = 'ops.us-central1.v1/' where subject_role = 'ops/';
update role_grants set object_role = 'ops.us-central1.v1/' where object_role = 'ops/';

-- Update storage mappings for the new ops tenant.
update storage_mappings set catalog_prefix = 'ops.us-central1.v1/' where catalog_prefix = 'ops/';
update storage_mappings set catalog_prefix = 'recovery/ops.us-central1.v1/' where catalog_prefix = 'recovery/ops/';

commit;
