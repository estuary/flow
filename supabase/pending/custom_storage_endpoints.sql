-- migration to setup custom endpoints for storage mappings

-- If a user supplies a custom storage endpoint, then we'll always use their tenant name as the AWS profile, which is used for looking up
-- the credentials. But the `default` AWS profile is special, and is configured with Flow's own credentials, so if a malicious
-- user created a `default` tenant with a custom storage endpoint, then we could end up sending our credentials to that endpoint.
-- This prevents a user from being able to create such a tenant.
insert into internal.illegal_tenant_names (name) values ('default') on conflict do nothing;

