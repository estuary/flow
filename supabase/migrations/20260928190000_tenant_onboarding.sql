begin;

alter table public.tenants
    add column created_by uuid references auth.users (id) on delete set null,
    add column metadata jsonb not null default '{}'::jsonb;

create unique index tenants_tenant_lower_key on public.tenants (lower(tenant));

insert into internal.illegal_tenant_names (name) values
    ('ops/'),
    ('recovery/'),
    ('ops.us-central1.v1/')
on conflict (name) do nothing;

commit;
