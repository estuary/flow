begin;

create table public.tenant_consent (
    user_id uuid not null references auth.users (id),
    user_email text not null,
    terms_version integer not null check (terms_version > 0),
    timestamp timestamptz not null default now(),
    tenant_name public.catalog_tenant not null,
    tenant_id public.flowid not null references public.tenants (id),
    primary key (tenant_id, user_id, terms_version)
);

comment on table public.tenant_consent is
    'Records the user and tenant associated with acceptance of a version of the terms.';
comment on column public.tenant_consent.user_email is
    'User email at the time consent was recorded.';
comment on column public.tenant_consent.tenant_name is
    'Tenant catalog prefix at the time consent was recorded.';

create index tenant_consent_user_id_idx on public.tenant_consent (user_id);

-- Consent is recorded by the control plane, not directly by API clients.
alter table public.tenant_consent enable row level security;
revoke all on public.tenant_consent from anon, authenticated;
grant all on public.tenant_consent to service_role;

commit;
