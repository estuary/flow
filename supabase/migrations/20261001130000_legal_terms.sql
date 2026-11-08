begin;

create type internal.legal_terms_type as enum ('msa');

create table internal.legal_terms (
    id public.flowid primary key not null default internal.id_generator(),
    type internal.legal_terms_type not null,
    text text not null,
    version integer not null check (version > 0),
    created_at timestamptz not null default now(),
    unique (type, version)
);

-- Users accept a specific (type, version) of the terms, so published text must
-- never change. New terms are published as a new version.
create function internal.legal_terms_reject_mutation() returns trigger
language plpgsql as $$
begin
    raise exception 'legal terms are immutable; insert a new version';
end;
$$;

create trigger legal_terms_reject_mutation
    before update or delete or truncate on internal.legal_terms
    for each statement execute function internal.legal_terms_reject_mutation();

-- Each type has its own version sequence, starting at 1. An insert that doesn't
-- define a version gets the next version number of its type.
create function internal.legal_terms_assign_version() returns trigger
language plpgsql as $$
begin
    if new.version is not null then
        return new;
    end if;

    select coalesce(max(version), 0) + 1 into new.version
    from internal.legal_terms
    where type = new.type;
    return new;
end;
$$;

create trigger legal_terms_assign_version
    before insert on internal.legal_terms
    for each row execute function internal.legal_terms_assign_version();

-- Preserve the original identity even after the user or tenant is deleted.
-- Foreign keys (including SET NULL actions) would couple this immutable audit
-- record to the lifecycle of those rows.
create table internal.tenant_consent (
    id public.flowid primary key not null default internal.id_generator(),
    user_id uuid not null,
    user_email text not null,
    terms_id public.flowid not null references internal.legal_terms (id),
    created_at timestamptz not null default now(),
    tenant_name public.catalog_tenant not null,
    -- `not null default null` overrides flowid's generated default:
    -- insert will fail unless a tenant ID is explicitly provided.
    tenant_id public.flowid not null default null
);

create index tenant_consent_tenant_id_idx on internal.tenant_consent (tenant_id);

create function internal.tenant_consent_reject_mutation() returns trigger
language plpgsql as $$
begin
    raise exception 'tenant consent is immutable; insert a new record';
end;
$$;

create trigger tenant_consent_reject_mutation
    before update or delete or truncate on internal.tenant_consent
    for each statement execute function internal.tenant_consent_reject_mutation();

commit;
