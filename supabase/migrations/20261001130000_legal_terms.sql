begin;

create type internal.legal_terms_type as enum ('msa', 'privacy_policy');

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
create function internal.legal_terms_reject_update() returns trigger
language plpgsql as $$
begin
    raise exception 'legal terms are immutable; insert a new version';
end;
$$;

create trigger legal_terms_reject_update
    before update on internal.legal_terms
    for each row execute function internal.legal_terms_reject_update();

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

commit;
