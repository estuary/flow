begin;

create table internal.invite_links (
    token           uuid primary key default gen_random_uuid(),
    catalog_prefix  public.catalog_prefix not null,
    capability      public.grant_capability not null,
    single_use      boolean not null default false,
    detail          text,
    created_at      timestamptz not null default now()
);

comment on table internal.invite_links is
    'Invite links grant users access to a catalog prefix upon redemption.';
comment on column internal.invite_links.token is
    'Secret bearer token that must be known to redeem this invite link.';
comment on column internal.invite_links.single_use is
    'If true, the row is deleted upon redemption.';

create index idx_invite_links_catalog_prefix on internal.invite_links
    using spgist ((catalog_prefix::text));

------------ TRANSITIONAL LOGIC - TO BE REMOVED AFTER UI ADOPTS GQL API -------------

-- Dual-write trigger: when a grant directive is created via the old PostgREST path,
-- also insert into internal.invite_links so the new GraphQL redeem path can find it.
create or replace function internal.mirror_grant_directive_to_invite_links()
returns trigger as $$
begin
    if (NEW.spec->>'type') = 'grant' then
        begin
            insert into internal.invite_links (
                token,
                catalog_prefix,
                capability,
                single_use,
                detail,
                created_at
            ) values (
                NEW.token,
                (NEW.spec->>'grantedPrefix')::catalog_prefix,
                (NEW.spec->>'capability')::grant_capability,
                NEW.uses_remaining is not null AND NEW.uses_remaining > 0,
                NEW.detail,
                NEW.created_at
            )
            on conflict (token) do nothing;
        exception when others then
            -- Skip directives with invalid spec values (e.g. malformed prefix).
            null;
        end;
    end if;
    return null;
end;
$$ language plpgsql security definer;

create trigger mirror_grant_directive
    after insert on public.directives
    for each row
    execute function internal.mirror_grant_directive_to_invite_links();

-- Reverse sync: when a directive is consumed (uses_remaining drops to 0) or deleted
-- via the old path, remove the corresponding invite_links row.
create or replace function internal.sync_directive_removal_to_invite_links()
returns trigger as $$
begin
    if TG_OP = 'DELETE' then
        delete from internal.invite_links where token = OLD.token;
    elsif (OLD.uses_remaining is not null and OLD.uses_remaining > 0)
       and (NEW.uses_remaining is not null and NEW.uses_remaining <= 0) then
        delete from internal.invite_links where token = NEW.token;
    end if;
    return null;
end;
$$ language plpgsql security definer;

create trigger sync_directive_removal
    after update of uses_remaining or delete on public.directives
    for each row
    execute function internal.sync_directive_removal_to_invite_links();

-- Backfill existing unredeemed grant directives into invite_links.
insert into internal.invite_links (token, catalog_prefix, capability, single_use, detail, created_at)
select
    d.token,
    (d.spec->>'grantedPrefix')::catalog_prefix,
    (d.spec->>'capability')::grant_capability,
    d.uses_remaining is not null,
    d.detail,
    d.created_at
from public.directives d
where (d.spec->>'type') = 'grant'
  and d.token is not null
  and (d.uses_remaining is null or d.uses_remaining > 0)
on conflict (token) do nothing;

commit;
