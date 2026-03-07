create table internal.invite_links (
    token           uuid primary key default gen_random_uuid(),
    catalog_prefix  public.catalog_prefix not null,
    capability      public.grant_capability not null,
    uses_remaining  bigint,  -- null = unlimited
    detail          text,
    created_by      uuid not null references auth.users(id),
    created_at      timestamptz not null default now(),
    updated_at      timestamptz not null default now()
);

comment on table internal.invite_links is
    'Invite links grant users access to a catalog prefix upon redemption.';
comment on column internal.invite_links.token is
    'Secret bearer token that must be known to redeem this invite link.';
comment on column internal.invite_links.uses_remaining is
    'Number of remaining uses, or null for unlimited. Decremented on each redemption.';
