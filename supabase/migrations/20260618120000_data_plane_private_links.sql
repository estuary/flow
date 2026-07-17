-- Model private links as first-class rows with a stable identity and
-- data-plane-controller-owned observed status, replacing the flat
-- `data_planes.private_links` JSON array as the source of truth. The agent-api
-- resolvers and the controller both read this table directly, and the controller
-- writes each link's observed status back to it. There is no wake trigger: the
-- controller re-reads desired links from this table on its poll loop and
-- detects changes by config and generation diff.
--
-- The legacy `data_planes.private_links` and `*_link_endpoints` columns are left
-- in place. A controller still running the pre-cutover binary during the rolling
-- deploy keeps reading `private_links`, so the column must survive the rollout;
-- nothing projects table changes back into it, so it is frozen at its
-- pre-migration value. A follow-up migration drops it and the endpoint columns
-- and recreates `data_planes_overview` once the fleet is fully cut over. Private
-- link edits are paused for the duration of the rolling deploy so the frozen
-- column and the table cannot disagree while both old and new controllers run.

begin;

create table internal.data_plane_private_links (
    id public.flowid primary key not null default internal.id_generator(),
    data_plane_id public.flowid not null
        references public.data_planes (id) on delete cascade,
    -- The polymorphic link configuration: the same element shape as the legacy
    -- `data_planes.private_links` array; round-trips `models::PrivateLink`.
    config jsonb not null,
    -- Cloud provider of the link, stored so consumers need not parse `config`
    -- to learn the variant. It is generated so it cannot drift from the config
    -- it describes. AWS and Azure links both key on `service_name`, so the
    -- provider disambiguates them.
    provider text generated always as
        (case
            when (config ->> 'service_attachment') is not null then 'gcp'
            when (config ->> 'az_ids') is not null then 'aws'
            else 'azure'
        end) stored,
    -- Monotonic version of the desired configuration, bumped by the
    -- desired-edit trigger below whenever `config` changes. The
    -- controller pins each link's `(id, generation)` when it reads desired state
    -- for a converge and lands that converge's observed status only on rows whose
    -- generation still matches, so an edit racing a converge cannot be stamped
    -- with a status computed from the pre-edit configuration.
    generation bigint not null default 1,
    -- The provider's service identifier, used to enforce uniqueness.
    -- Identities are only meaningful per provider (AWS and Azure both key on
    -- `service_name`), and a same-provider duplicate produces colliding Pulumi
    -- resource names in est-dry-dock, wedging the converge; uniqueness is
    -- therefore scoped to (data_plane_id, provider, service_identity).
    service_identity text generated always as
        (coalesce(config ->> 'service_name', config ->> 'service_attachment')) stored not null,
    -- Observed state, written by the data-plane controller from est-dry-dock's
    -- per-link `link_results` export, addressed by each row's id.
    status text not null default 'pending' check (status in ('pending', 'provisioned', 'failed')),
    details jsonb,
    error text,
    observed_at timestamptz,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    constraint data_plane_private_links_unique_provider_identity
        unique (data_plane_id, provider, service_identity)
);

comment on table internal.data_plane_private_links is
    'Per-link private networking configuration (desired) and controller-observed status for a data plane.';

-- No separate data_plane_id index: the unique constraint's index leads with
-- data_plane_id and serves those lookups.

-- No RLS and no grants. The table lives in `internal`, which PostgREST does not
-- expose (`config.toml` lists only `public`), so it is never reachable over the
-- REST API. Every reader connects as `postgres`: the agent-api resolvers, which
-- gate on the View/Modify private-networking capabilities, and the data-plane
-- controller. A row-level policy would only bind a PostgREST caller, and none
-- can reach an `internal` table. This mirrors `internal.invite_links`.

-- Backfill one row per element of every existing `private_links` array. The
-- column already holds the source data, so column and table start consistent.
-- The generated identity's NOT NULL constraint and the unique constraint abort
-- the migration if the legacy data cannot satisfy the table's invariants.
insert into internal.data_plane_private_links (data_plane_id, config)
select
    dp.id,
    elem::jsonb
from public.data_planes dp,
     lateral unnest(dp.private_links) as elem;

-- Any change to the user-owned desired config invalidates the observed status:
-- bump the generation and clear the observation columns in the same write. The
-- invariant then holds for every writer, not just the API mutations but also
-- the hand edits support performs directly against this table. Scoped to
-- `update of config` so the controller's post-converge status write
-- (which sets only status/details/observed_at/updated_at) does not fire it and
-- does not bump the generation it just pinned.
-- Runs as invoker (not `security definer`): it only rewrites fields of the row
-- already being updated.
create function internal.on_data_plane_private_links_desired_edit() returns trigger
    language plpgsql
    set search_path to ''
    as $$
begin
    new.generation := old.generation + 1;
    new.status := 'pending';
    new.details := null;
    new.error := null;
    new.observed_at := null;
    new.updated_at := now();
    return new;
end;
$$;

create trigger data_plane_private_links_desired_edit
    before update of config on internal.data_plane_private_links
    for each row when (old.config is distinct from new.config)
    execute function internal.on_data_plane_private_links_desired_edit();

commit;
