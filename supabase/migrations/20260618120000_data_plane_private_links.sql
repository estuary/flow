-- Model private links as first-class rows with a stable identity and
-- data-plane-controller-owned observed status, replacing the flat
-- `data_planes.private_links` JSON array as the source of truth.
--
-- During the transition a trigger projects rows back into the
-- `data_planes.private_links` column, so the controller (which still reads that
-- column until its own cutover) keeps working unchanged. A later migration
-- drops the projection and the legacy `private_links` / `*_link_endpoints`
-- columns once the controller reads and writes this table directly.

begin;

create table internal.data_plane_private_links (
    id public.flowid primary key not null default internal.id_generator(),
    data_plane_id public.flowid not null
        references public.data_planes (id) on delete cascade,
    -- Cloud provider of the link, stored so consumers need not parse `config`
    -- to learn the variant and so the controller selects the matching endpoint
    -- output array. AWS and Azure links both key on `service_name`, so the
    -- provider is what disambiguates them.
    provider text not null check (provider in ('aws', 'azure', 'gcp')),
    -- The polymorphic link configuration: the same element shape as the legacy
    -- `data_planes.private_links` array; round-trips `models::PrivateLink`.
    config jsonb not null,
    -- The provider's service identifier, used as the join key against the
    -- controller's provisioned endpoint outputs and to enforce uniqueness.
    -- Identities are only meaningful per provider (AWS and Azure both key on
    -- `service_name`), and a same-provider duplicate produces colliding Pulumi
    -- resource names in est-dry-dock, wedging the converge; uniqueness is
    -- therefore scoped to (data_plane_id, provider, service_identity).
    service_identity text generated always as
        (coalesce(config ->> 'service_name', config ->> 'service_attachment')) stored,
    -- Observed state, written by the data-plane controller. `status` is
    -- `pending` until a converge matches a provisioned endpoint; `failed` is
    -- reserved for when est-dry-dock reports per-link errors (a later change).
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

-- Pre-flight: abort the migration on legacy `private_links` data the new
-- table's invariants cannot represent, rather than silently dropping or
-- corrupting it. An element missing both `service_name` and
-- `service_attachment` would produce a NULL generated `service_identity` that
-- bypasses uniqueness; an element missing another required field of its
-- `models::PrivateLink` variant (possible via hand-edits to the column, which
-- were never validated) would backfill fine but fail the resolver's non-null
-- decode at read time, nulling the whole `dataPlanes` query; a duplicate
-- (data_plane_id, provider, service_identity) would collide on the unique
-- constraint. All indicate data needing hand-correction before this migration.
do $$
declare
    v_missing bigint;
    v_undecodable bigint;
    v_dupes bigint;
begin
    select count(*) into v_missing
    from public.data_planes dp,
         lateral unnest(dp.private_links) as elem
    where coalesce(elem ->> 'service_name', elem ->> 'service_attachment') is null;

    if v_missing > 0 then
        raise exception
            'cannot backfill data_plane_private_links: % private_links element(s) lack a service_name/service_attachment',
            v_missing;
    end if;

    -- Mirrors the required fields of each `models::PrivateLink` untagged
    -- variant (AWS: region + az_ids + service_name; Azure: service_name +
    -- location; GCP: service_attachment + region + dns_zone_name +
    -- dns_record_names) so an element that would fail decode aborts here
    -- instead of at read time.
    select count(*) into v_undecodable
    from public.data_planes dp,
         lateral unnest(dp.private_links) as elem
    where not (
        ((elem ->> 'service_name') is not null
            and (elem ->> 'region') is not null
            and (elem ->> 'az_ids') is not null)
        or ((elem ->> 'service_name') is not null
            and (elem ->> 'location') is not null)
        or ((elem ->> 'service_attachment') is not null
            and (elem ->> 'region') is not null
            and (elem ->> 'dns_zone_name') is not null
            and (elem ->> 'dns_record_names') is not null)
    );

    if v_undecodable > 0 then
        raise exception
            'cannot backfill data_plane_private_links: % private_links element(s) do not match any models::PrivateLink variant shape',
            v_undecodable;
    end if;

    select count(*) into v_dupes from (
        select 1
        from public.data_planes dp,
             lateral unnest(dp.private_links) as elem
        group by
            dp.id,
            case
                when (elem ->> 'service_attachment') is not null then 'gcp'
                when (elem ->> 'az_ids') is not null then 'aws'
                else 'azure'
            end,
            coalesce(elem ->> 'service_name', elem ->> 'service_attachment')
        having count(*) > 1
    ) d;

    if v_dupes > 0 then
        raise exception
            'cannot backfill data_plane_private_links: % data plane(s) have duplicate private_links service identities',
            v_dupes;
    end if;
end $$;

-- Backfill one row per element of every existing `private_links` array. Done
-- before the trigger exists, so it does not reproject or wake anything; the
-- column already holds the source data, so column and table are consistent. No
-- `on conflict` clause: the pre-flight above has proven there are no collisions,
-- so any conflict here is an unexpected invariant break that should abort.
insert into internal.data_plane_private_links (data_plane_id, provider, config)
select
    dp.id,
    case
        when (elem ->> 'service_attachment') is not null then 'gcp'
        when (elem ->> 'az_ids') is not null then 'aws'
        else 'azure'
    end,
    elem::jsonb
from public.data_planes dp,
     lateral unnest(dp.private_links) as elem;

-- When a link's desired configuration changes (an insert, a delete, or a
-- `config`/`provider` update; see the trigger's `update of` scope below):
-- reproject the rows back into the parent's `data_planes.private_links` column
-- (the controller still reads it until its cutover), and send the parent's
-- controller task a `Converge` message so it applies the new desired
-- configuration promptly rather than waiting for the next idle poll. The
-- message must deserialize into the data-plane-controller's
-- externally-tagged `Message` enum, whose `Converge` unit variant is the JSON
-- string `"converge"` (this is not the `{"type":...}` shape the live-specs
-- controller uses). The not-idle guard on `data_planes` only blocks
-- `config`/`deploy_branch`, so projecting `private_links` is allowed mid-converge.
create function internal.on_data_plane_private_links_change() returns trigger
    language plpgsql security definer
    set search_path to ''
    as $$
declare
    v_data_plane_id public.flowid := coalesce(new.data_plane_id, old.data_plane_id);
    v_controller_task_id public.flowid;
begin
    update public.data_planes dp set
        private_links = coalesce((
            select array_agg(l.config::json order by l.created_at, l.id)
            from internal.data_plane_private_links l
            where l.data_plane_id = v_data_plane_id
        ), array[]::json[])
    where dp.id = v_data_plane_id
    returning dp.controller_task_id into v_controller_task_id;

    if v_controller_task_id is not null then
        perform internal.send_to_task(
            v_controller_task_id,
            '00:00:00:00:00:00:00:00'::public.flowid,
            '"converge"'::json
        );
    end if;

    return null;
end;
$$;

-- Scoped to inserts, deletes, and updates that touch the user-owned desired
-- columns (`config`/`provider`). The controller's post-converge status write
-- only sets `status`/`details`/`observed_at`/`updated_at`, so it does not fire
-- this trigger; were it to, each converge would reproject, wake the controller,
-- and re-trigger itself in an unbounded reconverge loop.
create trigger on_data_plane_private_links_change
    after insert or delete or update of config, provider on internal.data_plane_private_links
    for each row execute function internal.on_data_plane_private_links_change();

commit;
