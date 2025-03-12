begin;

-- Populate these new columns. Note that the legacy data plane needs some
-- special handling because it doesn't follow the normal naming convention.
-- The substr call is to strip off the 'ops/dp/' prefix.
update public.data_planes
set
    ops_l1_events_name = case when data_plane_name = 'ops/dp/public/gcp-us-central1-c1'
        then 'ops/rollups/L1/public/gcp-us-central1-c1/events'
        else concat('ops/rollups/L1/', substr(data_plane_name, 8), '/events')
        end,
    ops_l2_events_transform = concat('from.', data_plane_fqdn, '.events')
where
    ops_l1_events_name is null and ops_l2_events_transform is null;

alter table public.data_planes alter column ops_l1_events_name set not null;
alter table public.data_planes alter column ops_l2_events_transform set not null;

commit;
