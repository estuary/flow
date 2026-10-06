begin;

-- Keep the signup picker and GraphQL tenant provisioning on the same set of
-- open planes. Existing tenants and legacy directive behavior are unaffected.
update public.data_planes set closed = true
where data_plane_name in (
    'ops/dp/public/gcp-us-central1-c1',
    'ops/dp/public/gcp-us-central1-c2'
);

commit;
