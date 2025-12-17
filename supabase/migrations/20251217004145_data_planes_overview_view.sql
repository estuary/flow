-- Creates an overview view for data_planes with key operational information
BEGIN;

CREATE OR REPLACE VIEW public.data_planes_overview AS
SELECT
    dp.data_plane_name,
    dp.data_plane_fqdn,
    -- Cloud provider from first deployment's template
    (dp.config::jsonb->'deployments'->0->'template'->>'provider') AS cloud_provider,
    dp.status,
    -- Extract version for each role from deployments array
    (SELECT d->>'oci_image'
     FROM jsonb_array_elements(dp.config::jsonb->'deployments') AS d
     WHERE d->>'role' = 'etcd' LIMIT 1) AS etcd_version,
    (SELECT d->>'oci_image'
     FROM jsonb_array_elements(dp.config::jsonb->'deployments') AS d
     WHERE d->>'role' = 'gazette' LIMIT 1) AS gazette_version,
    (SELECT d->>'oci_image'
     FROM jsonb_array_elements(dp.config::jsonb->'deployments') AS d
     WHERE d->>'role' = 'reactor' LIMIT 1) AS reactor_version,
    (SELECT d->>'oci_image'
     FROM jsonb_array_elements(dp.config::jsonb->'deployments') AS d
     WHERE d->>'role' = 'dekaf' LIMIT 1) AS dekaf_version,
    -- BYOC if any of these fields are present in config
    CASE
        WHEN dp.config::jsonb ? 'azure_byoc'
          OR dp.config::jsonb ? 'aws_assume_role'
          OR dp.config::jsonb ? 'gcp_byoc'
        THEN 'BYOC'
        ELSE 'Private'
    END AS deployment_type,
    -- Time since last successful pulumi up (from controller task inner_state)
    NOW() - (t.inner_state::jsonb->>'last_pulumi_up')::timestamptz AS time_since_last_successful_run,
    -- Reactor counts
    (SELECT (d->>'desired')::int
     FROM jsonb_array_elements(dp.config::jsonb->'deployments') AS d
     WHERE d->>'role' = 'reactor' LIMIT 1) AS reactors_desired,
    (SELECT (d->>'current')::int
     FROM jsonb_array_elements(dp.config::jsonb->'deployments') AS d
     WHERE d->>'role' = 'reactor' LIMIT 1) AS reactors_current,
    -- Private links configuration
    dp.private_links,
    dp.aws_link_endpoints,
    dp.azure_link_endpoints,
    dp.enable_l2
FROM public.data_planes dp
LEFT JOIN internal.tasks t ON dp.controller_task_id = t.task_id;

COMMENT ON VIEW public.data_planes_overview IS
'Overview of data planes with key operational metrics including versions, deployment type, and health indicators.';

COMMIT;
