BEGIN;

CREATE TABLE public.data_plane_releases (
  created_at     TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
  active         BOOLEAN NOT NULL DEFAULT TRUE,
  prev_image     TEXT PRIMARY KEY NOT NULL,
  next_image     TEXT NOT NULL,
  step           INTEGER NOT NULL,
  data_plane_id  public.flowid NOT NULL
);

COMMENT ON TABLE public.data_plane_releases IS
'Releases which are or have been deployed to data planes';
COMMENT ON COLUMN public.data_plane_releases.created_at IS
'Time of the creation of this release';
COMMENT ON COLUMN public.data_plane_releases.active IS
'Active releases are matched against current deployments to start a rollout. Inactive releases are not, and exist only as a historical record';
COMMENT ON COLUMN public.data_plane_releases.prev_image IS
'Previous deployment OCI image being replaced by this release';
COMMENT ON COLUMN public.data_plane_releases.next_image IS
'Next deployment OCI image being applied by this release';
COMMENT ON COLUMN public.data_plane_releases.step IS
'Number of instances to replace (when negative) or surge (when positive) with each rollout step of this release';
COMMENT ON COLUMN public.data_plane_releases.data_plane_id IS
'Data-plane to which this release is filtered. If "00:00:00:00:00:00:00:00", then the release applies to all data-planes';

END;