-- Add max_tier column to data_plane_releases table
ALTER TABLE public.data_plane_releases
  ADD COLUMN max_tier smallint DEFAULT 100 NOT NULL;

-- Add check constraint to ensure max_tier is between 0 and 100
ALTER TABLE public.data_plane_releases
  ADD CONSTRAINT data_plane_releases_max_tier_range CHECK (max_tier >= 0 AND max_tier <= 100);

-- Add comment for the new column
COMMENT ON COLUMN public.data_plane_releases.max_tier IS 'Maximum tier of deployments this release applies to. Deployments with tier <= max_tier will use this release.';
