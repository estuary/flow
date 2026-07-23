BEGIN;

-- Whether a data plane is closed to new selection. A closed data plane remains
-- in the catalog (and keeps serving existing tasks) but is hidden from the
-- default `dataPlanes` listing and from `publicDataPlanes`, so operators can
-- retire a plane from new selection without deleting its record. Defaults to
-- false so every existing plane stays open.
ALTER TABLE public.data_planes
ADD COLUMN closed boolean NOT NULL DEFAULT false;

GRANT
SELECT
  (closed) ON public.data_planes TO authenticated;

COMMENT ON COLUMN public.data_planes.closed IS 'Whether this data plane is closed to new selection';

COMMIT;
