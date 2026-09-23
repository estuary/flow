BEGIN;

-- Record the deprecation of gcp-us-central1-c1 (combustible-cronut) and its
-- successor c2 as data rather than as code.
--
-- These two planes were excluded from new-tenant storage mappings by a
-- hardcoded list in the control plane. `data_planes.closed` (added in
-- 20260716120000) is the mechanism for precisely this — "closed to new
-- selection, but still serving existing tasks" — and `publicDataPlanes`
-- already honors it, so the signup picker never offers a closed plane.
-- Marking them closed here lets tenant provisioning and claim validation read
-- the same source of truth, and lets a future plane be retired with a data
-- change instead of a code change and a deploy.
--
-- Existing tenants already mapped to these planes are unaffected: `closed`
-- only governs new selection.
UPDATE public.data_planes
SET
  closed = true
WHERE
  data_plane_name IN (
    'ops/dp/public/gcp-us-central1-c1',
    'ops/dp/public/gcp-us-central1-c2'
  );

COMMIT;
