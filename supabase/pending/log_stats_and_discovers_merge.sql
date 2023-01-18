
ALTER TABLE IF EXISTS public.catalog_stats
    ADD COLUMN errors integer NOT NULL DEFAULT 0,
    ADD COLUMN failures integer NOT NULL DEFAULT 0,
    ADD COLUMN warnings integer NOT NULL DEFAULT 0;

DELETE FROM flow_materializations_v2;

ALTER TABLE IF EXISTS public.discovers
    ADD COLUMN update_only boolean NOT NULL DEFAULT false;

COMMENT ON COLUMN public.discovers.update_only
    IS '
If true, this operation will draft updates to existing bindings and their
target collections but will not add new bindings or collections.';