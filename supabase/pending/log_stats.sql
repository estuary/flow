
ALTER TABLE IF EXISTS public.catalog_stats
    ADD COLUMN errors integer NOT NULL DEFAULT 0,
    ADD COLUMN failures integer NOT NULL DEFAULT 0,
    ADD COLUMN warnings integer NOT NULL DEFAULT 0;