BEGIN;

ALTER TABLE public.tenants
ADD COLUMN sensitive boolean NOT NULL DEFAULT false;

COMMIT;
