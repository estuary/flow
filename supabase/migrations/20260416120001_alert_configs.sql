-- Creates `public.alert_configs`, which stores alert thresholds and
-- auto-disable settings for either a catalog prefix or an exact catalog name.
-- Prefix rows apply to all matching tasks beneath the prefix. Exact-name rows
-- apply only to that task. More-specific matches override less-specific ones.
--
-- Accessed exclusively through GraphQL (which runs as `postgres`).
-- Supabase's default privileges auto-grant to `authenticated` and `anon`,
-- so we REVOKE after creation to deny PostgREST access.
CREATE TABLE
    public.alert_configs (
        id public.flowid NOT NULL DEFAULT internal.id_generator (),
        -- `catalog_prefix_or_name` stores either an exact catalog name or a
        -- catalog prefix ending in `/`. `IS NFKC NORMALIZED` prevents visually
        -- identical values with different byte sequences from being stored as
        -- distinct rows.
        catalog_prefix_or_name text NOT NULL CHECK (catalog_prefix_or_name IS NFKC NORMALIZED),
        config jsonb NOT NULL,
        detail text,
        created_at timestamp
        with
            time zone NOT NULL DEFAULT now (),
            updated_at timestamp
        with
            time zone NOT NULL DEFAULT now (),
            last_modified_by uuid REFERENCES auth.users (id) ON DELETE SET NULL,
            CONSTRAINT alert_configs_pkey PRIMARY KEY (id),
            CONSTRAINT alert_configs_prefix_or_name_key UNIQUE (catalog_prefix_or_name)
    );

ALTER TABLE public.alert_configs OWNER TO postgres;

REVOKE ALL ON public.alert_configs FROM authenticated, anon;

COMMENT ON COLUMN public.alert_configs.catalog_prefix_or_name IS '
Either a catalog prefix (ends in "/") or an exact catalog name. A prefix row
applies to every task whose name begins with it; an exact-name row applies
only to that single task.
';

COMMENT ON COLUMN public.alert_configs.config IS '
JSON alert configuration for matching tasks. Fields omitted from a row inherit
from broader matching rows and then from controller defaults.
';

-- Remove any remaining `DATA_MOVEMENT_ALERT_EVALS` (task_type = 11) rows.
-- `DataMovementStalled` is now evaluated by each controller task.
DELETE FROM internal.tasks
WHERE
    task_type = 11;