ALTER TABLE IF EXISTS public.connectors
    ADD COLUMN oauth2_injected_values jsonb_obj;

COMMENT ON COLUMN public.connectors.oauth2_injected_values
    IS 'oauth additional injected values, these values will be made available in the credentials key of the connector, as well as when rendering oauth2_spec templates';
