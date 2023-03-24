ALTER TABLE IF EXISTS public.connectors
    ADD COLUMN long_description jsonb_internationalized_value;

COMMENT ON COLUMN public.connectors.long_description
    IS 'A longform description of this connector. Represented as a json object with IETF language tags as keys (https://en.wikipedia.org/wiki/IETF_language_tag), and the description string as values';