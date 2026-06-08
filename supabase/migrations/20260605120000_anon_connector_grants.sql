-- Allow unauthenticated (anon) access to public connector catalog metadata,
-- excluding sensitive columns such as oauth2 configuration.

GRANT SELECT (id, created_at, external_url, image_name, short_description,
              long_description, title, logo_url, recommended)
    ON public.connectors TO anon;
GRANT SELECT (connector_id, protocol, documentation_url)
    ON public.connector_tags TO anon;
