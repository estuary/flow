BEGIN;

-- Add dekaf_address and dekaf_registry_address to data_planes table.
-- dekaf_address: URL with scheme, e.g., "tls://dekaf.gcp-us-central1-c1.dp.estuary-data.com:9092"
-- dekaf_registry_address: URL with scheme, e.g., "https://dekaf.gcp-us-central1-c1.dp.estuary-data.com"
-- Nullable because not every data-plane has a Dekaf instance.
-- For managed data-planes, these are set by the data-plane-controller when Dekaf is deployed.
-- For manual data-planes, these can be set via the create_data_plane API.
ALTER TABLE public.data_planes
ADD COLUMN dekaf_address TEXT;

ALTER TABLE public.data_planes
ADD COLUMN dekaf_registry_address TEXT;

GRANT
SELECT
  (dekaf_address, dekaf_registry_address) ON public.data_planes TO authenticated;

COMMENT ON COLUMN public.data_planes.dekaf_address IS 'Kafka-protocol URI for this dataplane''s Dekaf instance, or NULL if no Dekaf';

COMMENT ON COLUMN public.data_planes.dekaf_registry_address IS 'Schema registry HTTP endpoint URL for this dataplane''s Dekaf instance, or NULL if no Dekaf';

COMMIT;