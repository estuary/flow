BEGIN;

-- Marks tenants with contractual data-protection obligations: a signed HIPAA
-- BAA, or a DPA that declares GDPR Article 9 special-category data. Internal
-- tooling must not send a flagged tenant's data to external processors.
-- Processing on our own infrastructure is unaffected. This is set from the
-- contract, not detected from the data, so it defaults to false.
ALTER TABLE public.tenants
ADD COLUMN sensitive boolean NOT NULL DEFAULT false;

COMMENT ON COLUMN public.tenants.sensitive IS 'Tenant has a HIPAA BAA or declares GDPR Article 9 data. Do not send its data to external processors.';

COMMIT;
