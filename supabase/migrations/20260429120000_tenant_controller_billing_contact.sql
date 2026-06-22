-- Add billing contact fields and per-tenant controller task infrastructure.

BEGIN;

ALTER TABLE public.tenants
    ADD COLUMN billing_email text,
    ADD COLUMN billing_name text,
    ADD COLUMN billing_address jsonb,
    ADD COLUMN controller_task_id public.flowid;

-- Helper: create a tenant controller task and store its id on the tenant row.
CREATE OR REPLACE FUNCTION internal.create_tenant_controller(p_tenant public.catalog_tenant)
RETURNS public.flowid
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO ''
AS $$
DECLARE
    new_task_id public.flowid;
    -- Must match automations::task_types::TENANT_CONTROLLER = TaskType(12)
    tenant_controller_task_type CONSTANT smallint := 12;
BEGIN
    new_task_id := internal.id_generator();

    PERFORM internal.create_task(new_task_id, tenant_controller_task_type, NULL);

    UPDATE public.tenants
    SET controller_task_id = new_task_id
    WHERE tenant = p_tenant;

    RETURN new_task_id;
END;
$$;

-- Helper: wake a tenant controller, creating it lazily if one doesn't exist yet.
CREATE OR REPLACE FUNCTION internal.wake_tenant_controller(p_tenant public.catalog_tenant)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO ''
AS $$
DECLARE
    v_task_id public.flowid;
BEGIN
    -- Lock the tenant row so concurrent first-time wakes (e.g. a setBillingContact
    -- update racing customer creation) cannot both create a controller task and
    -- orphan one of them.
    SELECT controller_task_id INTO v_task_id
    FROM public.tenants
    WHERE tenant = p_tenant
    FOR UPDATE;

    IF v_task_id IS NULL THEN
        v_task_id := internal.create_tenant_controller(p_tenant);
    END IF;

    PERFORM internal.send_to_task(
        v_task_id,
        '00:00:00:00:00:00:00:00'::public.flowid,
        '{"type":"wake"}'::json
    );
END;
$$;

-- Trigger: auto-create a controller task when a new tenant is inserted.
CREATE OR REPLACE FUNCTION internal.on_tenant_insert_create_controller()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO ''
AS $$
BEGIN
    PERFORM internal.create_tenant_controller(NEW.tenant);
    RETURN NEW;
END;
$$;

CREATE TRIGGER tenant_insert_create_controller
    AFTER INSERT ON public.tenants
    FOR EACH ROW
    EXECUTE FUNCTION internal.on_tenant_insert_create_controller();

-- Backfill billing contact fields from the CDC-synced stripe.customers table so
-- existing rows mirror Stripe. This runs before the billing-contact update
-- trigger is created (below), so it does not wake any controllers: the data came
-- from Stripe, so reconciling it back would be a no-op. Backfilled tenants get a
-- controller lazily via wake_tenant_controller on their first setBillingContact.
--
-- billing_name is only backfilled when the Stripe customer name differs from the
-- tenant slug. Customers created by Flow set Customer.name to the tenant slug, so
-- copying that into billing_name would surface the slug as the contact name; only
-- a manually-set customer name is a real billing-contact name worth backfilling.
UPDATE public.tenants t
SET
    billing_email = COALESCE(t.billing_email, sc.email),
    billing_name = COALESCE(t.billing_name, NULLIF(sc.name, (t.tenant)::text)),
    billing_address = COALESCE(t.billing_address, sc.address::jsonb)
FROM stripe.customers sc
WHERE sc.metadata->>'estuary.dev/tenant_name' = t.tenant
  AND (
      (t.billing_email IS NULL AND sc.email IS NOT NULL)
   OR (t.billing_name IS NULL AND sc.name IS NOT NULL AND sc.name <> (t.tenant)::text)
   OR (t.billing_address IS NULL AND sc.address IS NOT NULL)
  );

-- Trigger: wake the controller when any billing contact field changes. Created
-- after the backfill so the backfill's writes do not wake every backfilled
-- tenant's controller at deploy time.
CREATE OR REPLACE FUNCTION internal.on_tenant_billing_contact_update()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO ''
AS $$
BEGIN
    IF (OLD.billing_email IS DISTINCT FROM NEW.billing_email)
       OR (OLD.billing_name IS DISTINCT FROM NEW.billing_name)
       OR (OLD.billing_address IS DISTINCT FROM NEW.billing_address) THEN
        PERFORM internal.wake_tenant_controller(NEW.tenant);
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER tenant_billing_contact_update
    AFTER UPDATE ON public.tenants
    FOR EACH ROW
    EXECUTE FUNCTION internal.on_tenant_billing_contact_update();

COMMIT;
