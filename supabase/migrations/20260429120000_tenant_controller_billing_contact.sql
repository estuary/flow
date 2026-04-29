-- Add billing contact fields and per-tenant controller task infrastructure.

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
    SELECT controller_task_id INTO v_task_id
    FROM public.tenants
    WHERE tenant = p_tenant;

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

-- Trigger: wake the controller when billing_email or billing_address changes.
CREATE OR REPLACE FUNCTION internal.on_tenant_billing_contact_update()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO ''
AS $$
BEGIN
    IF (OLD.billing_email IS DISTINCT FROM NEW.billing_email)
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

-- Backfill billing_email and billing_address from the CDC-synced stripe.customers table.
UPDATE public.tenants t
SET
    billing_email = COALESCE(t.billing_email, sc.email),
    billing_address = COALESCE(t.billing_address, sc.address::jsonb)
FROM stripe.customers sc
WHERE sc.metadata->>'estuary.dev/tenant_name' = t.tenant
  AND (
      (t.billing_email IS NULL AND sc.email IS NOT NULL)
   OR (t.billing_address IS NULL AND sc.address IS NOT NULL)
  );

-- Create controller tasks only for existing tenants that now have billing
-- data. Tenants without billing data get a task lazily via
-- wake_tenant_controller when setBillingContact or customer creation runs.
DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN
        SELECT tenant FROM public.tenants
        WHERE controller_task_id IS NULL
          AND (billing_email IS NOT NULL OR billing_address IS NOT NULL)
    LOOP
        PERFORM internal.create_tenant_controller(r.tenant);
    END LOOP;
END;
$$;
