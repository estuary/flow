-- Wake a tenant's controller when its payment_provider is set to 'external', so
-- the controller can reconcile the tenant against the change in how it's billed.

BEGIN;

CREATE OR REPLACE FUNCTION internal.on_tenant_payment_provider_update()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO ''
AS $$
BEGIN
    IF (OLD.payment_provider IS DISTINCT FROM NEW.payment_provider)
       AND (NEW.payment_provider = 'external'::public.payment_provider_type) THEN
        PERFORM internal.wake_tenant_controller(NEW.tenant);
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER tenant_payment_provider_update
    AFTER UPDATE ON public.tenants
    FOR EACH ROW
    EXECUTE FUNCTION internal.on_tenant_payment_provider_update();

COMMIT;
