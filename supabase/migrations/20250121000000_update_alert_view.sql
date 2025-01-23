-- Tenants with external payment methods do not need to give us a CC
-- so we do not want to alert them they are missing a payment method

CREATE OR REPLACE VIEW internal.alert_free_trial_stalled AS
 SELECT 'free_trial_stalled'::public.alert_type AS alert_type,
    (((tenants.tenant)::text || 'alerts/free_trial_stalled'::text))::public.catalog_name AS catalog_name,
    json_build_object('tenant', tenants.tenant, 'recipients', array_agg(DISTINCT jsonb_build_object('email', alert_subscriptions.email, 'full_name', (users.raw_user_meta_data ->> 'full_name'::text))), 'trial_start', tenants.trial_start, 'trial_end', ((tenants.trial_start + '1 mon'::interval))::date) AS arguments,
    true AS firing
   FROM (((public.tenants
     LEFT JOIN public.alert_subscriptions ON ((((alert_subscriptions.catalog_prefix)::text ^@ (tenants.tenant)::text) AND (alert_subscriptions.email IS NOT NULL))))
     LEFT JOIN stripe.customers ON ((customers.name = (tenants.tenant)::text)))
     LEFT JOIN auth.users ON ((((users.email)::text = alert_subscriptions.email) AND (users.is_sso_user IS FALSE))))
  WHERE ((tenants.trial_start IS NOT NULL) AND (tenants.payment_provider != 'external') AND ((now() - (tenants.trial_start)::timestamp with time zone) >= ('1 mon'::interval + '5 days'::interval)) AND (tenants.trial_start <= now()) AND (customers."invoice_settings/default_payment_method" IS NULL))
  GROUP BY tenants.tenant, tenants.trial_start;


ALTER VIEW internal.alert_free_trial_stalled OWNER TO postgres;