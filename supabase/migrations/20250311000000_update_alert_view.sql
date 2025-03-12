-- If a tenant has `external` payment method then we do not
-- want to trigger any messaging about missing payment.

CREATE OR REPLACE VIEW internal.alert_free_trial_ending AS
 SELECT 'free_trial_ending'::public.alert_type AS alert_type,
    (((tenants.tenant)::text || 'alerts/free_trial_ending'::text))::public.catalog_name AS catalog_name,
    json_build_object('tenant', tenants.tenant, 'recipients', array_agg(DISTINCT jsonb_build_object('email', alert_subscriptions.email, 'full_name', (users.raw_user_meta_data ->> 'full_name'::text))), 'trial_start', tenants.trial_start, 'trial_end', ((tenants.trial_start + '1 mon'::interval))::date, 'has_credit_card', bool_or((customers."invoice_settings/default_payment_method" IS NOT NULL) OR (tenants.payment_provider = 'external'))) AS arguments,
    ((tenants.trial_start IS NOT NULL) AND ((now() - (tenants.trial_start)::timestamp with time zone) >= ('1 mon'::interval - '5 days'::interval)) AND ((now() - (tenants.trial_start)::timestamp with time zone) < ('1 mon'::interval - '4 days'::interval)) AND (tenants.trial_start <= now())) AS firing
   FROM (((public.tenants
     LEFT JOIN public.alert_subscriptions ON ((((alert_subscriptions.catalog_prefix)::text ^@ (tenants.tenant)::text) AND (alert_subscriptions.email IS NOT NULL))))
     LEFT JOIN stripe.customers ON ((customers.name = (tenants.tenant)::text)))
     LEFT JOIN auth.users ON ((((users.email)::text = alert_subscriptions.email) AND (users.is_sso_user IS FALSE))))
  GROUP BY tenants.tenant, tenants.trial_start;


ALTER VIEW internal.alert_free_trial_ending OWNER TO postgres;