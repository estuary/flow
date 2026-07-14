do $$
declare

begin

  -- Tenants: acmeCo/ and one with preconfigured quotas
  insert into public.tenants (id, tenant, sso_provider_id, enforce_sso) values
    (internal.id_generator(), 'acmeCo/', NULL, false),
    (internal.id_generator(), 'acmeCo2GtQuotas/', NULL, false)
  ;

  -- Updating a testing tenant acmeCo2GtQuotas
  UPDATE public.tenants
    SET tasks_quota = 200, collections_quota = 20000
    WHERE tenants.tenant = 'acmeCo2GtQuotas/';
end
$$;
