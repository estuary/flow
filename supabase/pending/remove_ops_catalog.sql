begin;

-- We don't need this column anymore.
alter table tenants drop column l1_stat_rollup;

-- We don't need this table or publication function either. Agent will no longer create a reporting
-- publication for each tenant.
drop table ops_catalog_template;
drop function internal.create_ops_publication(tenant_prefix catalog_tenant, ops_user_id uuid);

end;
