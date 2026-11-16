alter table internal.tenant_consent
    add constraint tenant_consent_tenant_terms_key unique (tenant_id, terms_id);
