-- OAuth Client ID and Client Secret are associated uniquely with
-- connectors.
alter table if exists connectors
    add column oauth2_client_id text collate pg_catalog."default";

comment on column connectors.oauth2_client_id
    is 'oauth client id';

alter table if exists connectors
    add column oauth2_client_secret text collate pg_catalog."default";

comment on column connectors.oauth2_client_secret
    is 'oauth client secret';

-- The client secret must not be accessible by clients, and only priviledged
-- services must be able to access such secret.
revoke select on connectors from authenticated;
grant select(id, detail, external_url, image_name, open_graph_raw, open_graph_patch, open_graph, created_at, updated_at, oauth2_client_id) on connectors to authenticated;


-- The new OAuth2Spec part of SpecResponse for connectors should also be persisted
-- see https://github.com/estuary/flow/pull/570/files

alter table if exists connectors
    add column oauth2_spec json_obj;

comment on column connectors.oauth2_spec is
  'OAuth2 specification of the connector';
