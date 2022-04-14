
-- Known connectors.
create table connectors (
  like internal._model including all,

  image_name  text unique not null,
  --
  constraint "image_name must be a container image without a tag"
    check (image_name ~ '^(?:.+/)?([^:]+)$')
);
-- Public, no RLS.

comment on table connectors is '
Connectors are Docker / OCI images which implement a standard protocol,
and allow Flow to interface with an external system for the capture
or materialization of data.
';
comment on column connectors.image_name is
  'Name of the connector''s container (Docker) image';

-- authenticated may select all connectors without restrictions.
grant select on table connectors to authenticated;


create table connector_tags (
  like internal._model_async including all,

  connector_id          flowid not null references connectors(id),
  documentation_url     text,     -- Job output.
  endpoint_spec_schema  json_obj, -- Job output.
  image_tag             text not null,
  protocol              text,     -- Job output.
  --
  constraint "image_tag must start with : (as in :latest) or @sha256:<hash>"
    check (image_tag like ':%' or image_tag like '@sha256:')
);
-- Public, no RLS.

comment on table connector_tags is '
Available image tags (versions) of connectors.
Tags are _typically_ immutable versions,
but it''s possible to update the image digest backing a tag,
which is arguably a different version.
';
comment on column connector_tags.connector_id is
  'Connector which this record is a tag of';
comment on column connector_tags.documentation_url is
  'Documentation URL of the tagged connector, available on job completion';
comment on column connector_tags.endpoint_spec_schema is
  'Endpoint specification JSON-Schema of the tagged connector, available on job completion';
comment on column connector_tags.image_tag is
  'Image tag, in either ":v1.2.3", ":latest", or "@sha256:<a-sha256>" form';
comment on column connector_tags.protocol is
  'Protocol of the connector, available on job completion';

-- authenticated may select all connector_tags without restrictions.
grant select on table connector_tags to authenticated;

create index idx_connector_tags_connector_id on connector_tags(connector_id);
create unique index idx_connector_tags_id_where_queued on connector_tags(id)
  where job_status->>'type' = 'queued';

