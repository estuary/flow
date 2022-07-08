
-- Known connectors.
create table connectors (
  like internal._model including all,

  external_url         text not null,
  image_name           text unique not null,
  open_graph           jsonb_obj
    generated always as (internal.jsonb_merge_patch(open_graph_raw, open_graph_patch)) stored,
  open_graph_raw       jsonb_obj,
  open_graph_patch     jsonb_obj,
  oauth2_client_id     text,
  oauth2_client_secret text,
  oauth2_spec          jsonb_obj,
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
comment on column connectors.external_url is
  'External URL which provides more information about the endpoint';
comment on column connectors.image_name is
  'Name of the connector''s container (Docker) image, for example "ghcr.io/estuary/source-postgres"';
comment on column connectors.open_graph is
  'Open-graph metadata for the connector, such as title, description, and image';
comment on column connectors.open_graph_raw is
  'Open-graph metadata as returned by the external_url';
comment on column connectors.open_graph_patch is
  'Patches to open-graph metadata, as a JSON merge patch';
comment on column connectors.oauth2_client_id is
  'oauth client id';
comment on column connectors.oauth2_client_secret is
  'oauth client secret';
comment on column connectors.oauth2_spec is
  'OAuth2 specification of the connector';

-- don't expose details of open_graph raw responses & patching and oauth2 secret
-- authenticated may select other columns for all connectors connectors.
grant select(id, detail, updated_at, created_at, image_name, external_url, open_graph, oauth2_client_id) on table connectors to authenticated;


-- TODO(johnny): Here's the plan for open graph:
-- For any given connector, we need to identify a suitable URL which is typically
-- just it's website, like https://postgresql.org or https://hubspot.com.
-- We can fetch Open Graph responses from these URL as an administrative scripted task.
-- We can shell out for this, and this tool seems to do a pretty good job of it:
--   go install github.com/johnreutersward/opengraph/cmd/opengraph@latest
--
-- Example:
-- ~/go/bin/opengraph -json https://postgresql.org | jq 'map( { (.Property|tostring): .Content } ) | add'
-- {
--   "url": "https://www.postgresql.org/",
--   "type": "article",
--   "image": "https://www.postgresql.org/media/img/about/press/elephant.png",
--   "title": "PostgreSQL",
--   "description": "The world's most advanced open source database.",
--   "site_name": "PostgreSQL"
-- }
--
-- We'll store these responses verbatim in `open_graph_raw`.
-- Payloads almost always include `title`, `image`, `description`, `url`, sometimes `site_name`,
-- and sometimes other things. Often the responses are directly suitable for inclusion
-- in user-facing UI components. A few sites don't support any scrapping at all
-- (a notable example is Google analytics), and others return fields which aren't quite
-- right or suited for direct display within our UI.
--
-- So, we'll need to tweak many of them, and we'll do this by maintaining minimal
-- patches of open-graph responses in the `open_graph_patch`. These can be dynamically
-- edited via Supabase as needed, as an administrative function, and are applied
-- via JSON merge patch to the raw responses, with the merged object stored in the
-- user-facing `open_graph` column. Keeping patches in the database allows non-technical
-- folks to use Supabase, Retool, or similar to edit this stuff without getting
-- an engineer involved.
--
-- We can, for example, specify '{"title":"A better title"}' within the connector patch,
-- which will update the `open_graph` response while leaving all other fields (say, the
-- `description` or `image`) as they are in the raw response. This is important because
-- it gives us an easy means to periodically update connector logos, text copy, etc.


create table connector_tags (
  like internal._model_async including all,

  connector_id          flowid not null references connectors(id),
  documentation_url     text,     -- Job output.
  endpoint_spec_schema  json_obj, -- Job output.
  image_tag             text not null,
  protocol              text,     -- Job output.
  resource_spec_schema  json_obj, -- Job output.
  unique(connector_id, image_tag),
  --
  constraint "image_tag must start with : (as in :latest) or @sha256:<hash>"
    check (image_tag like ':%' or image_tag like '@sha256:')
);
-- Public, no RLS.
alter publication supabase_realtime add table connector_tags;

comment on table connector_tags is '
Available image tags (versions) of connectors.
Tags are _typically_ immutable versions,
but it''s possible to update the image digest backing a tag,
which is arguably a different version.
';
comment on column connector_tags.connector_id is
  'Connector which this record is a tag of';
comment on column connector_tags.documentation_url is
  'Documentation URL of the tagged connector';
comment on column connector_tags.endpoint_spec_schema is
  'Endpoint specification JSON-Schema of the tagged connector';
comment on column connector_tags.image_tag is
  'Image tag, in either ":v1.2.3", ":latest", or "@sha256:<a-sha256>" form';
comment on column connector_tags.protocol is
  'Protocol of the connector';
comment on column connector_tags.resource_spec_schema is
  'Resource specification JSON-Schema of the tagged connector';

-- authenticated may select all connector_tags without restrictions.
grant select on table connector_tags to authenticated;

create unique index idx_connector_tags_id_where_queued on connector_tags(id)
  where job_status->>'type' = 'queued';

