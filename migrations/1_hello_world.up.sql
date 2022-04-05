-- We write SQL according to https://www.sqlstyle.guide/
-- It's an arbitrary style guide, but it's important to have one for consistency.
-- We also lower-case SQL keywords, as is common within Supabase documentation.

-- Roles which are created by supabase.
-- create role if not exists anon;
-- create role if not exists authenticated;

-- A new supabase installation grants all in public to anon & authenticated.
-- We elect to NOT do this, instead explicitly granting access to tables and functions
alter default privileges in schema public revoke all on tables from anon, authenticated;
alter default privileges in schema public revoke all on routines from anon, authenticated;
alter default privileges in schema public revoke all on sequences from anon, authenticated;

-- Provide non-browser API clients a way to determine their effective user_id.
create function auth_uid()
returns uuid as $$
begin
  return auth.uid();
end;
$$ language plpgsql stable;
comment on function auth_uid is
  'auth_uid returns the user ID of the authenticated user';

create domain json_obj as json check (json_typeof(value) = 'object');
comment on domain json_obj is
  'json_obj is JSON which is restricted to the "object" type';

create domain jsonb_obj as jsonb check (jsonb_typeof(value) = 'object');
comment on domain jsonb_obj is
  'jsonb_obj is JSONB which is restricted to the "object" type';

create domain flowid as macaddr8;
comment on domain flowid is '
flowid is a montonic, time-ordered ID with gaps that fits within 64 bits.
We use macaddr8 as its underlying storage type because:

 1) It''s stored as exactly 8 bytes, with the same efficiency as BIGINT.
 2) It has a flexible, convienient to_json() behavior that (crucially)
    is loss-less by default when parsed in JavaScript.

Postgres''s to_json() serializes BIGINT as a bare integer,
which is subject to silent rounding by many parsers when values
exceed 53 bits (as is common with flowid).

The canonical flowid encoding is lower-case hexidecimal with each byte
separated by ":", which is what''s returned by Postgres & PostgREST.
Postgres (and PostgREST!) will accept any hex value of the correct
implied length, with bytes optionally separated by any arrangement
of ":" or "-".
';

create domain catalog_name as text
  constraint "Must be NFKC letters, numbers, -, _, ., separated by / and not end in /"
  check (value ~ '^([[:alpha:][:digit:]\-_.]+/)+[[:alpha:][:digit:]\-_.]+$' and value is nfkc normalized);
comment on domain catalog_name is
  'catalog_name is a unique name within the Flow catalog namespace';

create domain catalog_prefix as text
  constraint "Must be NFKC letters, numbers, -, _, ., separated by / and end in /"
  check (value ~ '^([[:alpha:][:digit:]\-_.]+/)+$' and value is nfkc normalized);
comment on domain catalog_prefix is
  'catalog_prefix is a unique prefix within the Flow catalog namespace';

create schema internal;
comment on schema internal is
  'Internal schema used for types, tables, and procedures we don''t expose in our API';

create sequence internal.shard_0_id_sequence;

create function internal.id_generator()
returns flowid as $$
declare
    -- This procedure generates unique 64-bit integers
    -- with the following bit layout:
    --
    --   0b00000010100000101011010111111000100000101010100100011111100011100
    --     |--         Timestamp Millis           --||-- SeqNo --||- Shard-|
    --
    -- Estuary epoch is the first representable timestamp in generated IDs.
    -- This could be zero, but subtracting |estuary_epoch| results in the
    -- high bit being zero for the next ~34 years,
    -- making ID representations equivalent for both signed and
    -- unsigned 64-bit integers.
    estuary_epoch bigint := 1600000000;
    -- The id of this parallizable ID generation shard.
    -- ID's generated inside of PostgreSQL always use |shard_id| zero.
    -- We reserve other shard IDs for future parallized ID generation.
    -- The allowed range is [0, 1024) (10 bits).
    shard_id int := 0;
    -- Sequence number is a monotonic tie-breaker for IDs generated
    -- within the same millisecond.
    -- The allowed range is [0, 8192) (13 bits).
    seq_no bigint;
    -- Current timestamp, as Unix millis since |estuary_epoch|.
    now_millis bigint;
begin
    -- We have 13 low bits of sequence ID, which allow us to generate
    -- up to 8,192 unique IDs within each given millisecond.
    select nextval('internal.shard_0_id_sequence') % 8192 into seq_no;

    select floor((extract(epoch from clock_timestamp()) - estuary_epoch) * 1000) into now_millis;
    return lpad(to_hex((now_millis << 23) | (seq_no << 10) | (shard_id)), 16, '0')::flowid;
end;
$$ language plpgsql
security definer
;
comment on function internal.id_generator is '
id_generator produces 64bit unique, non-sequential identifiers. They:
  * Have fixed storage that''s 1/2 the size of a UUID.
  * Have a monotonic generation order.
  * Embed a wall-clock timestamp than can be extracted if needed.
  * Avoid the leaky-ness of SERIAL id''s.

Adapted from: https://rob.conery.io/2014/05/29/a-better-id-generator-for-postgresql/
Which itself was inspired by http://instagram-engineering.tumblr.com/post/10853187575/sharding-ids-at-instagram
';

-- Set id_generator as the DEFAULT value of a flowid whenever it's used in a table.
alter domain flowid set default internal.id_generator();


-- TODO(johnny): I'm not sure we need this. Leaving here but commented out for the moment.
-- For $reasons PostgreSQL doesn't offer RFC 7396 JSON Merge Patch.
-- Implement as a function, credit to:
-- https://stackoverflow.com/questions/63345280/there-is-a-similar-function-json-merge-patch-in-postgres-as-in-oracle
-- CREATE FUNCTION jsonb_merge_patch("target" JSONB, "patch" JSONB)
-- RETURNS JSONB AS $$
-- BEGIN
--     RETURN COALESCE(jsonb_object_agg(
--         COALESCE("tkey", "pkey"),
--         CASE
--             WHEN "tval" ISNULL THEN "pval"
--             WHEN "pval" ISNULL THEN "tval"
--             WHEN jsonb_typeof("tval") != 'object' OR jsonb_typeof("pval") != 'object' THEN "pval"
--             ELSE jsonb_merge_patch("tval", "pval")
--         END
--     ), '{}'::jsonb)
--       FROM jsonb_each("target") e1("tkey", "tval")
--   FULL JOIN jsonb_each("patch") e2("pkey", "pval")
--         ON "tkey" = "pkey"
--       WHERE jsonb_typeof("pval") != 'null'
--         OR "pval" ISNULL;
-- END;
-- $$ LANGUAGE plpgsql;


-- _model is not used directly, but is a model for other created tables.
create table internal._model (
  created_at  timestamptz not null default now(),
  detail      text,
  id flowid   primary key not null,
  updated_at  timestamptz not null default now()
);

comment on table internal._model is
  'Model table for the creation of other tables';
comment on column internal._model.created_at is
  'Time at which the record was created';
comment on column internal._model.detail is
  'Description of the record';
comment on column internal._model.id is
  'ID of the record';
comment on column internal._model.updated_at is
  'Time at which the record was last updated';

-- _model_async is a model for other created tables that imply server-side operations.
create table internal._model_async (
  like internal._model including all,

  job_status  jsonb_obj not null default '{"type":"queued"}',
  logs_token  uuid not null default gen_random_uuid()
);

comment on table internal._model_async is
  'Model table for the creation of other tables representing a server-side operation';
comment on column internal._model_async.job_status is
  'Server-side job executation status of the record';
comment on column internal._model_async.logs_token is
  'Bearer token for accessing logs of the server-side operation';


-- Log lines are newline-delimited outputs from server-side jobs.
create table internal.log_lines (
  log_line  text not null,
  logged_at timestamptz not null default now(),
  stream    text not null,
  token     uuid not null
);

comment on table internal.log_lines is
  'Logs produced by server-side operations';
comment on column internal.log_lines.log_line is
  'Logged line';
comment on column internal.log_lines.token is
  'Bearer token which demarks and provides accesss to a set of logs';
comment on column internal.log_lines.stream is
  'Identifier of the log stream within the job';
comment on column internal.log_lines.logged_at is
  'Time at which the log was collected';

create index idx_logs_token_logged_at on internal.log_lines
  using brin(token, logged_at) with (autosummarize = on);


-- We cannot provide direct SELECT access to logs, but we *can* provide
-- a view on logs so long as the user always provides a bearer token.
create function view_logs(bearer_token uuid)
returns setof internal.log_lines as $$
begin
  return query select * from internal.log_lines where internal.log_lines.token = bearer_token;
end;
$$ language plpgsql
security definer
;
comment on function view_logs is
  'view_logs accepts a log bearer_token and returns its matching log lines';


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


-- User-initiated discover operations.
create table discovers (
  like internal._model_async including all,

  capture_name      catalog_name not null,
  catalog_spec      json_obj, -- Job output.
  connector_tag_id  flowid not null references connector_tags(id),
  endpoint_config   json_obj not null,
  user_id           uuid references auth.users(id) not null default auth.uid()
);
alter table discovers enable row level security;

create policy "Users can access only their initiated discovery operations"
  on discovers using (user_id = auth.uid());
grant select on discovers to authenticated;
grant insert (capture_name, connector_tag_id, endpoint_config)
  on discovers to authenticated;

comment on table discovers is
  'User-initiated connector discovery operations';
comment on column discovers.capture_name is
  'Intended name of the capture produced by this discover';
comment on column discovers.catalog_spec is
  'Discovered catalog specification, available on job completion';
comment on column discovers.connector_tag_id is
  'Tagged connector which is used for discovery';
comment on column discovers.endpoint_config is
  'Endpoint configuration of the connector. May be protected by sops';
comment on column discovers.user_id is
  'User which initiated this discovery operation';

create index idx_discovers_user_id on discovers(user_id);


-- User submitted drafts.
create table drafts (
  like internal._model_async including all,

  catalog_spec  json_obj not null,
  hide          bool not null default false,
  user_id       uuid references auth.users(id) not null default auth.uid()
);
alter table drafts enable row level security;

create policy "Users can access only their created drafts"
  on drafts using (user_id = auth.uid());
grant insert (hide, catalog_spec) on drafts to authenticated;
grant select on drafts to authenticated;
grant update (hide) on drafts to authenticated;

comment on table drafts is
  'Submitted drafts of Flow catalog specifications';
comment on column drafts.catalog_spec is
  'Submitted Flow catalog specification of this draft';
comment on column drafts.hide is
  'Whether this draft is treated as hidden';
comment on column drafts.user_id is
  'User which created this draft';

create index idx_drafts_user_id on drafts(user_id);
create unique index idx_drafts_id_where_queued on drafts(id)
  where job_status->>'type' = 'queued';
