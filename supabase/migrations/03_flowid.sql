
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
--
-- NOTE(johnny): Also add async tables to the `supabase_realtime` publication:
--   alter publication supabase_realtime add table my_async_table;
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

