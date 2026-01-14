--
-- PostgreSQL database dump
--

-- Dumped from database version 15.1 (Ubuntu 15.1-1.pgdg20.04+1)
-- Dumped by pg_dump version 16.4 (Ubuntu 16.4-0ubuntu0.24.04.1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: internal; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA internal;


ALTER SCHEMA internal OWNER TO postgres;

--
-- Name: SCHEMA internal; Type: COMMENT; Schema: -; Owner: postgres
--

COMMENT ON SCHEMA internal IS 'Internal schema used for types, tables, and procedures we don''t expose in our API';


--
-- Name: public; Type: SCHEMA; Schema: -; Owner: postgres
--



ALTER SCHEMA public OWNER TO postgres;

--
-- Name: alert_type; Type: TYPE; Schema: public; Owner: postgres
--

CREATE TYPE public.alert_type AS ENUM (
    'free_trial',
    'free_trial_ending',
    'free_trial_stalled',
    'missing_payment_method',
    'data_movement_stalled',
    'data_not_processed_in_interval'
);


ALTER TYPE public.alert_type OWNER TO postgres;

--
-- Name: catalog_name; Type: DOMAIN; Schema: public; Owner: postgres
--

CREATE DOMAIN public.catalog_name AS text
	CONSTRAINT "Must be a valid catalog name" CHECK (((VALUE ~ '^([[:alpha:][:digit:]\-_.]+/)+[[:alpha:][:digit:]\-_.]+$'::text) AND ((VALUE) IS NFKC NORMALIZED)));


ALTER DOMAIN public.catalog_name OWNER TO postgres;

--
-- Name: DOMAIN catalog_name; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON DOMAIN public.catalog_name IS '
catalog_name is a name within the Flow catalog namespace.

Catalog names consist of Unicode-normalized (NFKC) letters, numbers,
"-", "_", and ".", with components separated by "/" and not ending in "/".

For example: "acmeCo/anvils" or "acmeCo/products/TnT_v4",
but not "acmeCo//anvils/" or "acmeCo/some anvils".
';


--
-- Name: alert_snapshot; Type: TYPE; Schema: public; Owner: postgres
--

CREATE TYPE public.alert_snapshot AS (
	alert_type public.alert_type,
	catalog_name public.catalog_name,
	arguments json,
	firing boolean
);


ALTER TYPE public.alert_snapshot OWNER TO postgres;

--
-- Name: catalog_prefix; Type: DOMAIN; Schema: public; Owner: postgres
--

CREATE DOMAIN public.catalog_prefix AS text
	CONSTRAINT "Must be a valid catalog prefix" CHECK (((VALUE ~ '^([[:alpha:][:digit:]\-_.]+/)+$'::text) AND ((VALUE) IS NFKC NORMALIZED)));


ALTER DOMAIN public.catalog_prefix OWNER TO postgres;

--
-- Name: DOMAIN catalog_prefix; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON DOMAIN public.catalog_prefix IS '
catalog_name is a prefix within the Flow catalog namespace.

Catalog prefixes consist of Unicode-normalized (NFKC) letters, numbers,
"-", "_", and ".", with components separated by "/" and ending in a final "/".

For example: "acmeCo/anvils/" or "acmeCo/products/TnT_v4/",
but not "acmeCo/anvils" or "acmeCo/some anvils".
';


--
-- Name: catalog_spec_type; Type: TYPE; Schema: public; Owner: postgres
--

CREATE TYPE public.catalog_spec_type AS ENUM (
    'capture',
    'collection',
    'materialization',
    'test'
);


ALTER TYPE public.catalog_spec_type OWNER TO postgres;

--
-- Name: TYPE catalog_spec_type; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON TYPE public.catalog_spec_type IS '
Enumeration of Flow catalog specification types:
"capture", "collection", "materialization", or "test"
';


--
-- Name: catalog_tenant; Type: DOMAIN; Schema: public; Owner: postgres
--

CREATE DOMAIN public.catalog_tenant AS text
	CONSTRAINT "Must be a valid catalog tenant" CHECK (((VALUE ~ '^[[:alpha:][:digit:]\-_.]+/$'::text) AND ((VALUE) IS NFKC NORMALIZED)));


ALTER DOMAIN public.catalog_tenant OWNER TO postgres;

--
-- Name: DOMAIN catalog_tenant; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON DOMAIN public.catalog_tenant IS '
catalog_tenant is a prefix within the Flow catalog namespace
having exactly one top-level path component.

Catalog tenants consist of Unicode-normalized (NFKC) letters, numbers,
"-", "_", and "." and ending in a final "/".

For example: "acmeCo/" or "acmeCo.anvils/" or "acmeCo-TNT/",
but not "acmeCo" or "acmeCo/anvils/" or "acmeCo/TNT".
';


--
-- Name: id_generator(); Type: FUNCTION; Schema: internal; Owner: postgres
--

CREATE FUNCTION internal.id_generator() RETURNS macaddr8
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
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
      return lpad(to_hex((now_millis << 23) | (seq_no << 10) | (shard_id)), 16, '0')::macaddr8;
  end;
  $$;


ALTER FUNCTION internal.id_generator() OWNER TO postgres;

--
-- Name: FUNCTION id_generator(); Type: COMMENT; Schema: internal; Owner: postgres
--

COMMENT ON FUNCTION internal.id_generator() IS '
  id_generator produces 64bit unique, non-sequential identifiers. They:
    * Have fixed storage that''s 1/2 the size of a UUID.
    * Have a monotonic generation order.
    * Embed a wall-clock timestamp than can be extracted if needed.
    * Avoid the leaky-ness of SERIAL id''s.

  Adapted from: https://rob.conery.io/2014/05/29/a-better-id-generator-for-postgresql/
  Which itself was inspired by http://instagram-engineering.tumblr.com/post/10853187575/sharding-ids-at-instagram
  ';


--
-- Name: flowid; Type: DOMAIN; Schema: public; Owner: postgres
--

CREATE DOMAIN public.flowid AS macaddr8 DEFAULT internal.id_generator();


ALTER DOMAIN public.flowid OWNER TO postgres;

--
-- Name: DOMAIN flowid; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON DOMAIN public.flowid IS '
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


--
-- Name: json_obj; Type: DOMAIN; Schema: public; Owner: postgres
--

CREATE DOMAIN public.json_obj AS json
	CONSTRAINT json_obj_check CHECK ((json_typeof(VALUE) = 'object'::text));


ALTER DOMAIN public.json_obj OWNER TO postgres;

--
-- Name: DOMAIN json_obj; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON DOMAIN public.json_obj IS 'json_obj is JSON which is restricted to the "object" type';


--
-- Name: jsonb_obj; Type: DOMAIN; Schema: public; Owner: postgres
--

CREATE DOMAIN public.jsonb_obj AS jsonb
	CONSTRAINT jsonb_obj_check CHECK ((jsonb_typeof(VALUE) = 'object'::text));


ALTER DOMAIN public.jsonb_obj OWNER TO postgres;

--
-- Name: DOMAIN jsonb_obj; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON DOMAIN public.jsonb_obj IS 'jsonb_obj is JSONB which is restricted to the "object" type';


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: applied_directives; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.applied_directives (
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    detail text,
    id public.flowid NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    job_status public.jsonb_obj DEFAULT '{"type": "queued"}'::jsonb NOT NULL,
    logs_token uuid DEFAULT gen_random_uuid() NOT NULL,
    directive_id public.flowid NOT NULL,
    user_id uuid DEFAULT auth.uid() NOT NULL,
    user_claims public.json_obj,
    background boolean DEFAULT false NOT NULL
);


ALTER TABLE public.applied_directives OWNER TO postgres;

--
-- Name: TABLE applied_directives; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON TABLE public.applied_directives IS '
Directives which are being or have been applied by the user.

Users begin to apply a directive by exchanging its bearer token, which creates
a new applied_directives row. Then, upon supplying user_claims which further
parameterize the operation, the directive is validated and applied with the
user''s claims.
';


--
-- Name: COLUMN applied_directives.created_at; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.applied_directives.created_at IS 'Time at which the record was created';


--
-- Name: COLUMN applied_directives.detail; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.applied_directives.detail IS 'Description of the record';


--
-- Name: COLUMN applied_directives.id; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.applied_directives.id IS 'ID of the record';


--
-- Name: COLUMN applied_directives.updated_at; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.applied_directives.updated_at IS 'Time at which the record was last updated';


--
-- Name: COLUMN applied_directives.job_status; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.applied_directives.job_status IS 'Server-side job executation status of the record';


--
-- Name: COLUMN applied_directives.logs_token; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.applied_directives.logs_token IS 'Bearer token for accessing logs of the server-side operation';


--
-- Name: COLUMN applied_directives.directive_id; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.applied_directives.directive_id IS 'Directive which is being applied';


--
-- Name: COLUMN applied_directives.user_id; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.applied_directives.user_id IS 'User on whose behalf the directive is being applied';


--
-- Name: COLUMN applied_directives.user_claims; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.applied_directives.user_claims IS '
User-supplied claims which parameterize the directive''s evaluation.

User claims are initially null when an applied directive is first created,
and must be updated by the user for evaluation of the directive to begin.
';


--
-- Name: COLUMN applied_directives.background; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.applied_directives.background IS 'indicates whether this is a background job, which will be processed with a lower priority than interactive jobs';


--
-- Name: directives; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.directives (
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    detail text,
    id public.flowid NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    catalog_prefix public.catalog_prefix NOT NULL,
    spec public.jsonb_obj NOT NULL,
    token uuid DEFAULT gen_random_uuid(),
    uses_remaining bigint,
    CONSTRAINT "spec must have a string property `type`" CHECK ((NOT (jsonb_typeof(((spec)::jsonb -> 'type'::text)) IS DISTINCT FROM 'string'::text)))
);


ALTER TABLE public.directives OWNER TO postgres;

--
-- Name: TABLE directives; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON TABLE public.directives IS '
Directives are scoped operations that users may elect to apply.
For example, a directive might grant access to a specific catalog namespace,
or provision the setup of a new organization.

In general these operations require administrative priviledge that the user
does not directly have. The directive mechanism thus enables a user to have a
priviledged operation be applied on their behalf in a self-service fashion.

The types of operations supported by directives are open ended,
but each generally has a well-defined (but parameterizable) scope,
and may also be subject to additional server-side verification checks.

To apply a given directive a user must know its current token, which is
a secret credential that''s typically exchanged through another channel
(such as Slack, or email). The user then creates a corresponding entry in
applied_directives with accompanying user claims.
';


--
-- Name: COLUMN directives.created_at; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.directives.created_at IS 'Time at which the record was created';


--
-- Name: COLUMN directives.detail; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.directives.detail IS 'Description of the record';


--
-- Name: COLUMN directives.id; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.directives.id IS 'ID of the record';


--
-- Name: COLUMN directives.updated_at; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.directives.updated_at IS 'Time at which the record was last updated';


--
-- Name: COLUMN directives.catalog_prefix; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.directives.catalog_prefix IS '
Catalog prefix which contains the directive.

Operations undertaken by a directive are scoped within the catalog prefix,
and a user must admin the named prefix in order to admin its directives.
';


--
-- Name: COLUMN directives.spec; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.directives.spec IS '
Specification of the directive.

Specification documents must have a string `type` property which defines
the directive type. This type defines the meaning of the remainder of the
specification document.
';


--
-- Name: COLUMN directives.token; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.directives.token IS '
Bearer token which is presented by a user to access and apply a directive.
';


--
-- Name: COLUMN directives.uses_remaining; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.directives.uses_remaining IS '
The maximum number of times that this directive may be applied.
This value gets decremented each time the directive is applied.
Once it reaches 0, future attempts to apply the directive will fail.
A null here means that there is no limit.
';


--
-- Name: exchanged_directive; Type: TYPE; Schema: public; Owner: postgres
--

CREATE TYPE public.exchanged_directive AS (
	directive public.directives,
	applied_directive public.applied_directives
);


ALTER TYPE public.exchanged_directive OWNER TO postgres;

--
-- Name: flow_type; Type: TYPE; Schema: public; Owner: postgres
--

CREATE TYPE public.flow_type AS ENUM (
    'capture',
    'collection',
    'materialization',
    'test',
    'source_capture'
);


ALTER TYPE public.flow_type OWNER TO postgres;

--
-- Name: TYPE flow_type; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON TYPE public.flow_type IS 'Represents the type of a dependency of one spec on another. This enum is a
  strict superset of catalog_spec_type, for historical reasons.';


--
-- Name: grant_capability; Type: TYPE; Schema: public; Owner: postgres
--

CREATE TYPE public.grant_capability AS ENUM (
    'x_00',
    'x_01',
    'x_02',
    'x_03',
    'x_04',
    'x_05',
    'x_06',
    'x_07',
    'x_08',
    'x_09',
    'read',
    'x_11',
    'x_12',
    'x_13',
    'x_14',
    'x_15',
    'x_16',
    'x_17',
    'x_18',
    'x_19',
    'write',
    'x_21',
    'x_22',
    'x_23',
    'x_24',
    'x_25',
    'x_26',
    'x_27',
    'x_28',
    'x_29',
    'admin'
);


ALTER TYPE public.grant_capability OWNER TO postgres;

--
-- Name: TYPE grant_capability; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON TYPE public.grant_capability IS '
grant_capability is an ordered enumeration of grant capabilities
bestowed upon a grantee by a grantor. Higher enumerated values
imply all of the capabilities of lower enum values.

Enum values beginning with "x_" are placeholders for possible
future extension of the set of granted capabilities.

A "read" capability allows a user or catalog specifications to
read from collections.

A "write" capability allows a user or catalog specification to
write data into collections.

The "admin" capability allows for creating, updating, and deleting
specifications. Unlike "read" or "write", this capability also recursively
grants the bearer all capabilities of the object_role. Put differently,
a user capable of changing a catalog specification is also granted the
capabilities which that specification itself uses to read and write data.
';


--
-- Name: json_pointer; Type: DOMAIN; Schema: public; Owner: postgres
--

CREATE DOMAIN public.json_pointer AS text
	CONSTRAINT json_pointer_check CHECK (((VALUE = ''::text) OR ((VALUE ^@ '/'::text) AND (length(VALUE) > 1))));


ALTER DOMAIN public.json_pointer OWNER TO postgres;

--
-- Name: jsonb_internationalized_value; Type: DOMAIN; Schema: public; Owner: postgres
--

CREATE DOMAIN public.jsonb_internationalized_value AS jsonb
	CONSTRAINT jsonb_internationalized_value_check CHECK (((VALUE IS NULL) OR ((jsonb_typeof(VALUE) = 'object'::text) AND ((VALUE -> 'en-US'::text) IS NOT NULL))));


ALTER DOMAIN public.jsonb_internationalized_value OWNER TO postgres;

--
-- Name: DOMAIN jsonb_internationalized_value; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON DOMAIN public.jsonb_internationalized_value IS 'jsonb_internationalized_value is JSONB object which is required to at least have en-US internationalized values';


--
-- Name: payment_provider_type; Type: TYPE; Schema: public; Owner: postgres
--

CREATE TYPE public.payment_provider_type AS ENUM (
    'stripe',
    'external'
);


ALTER TYPE public.payment_provider_type OWNER TO postgres;

--
-- Name: TYPE payment_provider_type; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON TYPE public.payment_provider_type IS '
Enumeration of which payment provider this tenant is using.
';


--
-- Name: user_profile; Type: TYPE; Schema: public; Owner: postgres
--

CREATE TYPE public.user_profile AS (
	user_id uuid,
	email text,
	full_name text,
	avatar_url text
);


ALTER TYPE public.user_profile OWNER TO postgres;

--
-- Name: access_token_jwt_secret(); Type: FUNCTION; Schema: internal; Owner: postgres
--

CREATE FUNCTION internal.access_token_jwt_secret() RETURNS text
    LANGUAGE sql STABLE
    AS $$

  select coalesce(current_setting('app.settings.jwt_secret', true), 'super-secret-jwt-token-with-at-least-32-characters-long') limit 1

$$;


ALTER FUNCTION internal.access_token_jwt_secret() OWNER TO postgres;

--
-- Name: billing_report_202308(public.catalog_prefix, timestamp with time zone); Type: FUNCTION; Schema: internal; Owner: postgres
--

CREATE FUNCTION internal.billing_report_202308(billed_prefix public.catalog_prefix, billed_month timestamp with time zone) RETURNS jsonb
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare
  -- Output variables.
  o_daily_usage       jsonb;
  o_data_gb           numeric;
  o_line_items        jsonb = '[]';
  o_recurring_fee     integer;
  o_subtotal          integer;
  o_task_hours        numeric;
  o_trial_credit      integer;
  o_free_tier_credit  integer;
  o_trial_start       date;
  o_trial_range       daterange;
  o_free_tier_range   daterange;
  o_billed_range      daterange;
begin

  -- Ensure `billed_month` is the truncated start of the billed month.
  billed_month = date_trunc('month', billed_month);

  with vars as (
    select
      t.data_tiers,
      t.trial_start,
      t.usage_tiers,
      tstzrange(billed_month, billed_month  + '1 month', '[)') as billed_range,
      case when t.trial_start is not null
        then daterange(t.trial_start::date, ((t.trial_start::date) + interval '1 month')::date, '[)')
        else 'empty' end as trial_range,
      -- In order to smoothly transition between free tier credit and free trial credit,
      -- the free tier covers all usage up to, but _not including_ the trial start date.
      -- On the trial start date, the free trial credit takes over.
      daterange(NULL, t.trial_start::date, '[)') as free_tier_range,
      -- Reveal contract costs only when computing whole-tenant billing.
      case when t.tenant = billed_prefix then t.recurring_usd_cents else 0 end as recurring_fee
      from tenants t
      where billed_prefix ^@ t.tenant -- Prefix starts with tenant.
  ),
  -- Roll up each day's incremental usage.
  daily_stat_deltas as (
    select
      ts,
      sum(bytes_written_by_me + bytes_read_by_me) / (10.0^9.0) as data_gb,
      sum(usage_seconds) / (60.0 * 60) as task_hours
    from catalog_stats, vars
      where catalog_name ^@ billed_prefix -- Name starts with prefix.
      and grain = 'daily'
      and billed_range @> ts
      group by ts
  ),
  -- Map to cumulative daily usage.
  -- Note sum(...) over (order by ts) yields the running sum of its aggregate.
  daily_stats as (
    select
      ts,
      sum(data_gb) over w as data_gb,
      sum(task_hours) over w as task_hours
    from daily_stat_deltas
    window w as (order by ts)
  ),
  -- Extend with line items for each category for the period ending with the given day.
  daily_line_items as (
    select
      daily_stats.*,
      internal.tier_line_items(ceil(data_gb)::integer, data_tiers, 'Data processing', 'GB') as data_line_items,
      internal.tier_line_items(ceil(task_hours)::integer, usage_tiers, 'Task usage', 'hour') as task_line_items
    from daily_stats, vars
  ),
  -- Extend with per-category subtotals for the period ending with the given day.
  daily_totals as (
    select
      daily_line_items.*,
      data_subtotal,
      task_subtotal
    from daily_line_items,
      lateral (select sum((li->>'subtotal')::numeric) as data_subtotal from jsonb_array_elements(data_line_items) li) l1,
      lateral (select sum((li->>'subtotal')::numeric) as task_subtotal from jsonb_array_elements(task_line_items) li) l2
  ),
  -- Map cumulative totals to per-day deltas.
  daily_deltas as (
    select
      ts,
      data_gb       - (coalesce(lag(data_gb,         1) over w, 0)) as data_gb,
      data_subtotal - (coalesce(lag(data_subtotal,   1) over w, 0)) as data_subtotal,
      task_hours    - (coalesce(lag(task_hours,      1) over w, 0)) as task_hours,
      task_subtotal - (coalesce(lag(task_subtotal,   1) over w, 0)) as task_subtotal
      from daily_totals
      window w as (order by ts)
  ),
  -- 1) Group daily_deltas into a JSON array
  -- 2) Sum a trial credit from daily deltas that overlap with the trial period.
  daily_array_and_trial_credits as (
    select
    jsonb_agg(jsonb_build_object(
      'ts', ts,
      'data_gb', data_gb,
      'data_subtotal', data_subtotal,
      'task_hours', task_hours,
      'task_subtotal', task_subtotal
    )) as daily_usage,
    coalesce(sum(data_subtotal + task_subtotal) filter (where trial_range @> (ts::date)),0 ) as trial_credit,
    coalesce(sum(data_subtotal + task_subtotal) filter (where free_tier_range @> (ts::date)),0 ) as free_tier_credit
    from daily_deltas, vars
  ),
  -- The last day captures the cumulative billed period.
  last_day as (
    select * from daily_line_items
    order by ts desc limit 1
  ),
  -- If we're reporting for the whole tenant then gather billing adjustment line-items.
  adjustments as (
    select coalesce(jsonb_agg(
      jsonb_build_object(
        'description', detail,
        'count', 1,
        'rate', usd_cents,
        'subtotal', usd_cents
      )
    ), '[]') as adjustment_line_items
    from internal.billing_adjustments a
    where a.tenant = billed_prefix and a.billed_month = billing_report_202308.billed_month
  )
  select into
    -- Block of variables being selected into.
    o_daily_usage,
    o_data_gb,
    o_line_items,
    o_recurring_fee,
    o_task_hours,
    o_trial_credit,
    o_trial_start,
    o_trial_range,
    o_billed_range,
    o_free_tier_credit,
    o_free_tier_range
    -- The actual selected columns.
    daily_usage,
    data_gb,
    data_line_items || task_line_items || adjustment_line_items,
    recurring_fee,
    task_hours,
    trial_credit,
    trial_start,
    trial_range,
    billed_range,
    free_tier_credit,
    free_tier_range
  from daily_array_and_trial_credits, last_day, adjustments, vars;

  -- Add line items for recurring service fee & free trial credit.
  if o_recurring_fee != 0 then
    o_line_items = jsonb_build_object(
      'description', 'Recurring service charge',
      'count', 1,
      'rate', o_recurring_fee,
      'subtotal', o_recurring_fee
    ) || o_line_items;
  end if;

  -- Display a (possibly zero) free trial credit if the trial range overlaps the billed range
  if o_trial_range && o_billed_range then
    o_line_items = o_line_items || jsonb_build_object(
      'description', format('Free trial credit (%s - %s)', lower(o_trial_range), (upper(o_trial_range) - interval '1 day')::date),
      'count', 1,
      'rate', -o_trial_credit,
      'subtotal', -o_trial_credit
    );
  end if;

  -- Display the free tier credit if the free tier range overlaps the billed range
  if o_free_tier_range && o_billed_range then
    o_line_items = o_line_items || jsonb_build_object(
      'description', case when upper(o_free_tier_range) is not null
        then format('Free tier credit ending %s', (upper(o_free_tier_range) - interval '1 day')::date)
        else 'Free tier credit'
      end,
      'count', 1,
      'rate', -o_free_tier_credit,
      'subtotal', -o_free_tier_credit
    );
  end if;

  -- Roll up the final subtotal.
  select into o_subtotal sum((l->>'subtotal')::numeric)
    from jsonb_array_elements(o_line_items) l;

  return jsonb_build_object(
    'billed_month', billed_month,
    'billed_prefix', billed_prefix,
    'daily_usage', o_daily_usage,
    'line_items', o_line_items,
    'processed_data_gb', o_data_gb,
    'recurring_fee', o_recurring_fee,
    'subtotal', o_subtotal,
    'task_usage_hours', o_task_hours,
    'trial_credit', coalesce(o_trial_credit, 0),
    'free_tier_credit', coalesce(o_free_tier_credit, 0),
    'trial_start', o_trial_start
  );

end
$$;


ALTER FUNCTION internal.billing_report_202308(billed_prefix public.catalog_prefix, billed_month timestamp with time zone) OWNER TO postgres;

--
-- Name: compute_incremental_line_items(text, text, numeric, integer[], numeric); Type: FUNCTION; Schema: internal; Owner: postgres
--

CREATE FUNCTION internal.compute_incremental_line_items(item_name text, item_unit text, single_usage numeric, tiers integer[], running_usage_sum numeric) RETURNS jsonb
    LANGUAGE plpgsql
    AS $$
declare
  line_items jsonb = '[]';

  -- Calculating tiered usage.
  tier_rate    integer;
  tier_pivot   integer;
  tier_count   numeric;
begin
  -- Walk up the tiers
  for tier_idx in 1..array_length(tiers,1) by 2 loop
    tier_rate = tiers[tier_idx];
    tier_pivot = tiers[tier_idx+1];
    if tier_pivot is null then
      -- No limits here, roll all of the remaining usage into this tier
      tier_count = single_usage;
      running_usage_sum = running_usage_sum + tier_count;
      if tier_count > 0 then
        line_items = line_items || jsonb_build_object(
          'description', format(
            '%s (at %s/%s)',
            item_name,
            (tier_rate / 100.0)::money,
            item_unit
          ),
          'count', tier_count,
          'rate', tier_rate,
          'subtotal_frac', tier_count * tier_rate
        );
      end if;
    elsif tier_pivot > running_usage_sum then
      -- We haven't already surpassed this tier's pivot
      -- Calculate how much more usage we'd need to surpass this tier
      tier_count = least(single_usage, tier_pivot - running_usage_sum);
      single_usage = single_usage - tier_count;
      running_usage_sum = running_usage_sum + tier_count;
      if tier_count > 0 then
        line_items = line_items || jsonb_build_object(
          'description', format(
            case
              when tier_idx = 1 then '%s (first %s%ss at %s/%s)'
              else '%s (next %s%ss at %s/%s)'
            end,
            item_name,
            tier_pivot,
            item_unit,
            (tier_rate / 100.0)::money,
            item_unit
          ),
          'count', tier_count,
          'rate', tier_rate,
          'subtotal_frac', tier_count * tier_rate
        );
      end if;
    end if;
  end loop;

  return jsonb_build_object(
    'line_items', line_items,
    'running_usage_sum', running_usage_sum
  );
end
$$;


ALTER FUNCTION internal.compute_incremental_line_items(item_name text, item_unit text, single_usage numeric, tiers integer[], running_usage_sum numeric) OWNER TO postgres;

--
-- Name: create_auto_discovers(); Type: FUNCTION; Schema: internal; Owner: postgres
--

CREATE FUNCTION internal.create_auto_discovers() RETURNS integer
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare
  support_user_id uuid = (select id from auth.users where email = 'support@estuary.dev');
  next_row internal.next_auto_discovers;
  total_created integer := 0;
  tmp_draft_id flowid;
  tmp_discover_id flowid;
begin

for next_row in select * from internal.next_auto_discovers
loop
  -- Create a draft, which we'll discover into
  insert into drafts (user_id) values (support_user_id) returning id into tmp_draft_id;

  insert into discovers (capture_name, draft_id, connector_tag_id, endpoint_config, update_only, auto_publish, auto_evolve, background)
  values (
    next_row.capture_name,
    tmp_draft_id,
    next_row.connector_tags_id,
    next_row.endpoint_json,
    not next_row.add_new_bindings,
    true,
    next_row.evolve_incompatible_collections,
    true
  ) returning id into tmp_discover_id;

  -- This is just useful when invoking the function manually.
  total_created := total_created + 1;
end loop;

return total_created;
end;
$$;


ALTER FUNCTION internal.create_auto_discovers() OWNER TO postgres;

--
-- Name: FUNCTION create_auto_discovers(); Type: COMMENT; Schema: internal; Owner: postgres
--

COMMENT ON FUNCTION internal.create_auto_discovers() IS 'Creates discovers jobs for each capture that is due for an automatic discover. Each disocver will have auto_publish
set to true. The update_only and auto_evolve columns of the discover will be set based on the addNewBindings and
evolveIncompatibleCollections fields in the capture spec. This function is idempotent. Once a discover is created by
this function, the next_auto_discovers view will no longer include that capture until its interval has passed again.
So its safe to call this function at basically any frequency. The return value of the function is the count of newly
created discovers jobs.';


--
-- Name: delete_old_cron_runs(); Type: FUNCTION; Schema: internal; Owner: postgres
--

CREATE FUNCTION internal.delete_old_cron_runs() RETURNS integer
    LANGUAGE sql SECURITY DEFINER
    AS $$
    with r as (
        delete from cron.job_run_details where end_time < now() - '10 days'::interval returning runid
    )
    select count(*) from r;
$$;


ALTER FUNCTION internal.delete_old_cron_runs() OWNER TO postgres;

--
-- Name: FUNCTION delete_old_cron_runs(); Type: COMMENT; Schema: internal; Owner: postgres
--

COMMENT ON FUNCTION internal.delete_old_cron_runs() IS 'deletes cron.job_run_details rows that have aged out.';


--
-- Name: delete_old_drafts(); Type: FUNCTION; Schema: internal; Owner: postgres
--

CREATE FUNCTION internal.delete_old_drafts() RETURNS integer
    LANGUAGE sql SECURITY DEFINER
    AS $$
    with d as (
        delete from public.drafts where updated_at < (now() - '10 days'::interval) returning id
    )
    select count(id) from d;
$$;


ALTER FUNCTION internal.delete_old_drafts() OWNER TO postgres;

--
-- Name: FUNCTION delete_old_drafts(); Type: COMMENT; Schema: internal; Owner: postgres
--

COMMENT ON FUNCTION internal.delete_old_drafts() IS 'deletes drafts, discovers, draft_specs, and draft_errors rows that have aged out';


--
-- Name: delete_old_hourly_stats(); Type: FUNCTION; Schema: internal; Owner: postgres
--

CREATE FUNCTION internal.delete_old_hourly_stats() RETURNS integer
    LANGUAGE sql SECURITY DEFINER
    AS $$
    with s as (
        delete from catalog_stats_hourly where grain = 'hourly' and ts < (now() - '30 days'::interval) returning ts
    )
    select count(ts) from s;
$$;


ALTER FUNCTION internal.delete_old_hourly_stats() OWNER TO postgres;

--
-- Name: FUNCTION delete_old_hourly_stats(); Type: COMMENT; Schema: internal; Owner: postgres
--

COMMENT ON FUNCTION internal.delete_old_hourly_stats() IS 'deletes catalog_stats_hourly rows that have aged out';


--
-- Name: delete_old_log_lines(); Type: FUNCTION; Schema: internal; Owner: postgres
--

CREATE FUNCTION internal.delete_old_log_lines() RETURNS integer
    LANGUAGE sql SECURITY DEFINER
    AS $$
    with l as (
        delete from internal.log_lines where logged_at < (now() - '2 days'::interval) returning logged_at
    )
    select count(*) from l;
$$;


ALTER FUNCTION internal.delete_old_log_lines() OWNER TO postgres;

--
-- Name: FUNCTION delete_old_log_lines(); Type: COMMENT; Schema: internal; Owner: postgres
--

COMMENT ON FUNCTION internal.delete_old_log_lines() IS 'deletes internal.log_lines rows that have aged out';


--
-- Name: evaluate_alert_events(); Type: FUNCTION; Schema: internal; Owner: postgres
--

CREATE FUNCTION internal.evaluate_alert_events() RETURNS void
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
begin

  -- Create alerts which have transitioned from !firing => firing
  with open_alerts as (
    select alert_type, catalog_name from alert_history
    where resolved_at is null
  )
  insert into alert_history (alert_type, catalog_name, fired_at, arguments)
    select alert_all.alert_type, alert_all.catalog_name, now(), alert_all.arguments
    from alert_all
    left join open_alerts on
      alert_all.alert_type = open_alerts.alert_type and
      alert_all.catalog_name = open_alerts.catalog_name
    where alert_all.firing and open_alerts is null;

  -- Resolve alerts that have transitioned from firing => !firing
  with open_alerts as (
    select
      alert_history.alert_type,
      alert_history.catalog_name,
      fired_at
    from alert_history
    where resolved_at is null
  ),
  -- Find all open_alerts for which either there is not a row in alerts_all,
  -- or there is but its firing field is false.
  closing_alerts as (
    select
      open_alerts.alert_type,
      open_alerts.catalog_name,
      fired_at,
      coalesce(alert_all.arguments, null) as arguments
    from open_alerts
    left join alert_all on
      alert_all.alert_type = open_alerts.alert_type and
      alert_all.catalog_name = open_alerts.catalog_name
    where
      -- The open alert is no longer in alert_all, therefore it's no longer firing
      alert_all.alert_type is null or
      -- The open is still tracked, but it has stopped firing
      not alert_all.firing
  )
  update alert_history
    set resolved_at = now(),
        resolved_arguments = closing_alerts.arguments
    from closing_alerts
    where alert_history.alert_type = closing_alerts.alert_type
      and alert_history.catalog_name = closing_alerts.catalog_name
      and alert_history.fired_at = closing_alerts.fired_at;

end;
$$;


ALTER FUNCTION internal.evaluate_alert_events() OWNER TO postgres;

--
-- Name: freeze_billing_month(timestamp with time zone); Type: FUNCTION; Schema: internal; Owner: postgres
--

CREATE FUNCTION internal.freeze_billing_month(billed_month timestamp with time zone) RETURNS integer
    LANGUAGE plpgsql
    AS $$
declare
    tenant_row record;
    tenant_count integer = 0;
begin
    for tenant_row in select tenant as tenant_name from tenants loop
        insert into internal.billing_historicals
        select
            report->>'billed_prefix' as tenant,
            (report->>'billed_month')::timestamptz as billed_month,
            report
        from internal.billing_report_202308(tenant_row.tenant_name, billed_month) as report
        on conflict do nothing;

        -- INSERT statements set FOUND true if at least one row is affected, false if no row is affected.
        if found then
          tenant_count = tenant_count + 1;
        end if;
    end loop;
    return tenant_count;
end
$$;


ALTER FUNCTION internal.freeze_billing_month(billed_month timestamp with time zone) OWNER TO postgres;

--
-- Name: gateway_endpoint_url(); Type: FUNCTION; Schema: internal; Owner: postgres
--

CREATE FUNCTION internal.gateway_endpoint_url() RETURNS text
    LANGUAGE sql STABLE SECURITY DEFINER
    AS $$

  select url
  from internal.gateway_endpoints
  limit 1

$$;


ALTER FUNCTION internal.gateway_endpoint_url() OWNER TO postgres;

--
-- Name: incremental_usage_report(text, public.catalog_prefix, tstzrange); Type: FUNCTION; Schema: internal; Owner: postgres
--

CREATE FUNCTION internal.incremental_usage_report(requested_grain text, billed_prefix public.catalog_prefix, billed_range tstzrange) RETURNS jsonb
    LANGUAGE plpgsql
    AS $$
declare
  -- Retrieved from tenants table.
  data_tiers  integer[];
  usage_tiers integer[];

  granules jsonb = '[]';
  returned_data_line_items jsonb = '{}';
  returned_hours_line_items jsonb = '{}';
  combined_line_items jsonb;

  subtotal_frac numeric;

  running_gb_sum numeric = 0;
  running_hour_sum numeric = 0;
  line_items jsonb = '[]';
begin
  -- Fetch data & usage tiers for `billed_prefix`'s tenant.
  select into data_tiers, usage_tiers
    t.data_tiers,
    t.usage_tiers
  from tenants t
  where billed_prefix ^@ t.tenant;

  -- Get all stats records for the selected time period at the selected granularity
  select into granules
    (select json_agg(res.obj) from (
        select jsonb_build_object(
          'processed_data_gb', sum((bytes_written_by_me + bytes_read_by_me)) / (1024.0 * 1024 * 1024),
          'task_usage_hours', sum(usage_seconds) / (60.0 * 60),
          'ts', ts
        ) as obj
        from catalog_stats
        where catalog_name ^@ billed_prefix
        and grain = requested_grain
        and billed_range @> ts
        group by ts
    ) as res)
  ;

  for idx in 0..jsonb_array_length(granules)-1 loop
    returned_data_line_items = internal.compute_incremental_line_items('Data processing', 'GB', (granules->idx->'processed_data_gb')::numeric, data_tiers, running_gb_sum);
    running_gb_sum = (returned_data_line_items->'running_usage_sum')::numeric;

    returned_hours_line_items = internal.compute_incremental_line_items('Task usage', 'hour', (granules->idx->'task_usage_hours')::numeric, usage_tiers, running_hour_sum);
    running_hour_sum = (returned_hours_line_items->'running_usage_sum')::numeric;

    combined_line_items = (returned_data_line_items->'line_items')::jsonb || (returned_hours_line_items->'line_items')::jsonb;

    select into subtotal_frac sum((item->'subtotal_frac')::numeric) from jsonb_array_elements(combined_line_items) as item;

    line_items = line_items || jsonb_build_object(
      'line_items', combined_line_items,
      'subtotal_frac', subtotal_frac,
      'processed_data_gb', (granules->idx->'processed_data_gb')::numeric,
      'task_usage_hours', (granules->idx->'task_usage_hours')::numeric,
      'ts', granules->idx->'ts'
    );
  end loop;

  return line_items;
end
$$;


ALTER FUNCTION internal.incremental_usage_report(requested_grain text, billed_prefix public.catalog_prefix, billed_range tstzrange) OWNER TO postgres;

--
-- Name: jsonb_merge_diff(jsonb, jsonb); Type: FUNCTION; Schema: internal; Owner: postgres
--

CREATE FUNCTION internal.jsonb_merge_diff(target jsonb, source jsonb) RETURNS jsonb
    LANGUAGE plpgsql IMMUTABLE
    AS $$
begin
  case
    when "target" isnull then
      return 'null'; -- Marker to remove location.
    when jsonb_typeof("target") is distinct from 'object' or
         jsonb_typeof("source") is distinct from 'object' then
      return (case
        when "target" = "source" then null
        else jsonb_strip_nulls("target")
      end);
    else
      return (
        with props as (
          select
            coalesce("tkey", "skey")                  as "key",
            internal.jsonb_merge_diff("tval", "sval") as "val"
          from            jsonb_each("target") e1("tkey", "tval")
          full outer join jsonb_each("source") e2("skey", "sval") on "tkey" = "skey"
        )
        -- If no props are different, the result is NULL (not 'null').
        select jsonb_object_agg("key", "val")
        from props
        where "val" is not null
      );
  end case;
end;
$$;


ALTER FUNCTION internal.jsonb_merge_diff(target jsonb, source jsonb) OWNER TO postgres;

--
-- Name: jsonb_merge_patch(jsonb, jsonb); Type: FUNCTION; Schema: internal; Owner: postgres
--

CREATE FUNCTION internal.jsonb_merge_patch(target jsonb, patch jsonb) RETURNS jsonb
    LANGUAGE plpgsql IMMUTABLE
    AS $$
begin
  case
    when "patch" is null then
      return "target";
    when "patch" = 'null' then
      return null; -- Remove location.
    when jsonb_typeof("target") is distinct from 'object' or
         jsonb_typeof("patch")  is distinct from 'object' then
      -- If either side is not an object, take the patch.
      return jsonb_strip_nulls("patch");
    when "target" = jsonb_strip_nulls("patch") then
      -- Both are objects, and the patch doesn't change the target.
      -- This case *could* be handled by the recursive case,
      -- but equality and stripping nulls is dirt cheap compared to
      -- the cost of recursive jsonb_object_agg, which must repeatedly
      -- copy nested sub-structure.
      return "target";
    else
      return (
        with props as (
          select
            coalesce("tkey", "pkey") as "key",
            case
                when "pval" isnull then "tval"
                else internal.jsonb_merge_patch("tval", "pval")
            end as "val"
          from            jsonb_each("target") e1("tkey", "tval")
          full outer join jsonb_each("patch")  e2("pkey", "pval") on "tkey" = "pkey"
          where "pval" is distinct from 'null'
        )
        select coalesce(jsonb_object_agg("key", "val"), '{}') from props
      );
  end case;
end;
$$;


ALTER FUNCTION internal.jsonb_merge_patch(target jsonb, patch jsonb) OWNER TO postgres;

--
-- Name: notify_agent(); Type: FUNCTION; Schema: internal; Owner: postgres
--

CREATE FUNCTION internal.notify_agent() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
declare
  payload text;
begin
  -- Build the payload
  payload := json_build_object('timestamp',CURRENT_TIMESTAMP,'table',TG_TABLE_NAME);

  -- Notify the channel
  perform pg_notify('agent_notifications', payload);

  return null;
END;
$$;


ALTER FUNCTION internal.notify_agent() OWNER TO postgres;

--
-- Name: on_applied_directives_delete(); Type: FUNCTION; Schema: internal; Owner: postgres
--

CREATE FUNCTION internal.on_applied_directives_delete() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
begin
  if OLD.job_status->>'type' = 'success' then
    raise 'Cannot delete an applied directive which has completed'
      using errcode = 'check_violation';
  end if;

  return OLD;
end
$$;


ALTER FUNCTION internal.on_applied_directives_delete() OWNER TO postgres;

--
-- Name: on_applied_directives_update(); Type: FUNCTION; Schema: internal; Owner: postgres
--

CREATE FUNCTION internal.on_applied_directives_update() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
begin
  if OLD.job_status->>'type' = 'success' then
    raise 'Cannot modify an applied directive which has completed'
      using errcode = 'check_violation';
  end if;

  -- Clear a prior failed application, allowing the user to retry.
  if OLD.user_claims::text is distinct from NEW.user_claims::text then
    NEW.job_status = '{"type":"queued"}';
  end if;

  return NEW;
end
$$;


ALTER FUNCTION internal.on_applied_directives_update() OWNER TO postgres;

--
-- Name: on_inferred_schema_update(); Type: FUNCTION; Schema: internal; Owner: postgres
--

CREATE FUNCTION internal.on_inferred_schema_update() RETURNS trigger
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
begin

-- The least function is necessary in order to avoid delaying a controller job in scenarios
-- where there is a backlog of controller runs that are due.
update live_specs set controller_next_run = least(controller_next_run, now())
where catalog_name = new.collection_name and spec_type = 'collection';

return null;
end;
$$;


ALTER FUNCTION internal.on_inferred_schema_update() OWNER TO postgres;

--
-- Name: FUNCTION on_inferred_schema_update(); Type: COMMENT; Schema: internal; Owner: postgres
--

COMMENT ON FUNCTION internal.on_inferred_schema_update() IS 'Schedules a run of the controller in response to an inferred_schemas change.';


--
-- Name: send_alerts(); Type: FUNCTION; Schema: internal; Owner: postgres
--

CREATE FUNCTION internal.send_alerts() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
declare
  token text;
begin
  select decrypted_secret into token from vault.decrypted_secrets where name = 'alert-email-fn-shared-secret' limit 1;
    perform
      net.http_post(
        -- 'http://host.docker.internal:5431/functions/v1/alerts',
        'https://eyrcnmuzzyriypdajwdk.supabase.co/functions/v1/alerts',
        to_jsonb(new.*),
        headers:=format('{"Content-Type": "application/json", "Authorization": "Basic %s"}', token)::jsonb,
        timeout_milliseconds:=90000
      );
  return null;
end;
$$;


ALTER FUNCTION internal.send_alerts() OWNER TO postgres;

--
-- Name: set_new_free_trials(); Type: FUNCTION; Schema: internal; Owner: postgres
--

CREATE FUNCTION internal.set_new_free_trials() RETURNS integer
    LANGUAGE plpgsql
    AS $$
declare
    tenant_row record;
    update_count integer = 0;
begin
    for tenant_row in select tenant from internal.new_free_trial_tenants loop
      update tenants set trial_start = date_trunc('day', now())
      where tenants.tenant = tenant_row.tenant;

      -- INSERT statements set FOUND true if at least one row is affected, false if no row is affected.
      if found then
        update_count = update_count + 1;
      end if;
    end loop;
    return update_count;
end
$$;


ALTER FUNCTION internal.set_new_free_trials() OWNER TO postgres;

--
-- Name: sign_jwt(json); Type: FUNCTION; Schema: internal; Owner: postgres
--

CREATE FUNCTION internal.sign_jwt(obj json) RETURNS text
    LANGUAGE sql STABLE SECURITY DEFINER
    AS $$

  select sign(obj, secret_key::text)
  from internal.gateway_auth_keys
  limit 1

$$;


ALTER FUNCTION internal.sign_jwt(obj json) OWNER TO postgres;

--
-- Name: task_roles(text, public.grant_capability); Type: FUNCTION; Schema: internal; Owner: postgres
--

CREATE FUNCTION internal.task_roles(task_name_or_prefix text, min_capability public.grant_capability DEFAULT 'x_00'::public.grant_capability) RETURNS TABLE(role_prefix public.catalog_prefix, capability public.grant_capability)
    LANGUAGE sql STABLE
    AS $$

  with recursive
  all_roles(role_prefix, capability) as (
      select g.object_role, g.capability from role_grants g
      where starts_with(task_name_or_prefix, g.subject_role)
        and g.capability >= min_capability
    union
      -- Recursive case: for each object_role granted as 'admin',
      -- project through grants where object_role acts as the subject_role.
      select g.object_role, g.capability
      from role_grants g, all_roles a
      where starts_with(a.role_prefix, g.subject_role)
        and g.capability >= min_capability
        and a.capability = 'admin'
  )
  select role_prefix, max(capability) from all_roles
  group by role_prefix
  order by role_prefix;

$$;


ALTER FUNCTION internal.task_roles(task_name_or_prefix text, min_capability public.grant_capability) OWNER TO postgres;

--
-- Name: test_billing_report(public.catalog_prefix, timestamp with time zone); Type: FUNCTION; Schema: internal; Owner: postgres
--

CREATE FUNCTION internal.test_billing_report(billed_prefix public.catalog_prefix, billed_month timestamp with time zone) RETURNS jsonb
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
#variable_conflict use_variable
declare
  -- Auth checks
  has_admin_grant boolean;
  has_bypassrls boolean;

  -- Computed
  recurring_usd_cents integer;
  free_trial_range tstzrange;
  billed_range tstzrange;
  free_trial_overlap tstzrange;

  free_trial_credit numeric;

  -- Temporary line items holders for free trial calculations
  task_usage_line_items jsonb = '[]';
  data_usage_line_items jsonb = '[]';

  -- Calculating adjustments.
  adjustment   internal.billing_adjustments;

  -- Aggregated outputs.
  line_items jsonb = '[]';
  subtotal_usd_cents integer;
  processed_data_gb numeric;
  task_usage_hours numeric;

  -- Free trial outputs
  free_trial_gb numeric;
  free_trial_hours numeric;
begin

  -- Ensure `billed_month` is the truncated start of the billed month.
  billed_month = date_trunc('month', billed_month);
  billed_range = tstzrange(billed_month, billed_month + '1 month', '[)');

  -- Verify that the user has an admin grant for the requested `billed_prefix`.
  perform 1 from auth_roles('admin') as r where billed_prefix ^@ r.role_prefix;
  has_admin_grant = found;

  -- Check whether user has bypassrls flag
  perform 1 from pg_roles where rolname = session_user and rolbypassrls = true;
  has_bypassrls = found;

  if not has_bypassrls and not has_admin_grant then
    -- errcode 28000 causes PostgREST to return an HTTP 403
    -- see: https://www.postgresql.org/docs/current/errcodes-appendix.html
    -- and: https://postgrest.org/en/stable/errors.html#status-codes
    raise 'You are not authorized for the billed prefix %', billed_prefix using errcode = 28000;
  end if;

  -- Fetch data & usage tiers for `billed_prefix`'s tenant.
  select into free_trial_range
      case
      	when t.free_trial_start is null then 'empty'::tstzrange
        -- Inclusive start, exclusive end
       	else tstzrange(date_trunc('day', t.free_trial_start), date_trunc('day', t.free_trial_start) + '1 month', '[)')
      end
    from tenants t
    where billed_prefix ^@ t.tenant
  ;
  -- Reveal contract costs only when the computing tenant-level billing.
  select into recurring_usd_cents t.recurring_usd_cents
    from tenants t
    where billed_prefix = t.tenant
  ;

  -- Apply a recurring service cost, if defined.
  if recurring_usd_cents != 0 then
    line_items = line_items || jsonb_build_object(
      'description', 'Recurring service charge',
      'count', 1,
      'rate', recurring_usd_cents,
      'subtotal', recurring_usd_cents
    );
  end if;

  select into line_items, processed_data_gb, task_usage_hours
    line_items || (
      select json_agg(
              (item - 'subtotal_frac') ||
              jsonb_build_object(
                'subtotal', round((item->'subtotal_frac')::numeric)
              )
            )::jsonb
      from jsonb_array_elements(report->0->'line_items') as item
    ),
    (report->0->'processed_data_gb')::numeric,
    (report->0->'task_usage_hours')::numeric
  from internal.incremental_usage_report('monthly', billed_prefix, billed_range) as report;

  -- Does the free trial range overlap the month in question?
  if not isempty(free_trial_range) and (free_trial_range && billed_range) then
    free_trial_overlap = billed_range * free_trial_range;
    -- Determine the total amount of data processing and task usage under `billed_prefix`
    -- during the portion of `billed_month` that `free_trial_range` covers.
    select into
      free_trial_credit sum((line_item->>'subtotal_frac')::numeric)
    from
      jsonb_array_elements(
        internal.incremental_usage_report('daily', billed_prefix, free_trial_overlap)
      ) as line_item;

    line_items = line_items || jsonb_build_object(
      'description', 'Free trial credit',
      'count', 1,
      'rate', round(free_trial_credit) * -1,
      'subtotal', round(free_trial_credit) * -1
    );
  end if;

  -- Apply any billing adjustments.
  for adjustment in select * from internal.billing_adjustments a
    where a.billed_month = billed_month and a.tenant = billed_prefix
  loop
    line_items = line_items || jsonb_build_object(
      'description', adjustment.detail,
      'count', 1,
      'rate', adjustment.usd_cents,
      'subtotal', adjustment.usd_cents
    );
  end loop;

  -- Roll up the final subtotal.
  select into subtotal_usd_cents sum((l->>'subtotal')::numeric)
    from jsonb_array_elements(line_items) l;

  return jsonb_build_object(
    'billed_month', billed_month,
    'billed_prefix', billed_prefix,
    'line_items', line_items,
    'processed_data_gb', processed_data_gb,
    'recurring_fee', coalesce(recurring_usd_cents, 0),
    'subtotal', subtotal_usd_cents,
    'task_usage_hours', task_usage_hours
  );

end
$$;


ALTER FUNCTION internal.test_billing_report(billed_prefix public.catalog_prefix, billed_month timestamp with time zone) OWNER TO postgres;

--
-- Name: tier_line_items(integer, integer[], text, text); Type: FUNCTION; Schema: internal; Owner: postgres
--

CREATE FUNCTION internal.tier_line_items(amount integer, tiers integer[], name text, unit text) RETURNS jsonb
    LANGUAGE plpgsql
    AS $_$
declare
  o_line_items jsonb = '[]'; -- Output variable.
  tier_count   integer;
  tier_pivot   integer;
  tier_rate    integer;
begin

  for idx in 1..array_length(tiers, 1) by 2 loop
    tier_rate = tiers[idx];
    tier_pivot = tiers[idx+1];
    tier_count = least(amount, tier_pivot);
    amount = amount - tier_count;

    o_line_items = o_line_items || jsonb_build_object(
      'description', format(
        case
          when tier_pivot is null then '%1$s (at %4$s/%2$s)'      -- Data processing (at $0.50/GB)
          when idx = 1 then '%1s (first %3$s %2$ss at %4$s/%2$s)' -- Data processing (first 30 GBs at $0.50/GB)
          else '%1$s (next %3$s %2$ss at %4$s/%2$s)'              -- Data processing (next 6 GBs at $0.25/GB)
        end,
        name,
        unit,
        tier_pivot,
        (tier_rate / 100.0)::money
      ),
      'count', tier_count,
      'rate', tier_rate,
      'subtotal', tier_count * tier_rate
    );
  end loop;

  return o_line_items;

end
$_$;


ALTER FUNCTION internal.tier_line_items(amount integer, tiers integer[], name text, unit text) OWNER TO postgres;

--
-- Name: update_support_role(); Type: FUNCTION; Schema: internal; Owner: postgres
--

CREATE FUNCTION internal.update_support_role() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
begin
  insert into role_grants (
    detail,
    subject_role,
    object_role,
    capability
  )
  select
    'Automagically grant support role access to new tenant',
    'estuary_support/',
    tenants.tenant,
    'admin'
  from tenants
  left join role_grants on
    role_grants.object_role = tenants.tenant and
    role_grants.subject_role = 'estuary_support/'
  where role_grants.id is null and
  tenants.tenant not in ('ops/', 'estuary/');

  return null;
END;
$$;


ALTER FUNCTION internal.update_support_role() OWNER TO postgres;

--
-- Name: user_roles(uuid, public.grant_capability); Type: FUNCTION; Schema: internal; Owner: postgres
--

CREATE FUNCTION internal.user_roles(target_user_id uuid, min_capability public.grant_capability DEFAULT 'x_00'::public.grant_capability) RETURNS TABLE(role_prefix public.catalog_prefix, capability public.grant_capability)
    LANGUAGE sql STABLE
    AS $$

  with recursive
  all_roles(role_prefix, capability) as (
      select object_role, capability from user_grants
      where user_id = target_user_id
        and capability >= min_capability
    union
      -- Recursive case: for each object_role granted as 'admin',
      -- project through grants where object_role acts as the subject_role.
      select role_grants.object_role, role_grants.capability
      from role_grants, all_roles
      where role_grants.subject_role ^@ all_roles.role_prefix
        and role_grants.capability >= min_capability
        and all_roles.capability = 'admin'
  )
  select role_prefix, max(capability) from all_roles
  group by role_prefix
  order by role_prefix;

$$;


ALTER FUNCTION internal.user_roles(target_user_id uuid, min_capability public.grant_capability) OWNER TO postgres;

--
-- Name: auth_roles(public.grant_capability); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.auth_roles(min_capability public.grant_capability DEFAULT 'x_00'::public.grant_capability) RETURNS TABLE(role_prefix public.catalog_prefix, capability public.grant_capability)
    LANGUAGE sql STABLE SECURITY DEFINER
    AS $$
  select role_prefix, capability from internal.user_roles(auth_uid(), min_capability)
$$;


ALTER FUNCTION public.auth_roles(min_capability public.grant_capability) OWNER TO postgres;

--
-- Name: FUNCTION auth_roles(min_capability public.grant_capability); Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON FUNCTION public.auth_roles(min_capability public.grant_capability) IS 'auth_roles returns all roles and associated capabilities of the user';


--
-- Name: auth_uid(); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.auth_uid() RETURNS uuid
    LANGUAGE sql STABLE
    AS $$
  select auth.uid()
$$;


ALTER FUNCTION public.auth_uid() OWNER TO postgres;

--
-- Name: FUNCTION auth_uid(); Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON FUNCTION public.auth_uid() IS 'auth_uid returns the user ID of the authenticated user';


--
-- Name: billing_report_202308(public.catalog_prefix, timestamp with time zone, tstzrange); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.billing_report_202308(billed_prefix public.catalog_prefix, billed_month timestamp with time zone, free_trial_range tstzrange) RETURNS jsonb
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
#variable_conflict use_variable
declare
  -- Auth checks
  has_admin_grant boolean;
  has_bypassrls boolean;

  -- Computed
  recurring_usd_cents integer;
--  free_trial_range tstzrange;
  billed_range tstzrange;
  free_trial_overlap tstzrange;

  free_trial_credit numeric;

  -- Temporary line items holders for free trial calculations
  task_usage_line_items jsonb = '[]';
  data_usage_line_items jsonb = '[]';

  -- Calculating adjustments.
  adjustment   internal.billing_adjustments;

  -- Aggregated outputs.
  line_items jsonb = '[]';
  subtotal_usd_cents integer;

  -- Free trial outputs
  free_trial_gb numeric;
  free_trial_hours numeric;
begin

  -- Ensure `billed_month` is the truncated start of the billed month.
  billed_month = date_trunc('month', billed_month);
  billed_range = tstzrange(billed_month, billed_month + '1 month', '[)');

  -- Verify that the user has an admin grant for the requested `billed_prefix`.
  perform 1 from auth_roles('admin') as r where billed_prefix ^@ r.role_prefix;
  has_admin_grant = found;

  -- Check whether user has bypassrls flag
  perform 1 from pg_roles where rolname = session_user and rolbypassrls = true;
  has_bypassrls = found;

  if not has_bypassrls and not has_admin_grant then
    -- errcode 28000 causes PostgREST to return an HTTP 403
    -- see: https://www.postgresql.org/docs/current/errcodes-appendix.html
    -- and: https://postgrest.org/en/stable/errors.html#status-codes
    raise 'You are not authorized for the billed prefix %', billed_prefix using errcode = 28000;
  end if;

  -- Fetch data & usage tiers for `billed_prefix`'s tenant.
--  select into free_trial_range
--      case
--      	when t.free_trial_start is null then 'empty'::tstzrange
--        -- Inclusive start, exclusive end
--       	else tstzrange(date_trunc('day', t.free_trial_start), date_trunc('day', t.free_trial_start) + '1 month', '[)')
--      end
--    from tenants t
--    where billed_prefix ^@ t.tenant
--  ;
  -- Reveal contract costs only when the computing tenant-level billing.
  select into recurring_usd_cents t.recurring_usd_cents
    from tenants t
    where billed_prefix = t.tenant
  ;

  -- Apply a recurring service cost, if defined.
  if recurring_usd_cents != 0 then
    line_items = line_items || jsonb_build_object(
      'description', 'Recurring service charge',
      'count', 1,
      'rate', recurring_usd_cents,
      'subtotal', recurring_usd_cents
    );
  end if;

  line_items = line_items || internal.incremental_usage_report('monthly', billed_prefix, billed_range)->0->'line_items';

  -- Does the free trial range overlap the month in question?
  if not isempty(free_trial_range) and (free_trial_range && billed_range) then
    free_trial_overlap = billed_range * free_trial_range;
    -- Determine the total amount of data processing and task usage under `billed_prefix`
    -- during the portion of `billed_month` that `free_trial_range` covers.
    select into
      free_trial_credit sum((line_item->>'subtotal')::numeric)
    from
      jsonb_array_elements(
        internal.incremental_usage_report('daily', billed_prefix, free_trial_overlap)
      ) as line_item;

    line_items = line_items || jsonb_build_object(
      'description', 'Free trial credit',
      'count', 1,
      'rate', free_trial_credit * -1,
      'subtotal', free_trial_credit * -1
    );
  end if;

  -- Apply any billing adjustments.
  for adjustment in select * from internal.billing_adjustments a
    where a.billed_month = billed_month and a.tenant = billed_prefix
  loop
    line_items = line_items || jsonb_build_object(
      'description', adjustment.detail,
      'count', 1,
      'rate', adjustment.usd_cents,
      'subtotal', adjustment.usd_cents
    );
  end loop;

  -- Roll up the final subtotal.
  select into subtotal_usd_cents sum((l->>'subtotal')::numeric)
    from jsonb_array_elements(line_items) l;

  return jsonb_build_object(
    'billed_month', billed_month,
    'billed_prefix', billed_prefix,
    'line_items', line_items,
    'processed_data_gb', processed_data_gb,
    'recurring_fee', coalesce(recurring_usd_cents, 0),
    'subtotal', subtotal_usd_cents,
    'task_usage_hours', task_usage_hours
  );

end
$$;


ALTER FUNCTION public.billing_report_202308(billed_prefix public.catalog_prefix, billed_month timestamp with time zone, free_trial_range tstzrange) OWNER TO postgres;

--
-- Name: create_refresh_token(boolean, interval, text); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.create_refresh_token(multi_use boolean, valid_for interval, detail text DEFAULT NULL::text) RETURNS json
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare
  secret text;
  refresh_token_row refresh_tokens;
begin
  secret = gen_random_uuid();

  insert into refresh_tokens (detail, user_id, multi_use, valid_for, hash)
  values (
    detail,
    auth_uid(),
    multi_use,
    valid_for,
    crypt(secret, gen_salt('bf'))
  ) returning * into refresh_token_row;

  return json_build_object(
    'id', refresh_token_row.id,
    'secret', secret
  );
commit;
end
$$;


ALTER FUNCTION public.create_refresh_token(multi_use boolean, valid_for interval, detail text) OWNER TO postgres;

--
-- Name: FUNCTION create_refresh_token(multi_use boolean, valid_for interval, detail text); Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON FUNCTION public.create_refresh_token(multi_use boolean, valid_for interval, detail text) IS '
Create a new refresh token which can then be used to generate an access token using `generate_access_token` rpc.
';


--
-- Name: draft_collections_eligible_for_deletion(public.flowid, public.flowid); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.draft_collections_eligible_for_deletion(capture_id public.flowid, draft_id public.flowid) RETURNS void
    LANGUAGE plpgsql
    AS $$
begin

  insert into draft_specs (draft_id, catalog_name, expect_pub_id, spec, spec_type)
  with target_collections as (
    select target_id from live_spec_flows
      where source_id = capture_id
  ),
  collections_read as (
    select target_collections.target_id from target_collections
      join live_spec_flows lsf on target_collections.target_id = lsf.source_id
  ),
  collections_written as (
    select target_collections.target_id from target_collections
      join live_spec_flows lsf on target_collections.target_id = lsf.target_id and lsf.source_id <> capture_id
  ),
  ineligible_collections as (
    select target_id from collections_read
      union select target_id from collections_written
  ),
  eligible_collection_ids as (
    select target_id from target_collections
      except select target_id from ineligible_collections
  ),
  eligible_collections as (
    select
    ls.id,
    ls.catalog_name,
    ls.last_pub_id
    from eligible_collection_ids
    join live_specs ls on eligible_collection_ids.target_id = ls.id
  )
  select draft_id, catalog_name, last_pub_id, null, null from eligible_collections;

end;
$$;


ALTER FUNCTION public.draft_collections_eligible_for_deletion(capture_id public.flowid, draft_id public.flowid) OWNER TO postgres;

--
-- Name: FUNCTION draft_collections_eligible_for_deletion(capture_id public.flowid, draft_id public.flowid); Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON FUNCTION public.draft_collections_eligible_for_deletion(capture_id public.flowid, draft_id public.flowid) IS '
draft_collections_eligible_for_deletion facilitates the deletion of a capture and its associated collections
in the same publication by populating the specified draft with the collections eligible for deletion.
The specified draft should contain the capture pending deletion.
';


--
-- Name: exchange_directive_token(uuid); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.exchange_directive_token(bearer_token uuid) RETURNS public.exchanged_directive
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare
  directive_row directives;
  applied_row applied_directives;
begin

  -- Note that uses_remaining could be null, and in that case `uses_remaining - 1`
  -- would also evaluate to null. This means that we don't actually update
  -- uses_remaining here if the current value is null.
  -- We also intentionally leave the bearer_token in place when uses_remaining
  -- drops to 0, because it's possible that something may come along and
  -- increase uses_remaining again.
  update directives
    set uses_remaining = uses_remaining - 1
    where directives.token = bearer_token
    returning * into directive_row;

  if not found then
    raise 'Bearer token % is not valid', bearer_token
      using errcode = 'check_violation';
  end if;

  if directive_row.uses_remaining is not null and directive_row.uses_remaining < 0 then
    raise 'System quota has been reached, please contact support@estuary.dev in order to proceed.'
      using errcode = 'check_violation';
  end if;

  insert into applied_directives (directive_id, user_id)
  values (directive_row.id, auth.uid())
  returning * into applied_row;

  return (directive_row, applied_row);
end;
$$;


ALTER FUNCTION public.exchange_directive_token(bearer_token uuid) OWNER TO postgres;

--
-- Name: FUNCTION exchange_directive_token(bearer_token uuid); Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON FUNCTION public.exchange_directive_token(bearer_token uuid) IS '
exchange_directive_token allows a user to turn in a directive bearer token
and, in exchange, create an application of that directive.

If the supplied token is valid then a new row is created in `applied_directives`.
The user must next update it with their supplied claims.

Having applied a directive through its token, the user is now able to view
the directive. As a convience, this function also returns the directive
along with the newly-created applied_directive row.
';


--
-- Name: gateway_auth_token(text[]); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.gateway_auth_token(VARIADIC prefixes text[]) RETURNS TABLE(token text, gateway_url text)
    LANGUAGE plpgsql STABLE SECURITY DEFINER
    AS $$
declare
  -- The number of distinct prefixes (i.e. scopes) that were requested.
  requested_prefixes int := (select count(distinct p) from unnest(prefixes) p);
  -- The distinct prefixes, filtered by whether or not they are authorized.
  authorized_prefixes text[];
begin
  
  select array_agg(distinct p) into authorized_prefixes
    from 
      unnest(prefixes) as p
      join auth_roles() as r on starts_with(p, r.role_prefix);

  -- authorized_prefixes will be null when _none_ of the requested prefixes are authorized.
  -- In that case the array_length comparison won't work, so we need an explicit null check.
  if authorized_prefixes is null or array_length(authorized_prefixes, 1) != requested_prefixes then
    -- errcode 28000 causes potgrest to return an HTTP 403
    -- see: https://www.postgresql.org/docs/current/errcodes-appendix.html
    -- and: https://postgrest.org/en/stable/errors.html#status-codes
    raise 'you are not authorized for all of the requested scopes' using errcode = 28000;
  end if;

  return query select internal.sign_jwt(
    json_build_object(
      'exp', trunc(extract(epoch from (now() + interval '1 hour'))),
      'iat', trunc(extract(epoch from (now()))),
      'operation', 'read',
      'prefixes', authorized_prefixes,
      'sub', auth_uid()
    )
  ) as token, internal.gateway_endpoint_url() as gateway_url;
end;
$$;


ALTER FUNCTION public.gateway_auth_token(VARIADIC prefixes text[]) OWNER TO postgres;

--
-- Name: FUNCTION gateway_auth_token(VARIADIC prefixes text[]); Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON FUNCTION public.gateway_auth_token(VARIADIC prefixes text[]) IS 'gateway_auth_token returns a jwt that can be used with the Data Plane Gateway to interact directly with Gazette RPCs.';


--
-- Name: generate_access_token(public.flowid, text); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.generate_access_token(refresh_token_id public.flowid, secret text) RETURNS json
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare
  rt refresh_tokens;
  rt_new_secret text;
  access_token text;
begin

  select * into rt from refresh_tokens where
    refresh_tokens.id = refresh_token_id;

  if not found then
    raise 'could not find refresh_token with the given `refresh_token_id`';
  end if;

  if rt.hash <> crypt(secret, rt.hash) then
    raise 'invalid secret provided';
  end if;

  if (rt.updated_at + rt.valid_for) < now() then
    raise 'refresh_token has expired.';
  end if;

  select sign(json_build_object(
    'exp', trunc(extract(epoch from (now() + interval '1 hour'))),
    'iat', trunc(extract(epoch from (now()))),
    'sub', rt.user_id,
    'role', 'authenticated'
  ), internal.access_token_jwt_secret()) into access_token
  limit 1;

  if rt.multi_use = false then
    rt_new_secret = gen_random_uuid();
    update refresh_tokens
      set
        hash = crypt(rt_new_secret, gen_salt('bf')),
        uses = (uses + 1),
        updated_at = clock_timestamp()
      where refresh_tokens.id = rt.id;
  else
    -- re-set the updated_at timer so the token's validity is refreshed
    update refresh_tokens
      set
        uses = (uses + 1),
        updated_at = clock_timestamp()
      where refresh_tokens.id = rt.id;
  end if;

  if rt_new_secret is null then
    return json_build_object(
      'access_token', access_token
    );
  else
    return json_build_object(
      'access_token', access_token,
      'refresh_token', json_build_object(
        'id', rt.id,
        'secret', rt_new_secret
        )
    );
  end if;
commit;
end
$$;


ALTER FUNCTION public.generate_access_token(refresh_token_id public.flowid, secret text) OWNER TO postgres;

--
-- Name: FUNCTION generate_access_token(refresh_token_id public.flowid, secret text); Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON FUNCTION public.generate_access_token(refresh_token_id public.flowid, secret text) IS '
Given a refresh_token, generates a new access_token.
If the refresh_token is not multi-use, the token''s secret is rotated.
If the refresh_token is multi-use, we reset its validity period by updating its `updated_at` column
';


--
-- Name: generate_opengraph_value(jsonb, jsonb, text); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.generate_opengraph_value(opengraph_raw jsonb, opengraph_patch jsonb, field text) RETURNS public.jsonb_internationalized_value
    LANGUAGE plpgsql IMMUTABLE
    AS $$
BEGIN
    RETURN json_build_object('en-US',internal.jsonb_merge_patch(opengraph_raw, opengraph_patch) #>> ('{"en-US", "'|| field ||'"}')::text[]);
END
$$;


ALTER FUNCTION public.generate_opengraph_value(opengraph_raw jsonb, opengraph_patch jsonb, field text) OWNER TO postgres;

--
-- Name: prune_unchanged_draft_specs(public.flowid); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.prune_unchanged_draft_specs(prune_draft_id public.flowid) RETURNS TABLE(catalog_name public.catalog_name, spec_type public.catalog_spec_type, live_spec_md5 text, draft_spec_md5 text, inferred_schema_md5 text, live_inferred_schema_md5 text)
    LANGUAGE sql
    AS $$
  with to_prune as (
    select * from unchanged_draft_specs u where u.draft_id = prune_draft_id
  ),
  del as (
    delete from draft_specs ds
      where ds.draft_id = prune_draft_id
        and ds.catalog_name in (select catalog_name from to_prune)
  )
  select
    catalog_name,
    spec_type,
    live_spec_md5,
    draft_spec_md5,
    inferred_schema_md5,
    live_inferred_schema_md5
  from to_prune
$$;


ALTER FUNCTION public.prune_unchanged_draft_specs(prune_draft_id public.flowid) OWNER TO postgres;

--
-- Name: FUNCTION prune_unchanged_draft_specs(prune_draft_id public.flowid); Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON FUNCTION public.prune_unchanged_draft_specs(prune_draft_id public.flowid) IS 'Deletes draft_specs belonging to the given draft_id that are identical
 to the published live_specs. For collection specs that use inferred schemas,
 draft_specs will only be deleted if the inferred schema also remains identical.';


--
-- Name: republish_prefix(public.catalog_prefix); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.republish_prefix(prefix public.catalog_prefix) RETURNS public.flowid
    LANGUAGE plpgsql
    AS $$
declare
    draft_id flowid;
    pub_id flowid;
begin
    insert into drafts default values returning id into draft_id;
    insert into draft_specs (draft_id, catalog_name, spec_type, spec, expect_pub_id)
        select draft_id, catalog_name, spec_type, spec, last_pub_id as expect_pub_id
        from live_specs
        where starts_with(catalog_name, prefix) and spec_type is not null;

    insert into publications (draft_id) values (draft_id) returning id into pub_id;
    return pub_id;
end;
$$;


ALTER FUNCTION public.republish_prefix(prefix public.catalog_prefix) OWNER TO postgres;

--
-- Name: FUNCTION republish_prefix(prefix public.catalog_prefix); Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON FUNCTION public.republish_prefix(prefix public.catalog_prefix) IS 'Creates a publication of every task and collection under the given prefix. This will not modify any
of the specs, and will set expect_pub_id to ensure that the publication does not overwrite changes
from other publications. This is intended to be called after an update to the storage mappings of
the prefix to apply the updated mappings.';


--
-- Name: tier_line_items(numeric, integer[], text, text); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.tier_line_items(amount numeric, tiers integer[], name text, unit text) RETURNS jsonb
    LANGUAGE plpgsql
    AS $_$
declare
  o_line_items jsonb = '[]'; -- Output variable.
  tier_count   numeric;
  tier_pivot   integer;
  tier_rate    integer;
begin

  for idx in 1..array_length(tiers, 1) by 2 loop
    tier_rate = tiers[idx];
    tier_pivot = tiers[idx+1];
    tier_count = least(amount, tier_pivot);
    amount = amount - tier_count;

    o_line_items = o_line_items || jsonb_build_object(
      'description', format(
        case
          when tier_pivot is null then '%1$s (at %4$s/%2$s)'      -- Data processing (at $0.50/GB)
          when idx = 1 then '%1s (first %3$s %2$ss at %4$s/%2$s)' -- Data processing (first 30 GBs at $0.50/GB)
          else '%1$s (next %3$s %2$ss at %4$s/%2$s)'              -- Data processing (next 6 GBs at $0.25/GB)
        end,
        name,
        unit,
        tier_pivot,
        (tier_rate / 100.0)::money
      ),
      'count', tier_count,
      'rate', tier_rate,
      'subtotal', round(tier_count * tier_rate)
    );
  end loop;

  return o_line_items;

end
$_$;


ALTER FUNCTION public.tier_line_items(amount numeric, tiers integer[], name text, unit text) OWNER TO postgres;

--
-- Name: user_info_summary(); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.user_info_summary() RETURNS json
    LANGUAGE sql
    AS $$
    with all_grants(role_prefix, capability) as (
        select role_prefix, capability from auth_roles()
    )
    select json_build_object(
        'hasDemoAccess', exists(select 1 from all_grants where role_prefix = 'demo/' and capability >= 'read'),
        'hasSupportAccess', exists(select 1 from all_grants where role_prefix = 'estuary_support/' and capability >= 'admin'),
        'hasAnyAccess', exists(select 1 from all_grants)
    )

$$;


ALTER FUNCTION public.user_info_summary() OWNER TO postgres;

--
-- Name: FUNCTION user_info_summary(); Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON FUNCTION public.user_info_summary() IS 'Returns a JSON object with a few computed attributes for the UI.
These would otherwise require the UI to fetch the complete list of authorized grants,
which can be quite slow for users with many grants. Returns a response like:
{
    hasDemoAccess: boolean, //true if the user has `read` on `demo/` tenant,
    hasSupportAccess: boolean, // true if user has `admin` on `estuary_support/`
    hasAnyAccess: boolean, // true if user has any authorization grants at all
}';


--
-- Name: log_lines; Type: TABLE; Schema: internal; Owner: postgres
--

CREATE TABLE internal.log_lines (
    log_line text NOT NULL,
    logged_at timestamp with time zone DEFAULT now() NOT NULL,
    stream text NOT NULL,
    token uuid NOT NULL
);


ALTER TABLE internal.log_lines OWNER TO postgres;

--
-- Name: TABLE log_lines; Type: COMMENT; Schema: internal; Owner: postgres
--

COMMENT ON TABLE internal.log_lines IS 'Logs produced by server-side operations';


--
-- Name: COLUMN log_lines.log_line; Type: COMMENT; Schema: internal; Owner: postgres
--

COMMENT ON COLUMN internal.log_lines.log_line IS 'Logged line';


--
-- Name: COLUMN log_lines.logged_at; Type: COMMENT; Schema: internal; Owner: postgres
--

COMMENT ON COLUMN internal.log_lines.logged_at IS 'Time at which the log was collected';


--
-- Name: COLUMN log_lines.stream; Type: COMMENT; Schema: internal; Owner: postgres
--

COMMENT ON COLUMN internal.log_lines.stream IS 'Identifier of the log stream within the job';


--
-- Name: COLUMN log_lines.token; Type: COMMENT; Schema: internal; Owner: postgres
--

COMMENT ON COLUMN internal.log_lines.token IS 'Bearer token which demarks and provides accesss to a set of logs';


--
-- Name: view_logs(uuid); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.view_logs(bearer_token uuid) RETURNS SETOF internal.log_lines
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
begin
  return query select * from internal.log_lines where internal.log_lines.token = bearer_token;
end;
$$;


ALTER FUNCTION public.view_logs(bearer_token uuid) OWNER TO postgres;

--
-- Name: FUNCTION view_logs(bearer_token uuid); Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON FUNCTION public.view_logs(bearer_token uuid) IS 'view_logs accepts a log bearer_token and returns its matching log lines';


--
-- Name: view_user_profile(uuid); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.view_user_profile(bearer_user_id uuid) RETURNS public.user_profile
    LANGUAGE sql STABLE SECURITY DEFINER
    AS $$                                               
  select                                                                       
    user_id,                              
    email,                       
    full_name,                                                                 
    avatar_url                                                
  from internal.user_profiles where user_id = bearer_user_id; 
$$;


ALTER FUNCTION public.view_user_profile(bearer_user_id uuid) OWNER TO postgres;

--
-- Name: FUNCTION view_user_profile(bearer_user_id uuid); Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON FUNCTION public.view_user_profile(bearer_user_id uuid) IS 'view_user_profile returns the profile of the given user ID';


--
-- Name: _model; Type: TABLE; Schema: internal; Owner: postgres
--

CREATE TABLE internal._model (
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    detail text,
    id public.flowid NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE internal._model OWNER TO postgres;

--
-- Name: TABLE _model; Type: COMMENT; Schema: internal; Owner: postgres
--

COMMENT ON TABLE internal._model IS 'Model table for the creation of other tables';


--
-- Name: COLUMN _model.created_at; Type: COMMENT; Schema: internal; Owner: postgres
--

COMMENT ON COLUMN internal._model.created_at IS 'Time at which the record was created';


--
-- Name: COLUMN _model.detail; Type: COMMENT; Schema: internal; Owner: postgres
--

COMMENT ON COLUMN internal._model.detail IS 'Description of the record';


--
-- Name: COLUMN _model.id; Type: COMMENT; Schema: internal; Owner: postgres
--

COMMENT ON COLUMN internal._model.id IS 'ID of the record';


--
-- Name: COLUMN _model.updated_at; Type: COMMENT; Schema: internal; Owner: postgres
--

COMMENT ON COLUMN internal._model.updated_at IS 'Time at which the record was last updated';


--
-- Name: _model_async; Type: TABLE; Schema: internal; Owner: postgres
--

CREATE TABLE internal._model_async (
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    detail text,
    id public.flowid NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    job_status public.jsonb_obj DEFAULT '{"type": "queued"}'::jsonb NOT NULL,
    logs_token uuid DEFAULT gen_random_uuid() NOT NULL,
    background boolean DEFAULT false NOT NULL
);


ALTER TABLE internal._model_async OWNER TO postgres;

--
-- Name: TABLE _model_async; Type: COMMENT; Schema: internal; Owner: postgres
--

COMMENT ON TABLE internal._model_async IS 'Model table for the creation of other tables representing a server-side operation';


--
-- Name: COLUMN _model_async.created_at; Type: COMMENT; Schema: internal; Owner: postgres
--

COMMENT ON COLUMN internal._model_async.created_at IS 'Time at which the record was created';


--
-- Name: COLUMN _model_async.detail; Type: COMMENT; Schema: internal; Owner: postgres
--

COMMENT ON COLUMN internal._model_async.detail IS 'Description of the record';


--
-- Name: COLUMN _model_async.id; Type: COMMENT; Schema: internal; Owner: postgres
--

COMMENT ON COLUMN internal._model_async.id IS 'ID of the record';


--
-- Name: COLUMN _model_async.updated_at; Type: COMMENT; Schema: internal; Owner: postgres
--

COMMENT ON COLUMN internal._model_async.updated_at IS 'Time at which the record was last updated';


--
-- Name: COLUMN _model_async.job_status; Type: COMMENT; Schema: internal; Owner: postgres
--

COMMENT ON COLUMN internal._model_async.job_status IS 'Server-side job executation status of the record';


--
-- Name: COLUMN _model_async.logs_token; Type: COMMENT; Schema: internal; Owner: postgres
--

COMMENT ON COLUMN internal._model_async.logs_token IS 'Bearer token for accessing logs of the server-side operation';


--
-- Name: COLUMN _model_async.background; Type: COMMENT; Schema: internal; Owner: postgres
--

COMMENT ON COLUMN internal._model_async.background IS 'indicates whether this is a background job, which will be processed with a lower priority than interactive jobs';


--
-- Name: alert_data_processing; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.alert_data_processing (
    catalog_name public.catalog_name NOT NULL,
    evaluation_interval interval NOT NULL
);


ALTER TABLE public.alert_data_processing OWNER TO postgres;

--
-- Name: alert_subscriptions; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.alert_subscriptions (
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    detail text,
    id public.flowid NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    catalog_prefix public.catalog_prefix NOT NULL,
    email text
);


ALTER TABLE public.alert_subscriptions OWNER TO postgres;

--
-- Name: COLUMN alert_subscriptions.created_at; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.alert_subscriptions.created_at IS 'Time at which the record was created';


--
-- Name: COLUMN alert_subscriptions.detail; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.alert_subscriptions.detail IS 'Description of the record';


--
-- Name: COLUMN alert_subscriptions.id; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.alert_subscriptions.id IS 'ID of the record';


--
-- Name: COLUMN alert_subscriptions.updated_at; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.alert_subscriptions.updated_at IS 'Time at which the record was last updated';


--
-- Name: catalog_stats; Type: TABLE; Schema: public; Owner: stats_loader
--

CREATE TABLE public.catalog_stats (
    catalog_name public.catalog_name NOT NULL,
    grain text NOT NULL,
    ts timestamp with time zone NOT NULL,
    bytes_written_by_me bigint DEFAULT 0 NOT NULL,
    docs_written_by_me bigint DEFAULT 0 NOT NULL,
    bytes_read_by_me bigint DEFAULT 0 NOT NULL,
    docs_read_by_me bigint DEFAULT 0 NOT NULL,
    bytes_written_to_me bigint DEFAULT 0 NOT NULL,
    docs_written_to_me bigint DEFAULT 0 NOT NULL,
    bytes_read_from_me bigint DEFAULT 0 NOT NULL,
    docs_read_from_me bigint DEFAULT 0 NOT NULL,
    usage_seconds integer DEFAULT 0 NOT NULL,
    warnings integer DEFAULT 0 NOT NULL,
    errors integer DEFAULT 0 NOT NULL,
    failures integer DEFAULT 0 NOT NULL,
    flow_document json NOT NULL
)
PARTITION BY LIST (grain);


ALTER TABLE public.catalog_stats OWNER TO stats_loader;

--
-- Name: TABLE catalog_stats; Type: COMMENT; Schema: public; Owner: stats_loader
--

COMMENT ON TABLE public.catalog_stats IS 'Statistics for Flow catalogs';


--
-- Name: COLUMN catalog_stats.grain; Type: COMMENT; Schema: public; Owner: stats_loader
--

COMMENT ON COLUMN public.catalog_stats.grain IS '
Time grain that stats are summed over.

One of "monthly", "daily", or "hourly".
';


--
-- Name: COLUMN catalog_stats.ts; Type: COMMENT; Schema: public; Owner: stats_loader
--

COMMENT ON COLUMN public.catalog_stats.ts IS '
Timestamp indicating the start time of the time grain.

Monthly grains start on day 1 of the month, at hour 0 and minute 0.
Daily grains start on the day, at hour 0 and minute 0.
Hourly grains start on the hour, at minute 0.
';


--
-- Name: COLUMN catalog_stats.bytes_written_by_me; Type: COMMENT; Schema: public; Owner: stats_loader
--

COMMENT ON COLUMN public.catalog_stats.bytes_written_by_me IS 'Bytes written by this catalog, summed over the time grain.';


--
-- Name: COLUMN catalog_stats.docs_written_by_me; Type: COMMENT; Schema: public; Owner: stats_loader
--

COMMENT ON COLUMN public.catalog_stats.docs_written_by_me IS 'Documents written by this catalog, summed over the time grain.';


--
-- Name: COLUMN catalog_stats.bytes_read_by_me; Type: COMMENT; Schema: public; Owner: stats_loader
--

COMMENT ON COLUMN public.catalog_stats.bytes_read_by_me IS 'Bytes read by this catalog, summed over the time grain.';


--
-- Name: COLUMN catalog_stats.docs_read_by_me; Type: COMMENT; Schema: public; Owner: stats_loader
--

COMMENT ON COLUMN public.catalog_stats.docs_read_by_me IS 'Documents read by this catalog, summed over the time grain.';


--
-- Name: COLUMN catalog_stats.bytes_written_to_me; Type: COMMENT; Schema: public; Owner: stats_loader
--

COMMENT ON COLUMN public.catalog_stats.bytes_written_to_me IS 'Bytes written to this catalog, summed over the time grain.';


--
-- Name: COLUMN catalog_stats.docs_written_to_me; Type: COMMENT; Schema: public; Owner: stats_loader
--

COMMENT ON COLUMN public.catalog_stats.docs_written_to_me IS 'Documents written to this catalog, summed over the time grain.';


--
-- Name: COLUMN catalog_stats.bytes_read_from_me; Type: COMMENT; Schema: public; Owner: stats_loader
--

COMMENT ON COLUMN public.catalog_stats.bytes_read_from_me IS 'Bytes read from this catalog, summed over the time grain.';


--
-- Name: COLUMN catalog_stats.docs_read_from_me; Type: COMMENT; Schema: public; Owner: stats_loader
--

COMMENT ON COLUMN public.catalog_stats.docs_read_from_me IS 'Documents read from this catalog, summed over the time grain.';


--
-- Name: COLUMN catalog_stats.usage_seconds; Type: COMMENT; Schema: public; Owner: stats_loader
--

COMMENT ON COLUMN public.catalog_stats.usage_seconds IS 'Metered usage of this catalog task.';


--
-- Name: COLUMN catalog_stats.flow_document; Type: COMMENT; Schema: public; Owner: stats_loader
--

COMMENT ON COLUMN public.catalog_stats.flow_document IS 'Aggregated statistics document for the given catalog name and grain';


--
-- Name: catalog_stats_hourly; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.catalog_stats_hourly (
    catalog_name public.catalog_name NOT NULL,
    grain text NOT NULL,
    ts timestamp with time zone NOT NULL,
    bytes_written_by_me bigint DEFAULT 0 NOT NULL,
    docs_written_by_me bigint DEFAULT 0 NOT NULL,
    bytes_read_by_me bigint DEFAULT 0 NOT NULL,
    docs_read_by_me bigint DEFAULT 0 NOT NULL,
    bytes_written_to_me bigint DEFAULT 0 NOT NULL,
    docs_written_to_me bigint DEFAULT 0 NOT NULL,
    bytes_read_from_me bigint DEFAULT 0 NOT NULL,
    docs_read_from_me bigint DEFAULT 0 NOT NULL,
    usage_seconds integer DEFAULT 0 NOT NULL,
    warnings integer DEFAULT 0 NOT NULL,
    errors integer DEFAULT 0 NOT NULL,
    failures integer DEFAULT 0 NOT NULL,
    flow_document json NOT NULL
);


ALTER TABLE public.catalog_stats_hourly OWNER TO postgres;

--
-- Name: live_specs; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.live_specs (
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    detail text,
    id public.flowid NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    catalog_name public.catalog_name NOT NULL,
    connector_image_name text,
    connector_image_tag text,
    last_pub_id public.flowid NOT NULL,
    reads_from text[],
    spec json,
    spec_type public.catalog_spec_type,
    writes_to text[],
    last_build_id public.flowid NOT NULL,
    md5 text GENERATED ALWAYS AS (md5(TRIM(BOTH FROM (spec)::text))) STORED,
    built_spec json,
    inferred_schema_md5 text,
    controller_next_run timestamp with time zone,
    data_plane_id public.flowid DEFAULT '0e:8e:17:d0:4f:ac:d4:00'::macaddr8 NOT NULL,
    journal_template_name text GENERATED ALWAYS AS (((built_spec -> 'partitionTemplate'::text) ->> 'name'::text)) STORED,
    shard_template_id text GENERATED ALWAYS AS (COALESCE(((built_spec -> 'shardTemplate'::text) ->> 'id'::text), (((built_spec -> 'derivation'::text) -> 'shardTemplate'::text) ->> 'id'::text))) STORED,
    dependency_hash text
);


ALTER TABLE public.live_specs OWNER TO postgres;

--
-- Name: TABLE live_specs; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON TABLE public.live_specs IS 'Live (in other words, current) catalog specifications of the platform';


--
-- Name: COLUMN live_specs.created_at; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.live_specs.created_at IS 'Time at which the record was created';


--
-- Name: COLUMN live_specs.detail; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.live_specs.detail IS 'Description of the record';


--
-- Name: COLUMN live_specs.id; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.live_specs.id IS 'ID of the record';


--
-- Name: COLUMN live_specs.updated_at; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.live_specs.updated_at IS 'Time at which the record was last updated';


--
-- Name: COLUMN live_specs.catalog_name; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.live_specs.catalog_name IS 'Catalog name of this specification';


--
-- Name: COLUMN live_specs.connector_image_name; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.live_specs.connector_image_name IS 'OCI (Docker) connector image name used by this specification';


--
-- Name: COLUMN live_specs.connector_image_tag; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.live_specs.connector_image_tag IS 'OCI (Docker) connector image tag used by this specification';


--
-- Name: COLUMN live_specs.last_pub_id; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.live_specs.last_pub_id IS 'Last publication ID which updated this specification';


--
-- Name: COLUMN live_specs.reads_from; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.live_specs.reads_from IS '
List of collections read by this catalog task specification,
or NULL if not applicable to this specification type.
These adjacencies are also indexed within `live_spec_flows`.
';


--
-- Name: COLUMN live_specs.spec; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.live_specs.spec IS 'Serialized catalog specification, or NULL if this specification is deleted';


--
-- Name: COLUMN live_specs.spec_type; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.live_specs.spec_type IS 'Type of this catalog specification, or NULL if this specification is deleted';


--
-- Name: COLUMN live_specs.writes_to; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.live_specs.writes_to IS '
List of collections written by this catalog task specification,
or NULL if not applicable to this specification type.
These adjacencies are also indexed within `live_spec_flows`.
';


--
-- Name: COLUMN live_specs.last_build_id; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.live_specs.last_build_id IS '
Last publication ID under which this specification was built and activated
into the data-plane, even if it was not necessarily updated.

A specification may be included in a publication which did not directly
change it simply because of its connection to other specifications which
were part of that publication: Flow identifies connected specifications
in order to holistically verify and test their combined behaviors.
';


--
-- Name: COLUMN live_specs.built_spec; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.live_specs.built_spec IS 'Built specification for this catalog';


--
-- Name: COLUMN live_specs.inferred_schema_md5; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.live_specs.inferred_schema_md5 IS 'The md5 sum of the inferred schema that was published with this spec';


--
-- Name: COLUMN live_specs.controller_next_run; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.live_specs.controller_next_run IS 'The next time the controller for this spec should run.';


--
-- Name: COLUMN live_specs.dependency_hash; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.live_specs.dependency_hash IS 'An hash of all the dependencies which were used to build this spec.
Any change to the _model_ of a dependency will change this hash.
Changes to the built spec of a dependency without an accompanying
model change will not change the hash.';


--
-- Name: alert_data_movement_stalled; Type: VIEW; Schema: internal; Owner: postgres
--

CREATE VIEW internal.alert_data_movement_stalled AS
 SELECT 'data_movement_stalled'::public.alert_type AS alert_type,
    alert_data_processing.catalog_name,
    json_build_object('bytes_processed', (COALESCE(sum(((catalog_stats_hourly.bytes_written_by_me + catalog_stats_hourly.bytes_written_to_me) + catalog_stats_hourly.bytes_read_by_me)), (0)::numeric))::bigint, 'recipients', array_agg(DISTINCT jsonb_build_object('email', alert_subscriptions.email, 'full_name', (users.raw_user_meta_data ->> 'full_name'::text))), 'evaluation_interval', alert_data_processing.evaluation_interval, 'spec_type', live_specs.spec_type) AS arguments,
    true AS firing
   FROM ((((public.alert_data_processing
     LEFT JOIN public.live_specs ON ((((alert_data_processing.catalog_name)::text = (live_specs.catalog_name)::text) AND (live_specs.spec IS NOT NULL) AND ((((live_specs.spec -> 'shards'::text) ->> 'disable'::text))::boolean IS NOT TRUE))))
     LEFT JOIN public.catalog_stats_hourly ON ((((alert_data_processing.catalog_name)::text = (catalog_stats_hourly.catalog_name)::text) AND (catalog_stats_hourly.ts >= date_trunc('hour'::text, (now() - alert_data_processing.evaluation_interval))))))
     LEFT JOIN public.alert_subscriptions ON ((((alert_data_processing.catalog_name)::text ^@ (alert_subscriptions.catalog_prefix)::text) AND (alert_subscriptions.email IS NOT NULL))))
     LEFT JOIN auth.users ON ((((users.email)::text = alert_subscriptions.email) AND (users.is_sso_user IS FALSE))))
  WHERE (live_specs.created_at <= date_trunc('hour'::text, (now() - alert_data_processing.evaluation_interval)))
  GROUP BY alert_data_processing.catalog_name, alert_data_processing.evaluation_interval, live_specs.spec_type
 HAVING ((COALESCE(sum(((catalog_stats_hourly.bytes_written_by_me + catalog_stats_hourly.bytes_written_to_me) + catalog_stats_hourly.bytes_read_by_me)), (0)::numeric))::bigint = 0);


ALTER VIEW internal.alert_data_movement_stalled OWNER TO postgres;

--
-- Name: tenants; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.tenants (
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    detail text,
    id public.flowid NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    tenant public.catalog_tenant NOT NULL,
    tasks_quota integer DEFAULT 10 NOT NULL,
    collections_quota integer DEFAULT 500 NOT NULL,
    data_tiers integer[] DEFAULT '{50}'::integer[] NOT NULL,
    usage_tiers integer[] DEFAULT '{14,4464,7}'::integer[] NOT NULL,
    recurring_usd_cents integer DEFAULT 0 NOT NULL,
    trial_start date,
    payment_provider public.payment_provider_type DEFAULT 'stripe'::public.payment_provider_type,
    gcm_account_id uuid,
    hide_preview boolean DEFAULT false NOT NULL,
    CONSTRAINT "data_tiers is odd" CHECK (((array_length(data_tiers, 1) % 2) = 1)),
    CONSTRAINT "usage_tiers is odd" CHECK (((array_length(usage_tiers, 1) % 2) = 1))
);


ALTER TABLE public.tenants OWNER TO postgres;

--
-- Name: TABLE tenants; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON TABLE public.tenants IS '
A tenant is the top-level unit of organization in the Flow catalog namespace.
';


--
-- Name: COLUMN tenants.created_at; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.tenants.created_at IS 'Time at which the record was created';


--
-- Name: COLUMN tenants.detail; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.tenants.detail IS 'Description of the record';


--
-- Name: COLUMN tenants.id; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.tenants.id IS 'ID of the record';


--
-- Name: COLUMN tenants.updated_at; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.tenants.updated_at IS 'Time at which the record was last updated';


--
-- Name: COLUMN tenants.tenant; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.tenants.tenant IS 'Catalog tenant identified by this record';


--
-- Name: COLUMN tenants.tasks_quota; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.tenants.tasks_quota IS 'Maximum number of active tasks that the tenant may have';


--
-- Name: COLUMN tenants.collections_quota; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.tenants.collections_quota IS 'Maximum number of collections that the tenant may have';


--
-- Name: COLUMN tenants.data_tiers; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.tenants.data_tiers IS '
Tiered data processing volumes and prices.

Structured as an odd-length array of a price (in cents) followed by a volume (in GB).
For example, `{50, 1024, 30, 2048, 20}` is interpreted as:
  * $0.50 per GB for the first TB (1,024 GB).
  * $0.30 per GB for the next two TB (3TB cumulative).
  * $0.20 per GB thereafter.
';


--
-- Name: COLUMN tenants.usage_tiers; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.tenants.usage_tiers IS '
Tiered task usage quantities and prices.

Structured as an odd-length array of a price (in cents) followed by a quantity (in hours).
For example, `{30, 1440, 20, 2880, 15}` is interpreted as:
  * $0.30 per hour for the first 1,440 hours.
  * $0.20 per hour for the next 2,880 hours (4,320 hours total).
  * $0.15 per hour thereafter.
';


--
-- Name: COLUMN tenants.recurring_usd_cents; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.tenants.recurring_usd_cents IS '
Recurring monthly cost incurred by a tenant under a contracted relationship, in US cents (1/100ths of a USD).
';


--
-- Name: COLUMN tenants.hide_preview; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.tenants.hide_preview IS '
Hide data preview in the collections page for this tenant, used as a measure for preventing users with access to this tenant from viewing sensitive data in collections
';


--
-- Name: alert_free_trial; Type: VIEW; Schema: internal; Owner: postgres
--

CREATE VIEW internal.alert_free_trial AS
 SELECT 'free_trial'::public.alert_type AS alert_type,
    (((tenants.tenant)::text || 'alerts/free_trial'::text))::public.catalog_name AS catalog_name,
    json_build_object('tenant', tenants.tenant, 'recipients', array_agg(DISTINCT jsonb_build_object('email', alert_subscriptions.email, 'full_name', (users.raw_user_meta_data ->> 'full_name'::text))), 'trial_start', tenants.trial_start, 'trial_end', ((tenants.trial_start + '1 mon'::interval))::date, 'has_credit_card', bool_or((customers."invoice_settings/default_payment_method" IS NOT NULL))) AS arguments,
    ((tenants.trial_start IS NOT NULL) AND ((now() - (tenants.trial_start)::timestamp with time zone) < '1 mon'::interval) AND (tenants.trial_start <= now())) AS firing
   FROM (((public.tenants
     LEFT JOIN public.alert_subscriptions ON ((((alert_subscriptions.catalog_prefix)::text ^@ (tenants.tenant)::text) AND (alert_subscriptions.email IS NOT NULL))))
     LEFT JOIN stripe.customers ON ((customers.name = (tenants.tenant)::text)))
     LEFT JOIN auth.users ON ((((users.email)::text = alert_subscriptions.email) AND (users.is_sso_user IS FALSE))))
  GROUP BY tenants.tenant, tenants.trial_start;


ALTER VIEW internal.alert_free_trial OWNER TO postgres;

--
-- Name: alert_free_trial_ending; Type: VIEW; Schema: internal; Owner: postgres
--

CREATE VIEW internal.alert_free_trial_ending AS
 SELECT 'free_trial_ending'::public.alert_type AS alert_type,
    (((tenants.tenant)::text || 'alerts/free_trial_ending'::text))::public.catalog_name AS catalog_name,
    json_build_object('tenant', tenants.tenant, 'recipients', array_agg(DISTINCT jsonb_build_object('email', alert_subscriptions.email, 'full_name', (users.raw_user_meta_data ->> 'full_name'::text))), 'trial_start', tenants.trial_start, 'trial_end', ((tenants.trial_start + '1 mon'::interval))::date, 'has_credit_card', bool_or((customers."invoice_settings/default_payment_method" IS NOT NULL))) AS arguments,
    ((tenants.trial_start IS NOT NULL) AND ((now() - (tenants.trial_start)::timestamp with time zone) >= ('1 mon'::interval - '5 days'::interval)) AND ((now() - (tenants.trial_start)::timestamp with time zone) < ('1 mon'::interval - '4 days'::interval)) AND (tenants.trial_start <= now())) AS firing
   FROM (((public.tenants
     LEFT JOIN public.alert_subscriptions ON ((((alert_subscriptions.catalog_prefix)::text ^@ (tenants.tenant)::text) AND (alert_subscriptions.email IS NOT NULL))))
     LEFT JOIN stripe.customers ON ((customers.name = (tenants.tenant)::text)))
     LEFT JOIN auth.users ON ((((users.email)::text = alert_subscriptions.email) AND (users.is_sso_user IS FALSE))))
  GROUP BY tenants.tenant, tenants.trial_start;


ALTER VIEW internal.alert_free_trial_ending OWNER TO postgres;

--
-- Name: alert_free_trial_stalled; Type: VIEW; Schema: internal; Owner: postgres
--

CREATE VIEW internal.alert_free_trial_stalled AS
 SELECT 'free_trial_stalled'::public.alert_type AS alert_type,
    (((tenants.tenant)::text || 'alerts/free_trial_stalled'::text))::public.catalog_name AS catalog_name,
    json_build_object('tenant', tenants.tenant, 'recipients', array_agg(DISTINCT jsonb_build_object('email', alert_subscriptions.email, 'full_name', (users.raw_user_meta_data ->> 'full_name'::text))), 'trial_start', tenants.trial_start, 'trial_end', ((tenants.trial_start + '1 mon'::interval))::date) AS arguments,
    true AS firing
   FROM (((public.tenants
     LEFT JOIN public.alert_subscriptions ON ((((alert_subscriptions.catalog_prefix)::text ^@ (tenants.tenant)::text) AND (alert_subscriptions.email IS NOT NULL))))
     LEFT JOIN stripe.customers ON ((customers.name = (tenants.tenant)::text)))
     LEFT JOIN auth.users ON ((((users.email)::text = alert_subscriptions.email) AND (users.is_sso_user IS FALSE))))
  WHERE ((tenants.trial_start IS NOT NULL) AND ((now() - (tenants.trial_start)::timestamp with time zone) >= ('1 mon'::interval + '5 days'::interval)) AND (tenants.trial_start <= now()) AND (customers."invoice_settings/default_payment_method" IS NULL))
  GROUP BY tenants.tenant, tenants.trial_start;


ALTER VIEW internal.alert_free_trial_stalled OWNER TO postgres;

--
-- Name: alert_missing_payment_method; Type: VIEW; Schema: internal; Owner: postgres
--

CREATE VIEW internal.alert_missing_payment_method AS
 SELECT 'missing_payment_method'::public.alert_type AS alert_type,
    (((tenants.tenant)::text || 'alerts/missing_payment_method'::text))::public.catalog_name AS catalog_name,
    json_build_object('tenant', tenants.tenant, 'recipients', array_agg(DISTINCT jsonb_build_object('email', alert_subscriptions.email, 'full_name', (users.raw_user_meta_data ->> 'full_name'::text))), 'trial_start', tenants.trial_start, 'trial_end', ((tenants.trial_start + '1 mon'::interval))::date, 'plan_state',
        CASE
            WHEN (tenants.trial_start IS NULL) THEN 'free_tier'::text
            WHEN ((now() - (tenants.trial_start)::timestamp with time zone) < '1 mon'::interval) THEN 'free_trial'::text
            ELSE 'paid'::text
        END) AS arguments,
    bool_or((customers."invoice_settings/default_payment_method" IS NULL)) AS firing
   FROM (((public.tenants
     LEFT JOIN public.alert_subscriptions ON ((((alert_subscriptions.catalog_prefix)::text ^@ (tenants.tenant)::text) AND (alert_subscriptions.email IS NOT NULL))))
     LEFT JOIN stripe.customers ON ((customers.name = (tenants.tenant)::text)))
     LEFT JOIN auth.users ON ((((users.email)::text = alert_subscriptions.email) AND (users.is_sso_user IS FALSE))))
  GROUP BY tenants.tenant, tenants.trial_start;


ALTER VIEW internal.alert_missing_payment_method OWNER TO postgres;

--
-- Name: billing_adjustments; Type: TABLE; Schema: internal; Owner: postgres
--

CREATE TABLE internal.billing_adjustments (
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    detail text,
    id public.flowid NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    tenant public.catalog_tenant NOT NULL,
    billed_month timestamp with time zone NOT NULL,
    usd_cents integer NOT NULL,
    authorizer text NOT NULL,
    CONSTRAINT "billed_month must be at a month boundary" CHECK ((billed_month = date_trunc('month'::text, billed_month)))
);


ALTER TABLE internal.billing_adjustments OWNER TO postgres;

--
-- Name: TABLE billing_adjustments; Type: COMMENT; Schema: internal; Owner: postgres
--

COMMENT ON TABLE internal.billing_adjustments IS 'Internal table for authorized adjustments to tenant invoices, such as make-goods or negotiated service fees';


--
-- Name: COLUMN billing_adjustments.created_at; Type: COMMENT; Schema: internal; Owner: postgres
--

COMMENT ON COLUMN internal.billing_adjustments.created_at IS 'Time at which the record was created';


--
-- Name: COLUMN billing_adjustments.detail; Type: COMMENT; Schema: internal; Owner: postgres
--

COMMENT ON COLUMN internal.billing_adjustments.detail IS 'Description of the record';


--
-- Name: COLUMN billing_adjustments.id; Type: COMMENT; Schema: internal; Owner: postgres
--

COMMENT ON COLUMN internal.billing_adjustments.id IS 'ID of the record';


--
-- Name: COLUMN billing_adjustments.updated_at; Type: COMMENT; Schema: internal; Owner: postgres
--

COMMENT ON COLUMN internal.billing_adjustments.updated_at IS 'Time at which the record was last updated';


--
-- Name: COLUMN billing_adjustments.tenant; Type: COMMENT; Schema: internal; Owner: postgres
--

COMMENT ON COLUMN internal.billing_adjustments.tenant IS 'Tenant which is being credited or debited.';


--
-- Name: COLUMN billing_adjustments.billed_month; Type: COMMENT; Schema: internal; Owner: postgres
--

COMMENT ON COLUMN internal.billing_adjustments.billed_month IS 'Month to which the adjustment is applied';


--
-- Name: COLUMN billing_adjustments.usd_cents; Type: COMMENT; Schema: internal; Owner: postgres
--

COMMENT ON COLUMN internal.billing_adjustments.usd_cents IS 'Amount of adjustment. Positive values make the bill larger, negative values make it smaller';


--
-- Name: COLUMN billing_adjustments.authorizer; Type: COMMENT; Schema: internal; Owner: postgres
--

COMMENT ON COLUMN internal.billing_adjustments.authorizer IS 'Estuary employee who authorizes the adjustment';


--
-- Name: billing_historicals; Type: TABLE; Schema: internal; Owner: postgres
--

CREATE TABLE internal.billing_historicals (
    tenant public.catalog_tenant NOT NULL,
    billed_month timestamp with time zone NOT NULL,
    report jsonb NOT NULL,
    CONSTRAINT billing_historicals_billed_month_check CHECK ((date_trunc('month'::text, billed_month) = billed_month))
);


ALTER TABLE internal.billing_historicals OWNER TO postgres;

--
-- Name: TABLE billing_historicals; Type: COMMENT; Schema: internal; Owner: postgres
--

COMMENT ON TABLE internal.billing_historicals IS 'Historical billing statements frozen from `billing_report_202308()`.';


--
-- Name: COLUMN billing_historicals.tenant; Type: COMMENT; Schema: internal; Owner: postgres
--

COMMENT ON COLUMN internal.billing_historicals.tenant IS 'The tenant for this billing statement';


--
-- Name: COLUMN billing_historicals.billed_month; Type: COMMENT; Schema: internal; Owner: postgres
--

COMMENT ON COLUMN internal.billing_historicals.billed_month IS 'The month for this billing statement';


--
-- Name: COLUMN billing_historicals.report; Type: COMMENT; Schema: internal; Owner: postgres
--

COMMENT ON COLUMN internal.billing_historicals.report IS 'The historical billing report generated by billing_report_202308()';


--
-- Name: gateway_auth_keys; Type: TABLE; Schema: internal; Owner: postgres
--

CREATE TABLE internal.gateway_auth_keys (
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    detail text,
    id public.flowid NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    secret_key text
);


ALTER TABLE internal.gateway_auth_keys OWNER TO postgres;

--
-- Name: COLUMN gateway_auth_keys.created_at; Type: COMMENT; Schema: internal; Owner: postgres
--

COMMENT ON COLUMN internal.gateway_auth_keys.created_at IS 'Time at which the record was created';


--
-- Name: COLUMN gateway_auth_keys.detail; Type: COMMENT; Schema: internal; Owner: postgres
--

COMMENT ON COLUMN internal.gateway_auth_keys.detail IS 'Description of the record';


--
-- Name: COLUMN gateway_auth_keys.id; Type: COMMENT; Schema: internal; Owner: postgres
--

COMMENT ON COLUMN internal.gateway_auth_keys.id IS 'ID of the record';


--
-- Name: COLUMN gateway_auth_keys.updated_at; Type: COMMENT; Schema: internal; Owner: postgres
--

COMMENT ON COLUMN internal.gateway_auth_keys.updated_at IS 'Time at which the record was last updated';


--
-- Name: gateway_endpoints; Type: TABLE; Schema: internal; Owner: postgres
--

CREATE TABLE internal.gateway_endpoints (
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    detail text,
    id public.flowid NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    name text,
    url text
);


ALTER TABLE internal.gateway_endpoints OWNER TO postgres;

--
-- Name: COLUMN gateway_endpoints.created_at; Type: COMMENT; Schema: internal; Owner: postgres
--

COMMENT ON COLUMN internal.gateway_endpoints.created_at IS 'Time at which the record was created';


--
-- Name: COLUMN gateway_endpoints.detail; Type: COMMENT; Schema: internal; Owner: postgres
--

COMMENT ON COLUMN internal.gateway_endpoints.detail IS 'Description of the record';


--
-- Name: COLUMN gateway_endpoints.id; Type: COMMENT; Schema: internal; Owner: postgres
--

COMMENT ON COLUMN internal.gateway_endpoints.id IS 'ID of the record';


--
-- Name: COLUMN gateway_endpoints.updated_at; Type: COMMENT; Schema: internal; Owner: postgres
--

COMMENT ON COLUMN internal.gateway_endpoints.updated_at IS 'Time at which the record was last updated';


--
-- Name: gcm_accounts; Type: TABLE; Schema: internal; Owner: postgres
--

CREATE TABLE internal.gcm_accounts (
    id uuid NOT NULL,
    obfuscated_id text,
    entitlement_id uuid,
    approved boolean DEFAULT false
);


ALTER TABLE internal.gcm_accounts OWNER TO postgres;

--
-- Name: COLUMN gcm_accounts.id; Type: COMMENT; Schema: internal; Owner: postgres
--

COMMENT ON COLUMN internal.gcm_accounts.id IS 'Google marketplace user ID, received in the first ACCOUNT_ACTIVE pub/sub event and as the subject of the JWT token during signup';


--
-- Name: COLUMN gcm_accounts.obfuscated_id; Type: COMMENT; Schema: internal; Owner: postgres
--

COMMENT ON COLUMN internal.gcm_accounts.obfuscated_id IS 'Google GAIA ID, received in JWT during sign-up, can be used to sign the user in using OAuth2';


--
-- Name: COLUMN gcm_accounts.approved; Type: COMMENT; Schema: internal; Owner: postgres
--

COMMENT ON COLUMN internal.gcm_accounts.approved IS 'Has the account been approved with Google';


--
-- Name: illegal_tenant_names; Type: TABLE; Schema: internal; Owner: postgres
--

CREATE TABLE internal.illegal_tenant_names (
    name public.catalog_tenant NOT NULL
);


ALTER TABLE internal.illegal_tenant_names OWNER TO postgres;

--
-- Name: TABLE illegal_tenant_names; Type: COMMENT; Schema: internal; Owner: postgres
--

COMMENT ON TABLE internal.illegal_tenant_names IS 'Illegal tenant names which are not allowed to be provisioned by users';


--
-- Name: manual_bills; Type: TABLE; Schema: internal; Owner: postgres
--

CREATE TABLE internal.manual_bills (
    tenant public.catalog_tenant NOT NULL,
    usd_cents integer NOT NULL,
    description text NOT NULL,
    date_start date NOT NULL,
    date_end date NOT NULL,
    CONSTRAINT dates_make_sense CHECK ((date_start < date_end))
);


ALTER TABLE internal.manual_bills OWNER TO postgres;

--
-- Name: TABLE manual_bills; Type: COMMENT; Schema: internal; Owner: postgres
--

COMMENT ON TABLE internal.manual_bills IS 'Manually entered bills that span an arbitrary date range';


--
-- Name: catalog_stats_daily; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.catalog_stats_daily (
    catalog_name public.catalog_name NOT NULL,
    grain text NOT NULL,
    ts timestamp with time zone NOT NULL,
    bytes_written_by_me bigint DEFAULT 0 NOT NULL,
    docs_written_by_me bigint DEFAULT 0 NOT NULL,
    bytes_read_by_me bigint DEFAULT 0 NOT NULL,
    docs_read_by_me bigint DEFAULT 0 NOT NULL,
    bytes_written_to_me bigint DEFAULT 0 NOT NULL,
    docs_written_to_me bigint DEFAULT 0 NOT NULL,
    bytes_read_from_me bigint DEFAULT 0 NOT NULL,
    docs_read_from_me bigint DEFAULT 0 NOT NULL,
    usage_seconds integer DEFAULT 0 NOT NULL,
    warnings integer DEFAULT 0 NOT NULL,
    errors integer DEFAULT 0 NOT NULL,
    failures integer DEFAULT 0 NOT NULL,
    flow_document json NOT NULL
);


ALTER TABLE public.catalog_stats_daily OWNER TO postgres;

--
-- Name: catalog_stats_monthly; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.catalog_stats_monthly (
    catalog_name public.catalog_name NOT NULL,
    grain text NOT NULL,
    ts timestamp with time zone NOT NULL,
    bytes_written_by_me bigint DEFAULT 0 NOT NULL,
    docs_written_by_me bigint DEFAULT 0 NOT NULL,
    bytes_read_by_me bigint DEFAULT 0 NOT NULL,
    docs_read_by_me bigint DEFAULT 0 NOT NULL,
    bytes_written_to_me bigint DEFAULT 0 NOT NULL,
    docs_written_to_me bigint DEFAULT 0 NOT NULL,
    bytes_read_from_me bigint DEFAULT 0 NOT NULL,
    docs_read_from_me bigint DEFAULT 0 NOT NULL,
    usage_seconds integer DEFAULT 0 NOT NULL,
    warnings integer DEFAULT 0 NOT NULL,
    errors integer DEFAULT 0 NOT NULL,
    failures integer DEFAULT 0 NOT NULL,
    flow_document json NOT NULL
);


ALTER TABLE public.catalog_stats_monthly OWNER TO postgres;

--
-- Name: new_free_trial_tenants; Type: VIEW; Schema: internal; Owner: postgres
--

CREATE VIEW internal.new_free_trial_tenants AS
 WITH hours_by_day AS (
         SELECT tenants_1.tenant,
            catalog_stats_daily.ts,
            sum(((catalog_stats_daily.usage_seconds)::numeric / (60.0 * (60)::numeric))) AS daily_usage_hours
           FROM (public.catalog_stats_daily
             JOIN public.tenants tenants_1 ON (((catalog_stats_daily.catalog_name)::text ^@ (tenants_1.tenant)::text)))
          WHERE (tenants_1.trial_start IS NULL)
          GROUP BY tenants_1.tenant, catalog_stats_daily.ts
         HAVING (sum(((catalog_stats_daily.usage_seconds)::numeric / (60.0 * (60)::numeric))) > (((2 * 24))::numeric * 1.1))
        ), hours_by_month AS (
         SELECT tenants_1.tenant,
            catalog_stats_monthly.ts,
            sum(((catalog_stats_monthly.usage_seconds)::numeric / (60.0 * (60)::numeric))) AS monthly_usage_hours
           FROM (public.catalog_stats_monthly
             JOIN public.tenants tenants_1 ON (((catalog_stats_monthly.catalog_name)::text ^@ (tenants_1.tenant)::text)))
          WHERE (tenants_1.trial_start IS NULL)
          GROUP BY tenants_1.tenant, catalog_stats_monthly.ts
         HAVING (sum(((catalog_stats_monthly.usage_seconds)::numeric / (60.0 * (60)::numeric))) > ((((24 * 31) * 2))::numeric * 1.1))
        ), gbs_by_month AS (
         SELECT tenants_1.tenant,
            catalog_stats_monthly.ts,
            ceil(sum((((catalog_stats_monthly.bytes_written_by_me + catalog_stats_monthly.bytes_read_by_me))::numeric / (10.0 ^ 9.0)))) AS monthly_usage_gbs
           FROM (public.catalog_stats_monthly
             JOIN public.tenants tenants_1 ON (((catalog_stats_monthly.catalog_name)::text ^@ (tenants_1.tenant)::text)))
          WHERE (tenants_1.trial_start IS NULL)
          GROUP BY tenants_1.tenant, catalog_stats_monthly.ts
         HAVING (ceil(sum((((catalog_stats_monthly.bytes_written_by_me + catalog_stats_monthly.bytes_read_by_me))::numeric / (10.0 ^ 9.0)))) > (10)::numeric)
        )
 SELECT tenants.tenant,
    max(hours_by_day.daily_usage_hours) AS max_daily_usage_hours,
    max(hours_by_month.monthly_usage_hours) AS max_monthly_usage_hours,
    max(gbs_by_month.monthly_usage_gbs) AS max_monthly_gb,
    count(DISTINCT live_specs.id) FILTER (WHERE (live_specs.spec_type = 'capture'::public.catalog_spec_type)) AS today_captures,
    count(DISTINCT live_specs.id) FILTER (WHERE (live_specs.spec_type = 'materialization'::public.catalog_spec_type)) AS today_materializations
   FROM ((((public.tenants
     LEFT JOIN hours_by_day ON (((hours_by_day.tenant)::text = (tenants.tenant)::text)))
     LEFT JOIN hours_by_month ON (((hours_by_month.tenant)::text = (tenants.tenant)::text)))
     LEFT JOIN gbs_by_month ON (((gbs_by_month.tenant)::text = (tenants.tenant)::text)))
     JOIN public.live_specs ON ((((split_part((live_specs.catalog_name)::text, '/'::text, 1) || '/'::text) = (tenants.tenant)::text) AND (((live_specs.spec #>> '{shards,disable}'::text[]))::boolean IS NOT TRUE))))
  WHERE (tenants.trial_start IS NULL)
  GROUP BY tenants.tenant
 HAVING ((count(hours_by_month.*) > 0) OR (count(hours_by_day.*) > 0) OR (count(gbs_by_month.*) > 0));


ALTER VIEW internal.new_free_trial_tenants OWNER TO postgres;

--
-- Name: next_auto_discovers; Type: VIEW; Schema: internal; Owner: postgres
--

CREATE VIEW internal.next_auto_discovers AS
SELECT
    NULL::public.flowid AS capture_id,
    NULL::public.catalog_name AS capture_name,
    NULL::json AS endpoint_json,
    NULL::boolean AS add_new_bindings,
    NULL::boolean AS evolve_incompatible_collections,
    NULL::public.flowid AS connector_tags_id,
    NULL::interval AS overdue_interval;


ALTER VIEW internal.next_auto_discovers OWNER TO postgres;

--
-- Name: VIEW next_auto_discovers; Type: COMMENT; Schema: internal; Owner: postgres
--

COMMENT ON VIEW internal.next_auto_discovers IS 'A view of captures that are due for an automatic discovery operation.
This is determined by comparing the time of the last discover operation
against the curent time';


--
-- Name: COLUMN next_auto_discovers.capture_id; Type: COMMENT; Schema: internal; Owner: postgres
--

COMMENT ON COLUMN internal.next_auto_discovers.capture_id IS 'Primary key of the live_specs row for the capture';


--
-- Name: COLUMN next_auto_discovers.capture_name; Type: COMMENT; Schema: internal; Owner: postgres
--

COMMENT ON COLUMN internal.next_auto_discovers.capture_name IS 'Catalog name of the capture';


--
-- Name: COLUMN next_auto_discovers.endpoint_json; Type: COMMENT; Schema: internal; Owner: postgres
--

COMMENT ON COLUMN internal.next_auto_discovers.endpoint_json IS 'The endpoint configuration of the capture, to use with the next discover.';


--
-- Name: COLUMN next_auto_discovers.add_new_bindings; Type: COMMENT; Schema: internal; Owner: postgres
--

COMMENT ON COLUMN internal.next_auto_discovers.add_new_bindings IS 'Whether to add newly discovered bindings. If false, then it will only update existing bindings.';


--
-- Name: COLUMN next_auto_discovers.evolve_incompatible_collections; Type: COMMENT; Schema: internal; Owner: postgres
--

COMMENT ON COLUMN internal.next_auto_discovers.evolve_incompatible_collections IS 'Whether to automatically perform schema evolution in the event that the newly discovered collections are incompatble.';


--
-- Name: COLUMN next_auto_discovers.connector_tags_id; Type: COMMENT; Schema: internal; Owner: postgres
--

COMMENT ON COLUMN internal.next_auto_discovers.connector_tags_id IS 'The id of the connector_tags row that corresponds to the image used by this capture.';


--
-- Name: shard_0_id_sequence; Type: SEQUENCE; Schema: internal; Owner: postgres
--

CREATE SEQUENCE internal.shard_0_id_sequence
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE internal.shard_0_id_sequence OWNER TO postgres;

--
-- Name: user_profiles; Type: VIEW; Schema: internal; Owner: postgres
--

CREATE VIEW internal.user_profiles AS
 SELECT users.id AS user_id,
    users.email,
    COALESCE((users.raw_user_meta_data ->> 'full_name'::text), (users.raw_user_meta_data ->> 'name'::text)) AS full_name,
    COALESCE((users.raw_user_meta_data ->> 'picture'::text), (users.raw_user_meta_data ->> 'avatar_url'::text)) AS avatar_url
   FROM auth.users;


ALTER VIEW internal.user_profiles OWNER TO postgres;

--
-- Name: alert_all; Type: VIEW; Schema: public; Owner: postgres
--

CREATE VIEW public.alert_all AS
 SELECT alert_free_trial.alert_type,
    alert_free_trial.catalog_name,
    alert_free_trial.arguments,
    alert_free_trial.firing
   FROM internal.alert_free_trial
UNION ALL
 SELECT alert_free_trial_ending.alert_type,
    alert_free_trial_ending.catalog_name,
    alert_free_trial_ending.arguments,
    alert_free_trial_ending.firing
   FROM internal.alert_free_trial_ending
UNION ALL
 SELECT alert_free_trial_stalled.alert_type,
    alert_free_trial_stalled.catalog_name,
    alert_free_trial_stalled.arguments,
    alert_free_trial_stalled.firing
   FROM internal.alert_free_trial_stalled
UNION ALL
 SELECT alert_missing_payment_method.alert_type,
    alert_missing_payment_method.catalog_name,
    alert_missing_payment_method.arguments,
    alert_missing_payment_method.firing
   FROM internal.alert_missing_payment_method
UNION ALL
 SELECT alert_data_movement_stalled.alert_type,
    alert_data_movement_stalled.catalog_name,
    alert_data_movement_stalled.arguments,
    alert_data_movement_stalled.firing
   FROM internal.alert_data_movement_stalled;


ALTER VIEW public.alert_all OWNER TO postgres;

--
-- Name: alert_history; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.alert_history (
    alert_type public.alert_type NOT NULL,
    catalog_name public.catalog_name NOT NULL,
    fired_at timestamp with time zone NOT NULL,
    resolved_at timestamp with time zone,
    arguments json NOT NULL,
    resolved_arguments jsonb
);


ALTER TABLE public.alert_history OWNER TO postgres;

--
-- Name: role_grants; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.role_grants (
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    detail text,
    id public.flowid NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    subject_role public.catalog_prefix NOT NULL,
    object_role public.catalog_prefix NOT NULL,
    capability public.grant_capability NOT NULL,
    CONSTRAINT valid_capability CHECK ((capability = ANY (ARRAY['read'::public.grant_capability, 'write'::public.grant_capability, 'admin'::public.grant_capability])))
);


ALTER TABLE public.role_grants OWNER TO postgres;

--
-- Name: TABLE role_grants; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON TABLE public.role_grants IS 'Roles and capabilities that roles have been granted to other roles';


--
-- Name: COLUMN role_grants.created_at; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.role_grants.created_at IS 'Time at which the record was created';


--
-- Name: COLUMN role_grants.detail; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.role_grants.detail IS 'Description of the record';


--
-- Name: COLUMN role_grants.id; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.role_grants.id IS 'ID of the record';


--
-- Name: COLUMN role_grants.updated_at; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.role_grants.updated_at IS 'Time at which the record was last updated';


--
-- Name: COLUMN role_grants.subject_role; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.role_grants.subject_role IS 'Role which has been granted a capability to another role';


--
-- Name: COLUMN role_grants.object_role; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.role_grants.object_role IS 'Role to which a capability has been granted';


--
-- Name: COLUMN role_grants.capability; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.role_grants.capability IS 'Capability which is granted to the subject role';


--
-- Name: user_grants; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.user_grants (
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    detail text,
    id public.flowid NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    user_id uuid NOT NULL,
    object_role public.catalog_prefix NOT NULL,
    capability public.grant_capability NOT NULL,
    CONSTRAINT valid_capability CHECK ((capability = ANY (ARRAY['read'::public.grant_capability, 'write'::public.grant_capability, 'admin'::public.grant_capability])))
);


ALTER TABLE public.user_grants OWNER TO postgres;

--
-- Name: TABLE user_grants; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON TABLE public.user_grants IS 'Roles and capabilities that the user has been granted';


--
-- Name: COLUMN user_grants.created_at; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.user_grants.created_at IS 'Time at which the record was created';


--
-- Name: COLUMN user_grants.detail; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.user_grants.detail IS 'Description of the record';


--
-- Name: COLUMN user_grants.id; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.user_grants.id IS 'ID of the record';


--
-- Name: COLUMN user_grants.updated_at; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.user_grants.updated_at IS 'Time at which the record was last updated';


--
-- Name: COLUMN user_grants.user_id; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.user_grants.user_id IS 'User who has been granted a role';


--
-- Name: COLUMN user_grants.object_role; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.user_grants.object_role IS 'Role which is granted to the user';


--
-- Name: COLUMN user_grants.capability; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.user_grants.capability IS 'Capability which is granted to the user';


--
-- Name: combined_grants_ext; Type: VIEW; Schema: public; Owner: postgres
--

CREATE VIEW public.combined_grants_ext AS
 WITH admin_roles AS (
         SELECT auth_roles.role_prefix
           FROM public.auth_roles('admin'::public.grant_capability) auth_roles(role_prefix, capability)
        ), user_id(id) AS (
         SELECT auth.uid() AS uid
        )
 SELECT g.capability,
    g.created_at,
    g.detail,
    g.id,
    g.object_role,
    g.updated_at,
    g.subject_role,
    NULL::text AS user_avatar_url,
    NULL::character varying AS user_email,
    NULL::text AS user_full_name,
    NULL::uuid AS user_id
   FROM public.role_grants g
  WHERE ((g.id)::macaddr8 IN ( SELECT g_1.id
           FROM admin_roles r,
            public.role_grants g_1
          WHERE (((g_1.subject_role)::text ^@ (r.role_prefix)::text) OR ((g_1.object_role)::text ^@ (r.role_prefix)::text))))
UNION ALL
 SELECT g.capability,
    g.created_at,
    g.detail,
    g.id,
    g.object_role,
    g.updated_at,
    NULL::text AS subject_role,
    u.avatar_url AS user_avatar_url,
    u.email AS user_email,
    u.full_name AS user_full_name,
    g.user_id
   FROM (public.user_grants g
     LEFT JOIN internal.user_profiles u ON ((u.user_id = g.user_id)))
  WHERE ((g.id)::macaddr8 IN ( SELECT g_1.id
           FROM admin_roles r,
            public.user_grants g_1
          WHERE ((g_1.user_id = ( SELECT user_id.id
                   FROM user_id)) OR ((g_1.object_role)::text ^@ (r.role_prefix)::text))));


ALTER VIEW public.combined_grants_ext OWNER TO postgres;

--
-- Name: VIEW combined_grants_ext; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON VIEW public.combined_grants_ext IS 'Combined view of `role_grants` and `user_grants` extended with user metadata';


--
-- Name: connector_tags; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.connector_tags (
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    detail text,
    id public.flowid NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    job_status public.jsonb_obj DEFAULT '{"type": "queued"}'::jsonb NOT NULL,
    logs_token uuid DEFAULT gen_random_uuid() NOT NULL,
    connector_id public.flowid NOT NULL,
    documentation_url text,
    endpoint_spec_schema public.json_obj,
    image_tag text NOT NULL,
    protocol text,
    resource_spec_schema public.json_obj,
    auto_discover_interval interval DEFAULT '02:00:00'::interval NOT NULL,
    resource_path_pointers public.json_pointer[],
    background boolean DEFAULT false NOT NULL,
    default_capture_interval interval,
    disable_backfill boolean DEFAULT false NOT NULL,
    CONSTRAINT connector_tags_resource_path_pointers_check CHECK ((array_length(resource_path_pointers, 1) > 0)),
    CONSTRAINT "image_tag must start with : (as in :latest) or @sha256:<hash>" CHECK (((image_tag ~~ ':%'::text) OR (image_tag ~~ '@sha256:'::text)))
);


ALTER TABLE public.connector_tags OWNER TO postgres;

--
-- Name: TABLE connector_tags; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON TABLE public.connector_tags IS '
Available image tags (versions) of connectors.
Tags are _typically_ immutable versions,
but it''s possible to update the image digest backing a tag,
which is arguably a different version.
';


--
-- Name: COLUMN connector_tags.created_at; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.connector_tags.created_at IS 'Time at which the record was created';


--
-- Name: COLUMN connector_tags.detail; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.connector_tags.detail IS 'Description of the record';


--
-- Name: COLUMN connector_tags.id; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.connector_tags.id IS 'ID of the record';


--
-- Name: COLUMN connector_tags.updated_at; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.connector_tags.updated_at IS 'Time at which the record was last updated';


--
-- Name: COLUMN connector_tags.job_status; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.connector_tags.job_status IS 'Server-side job executation status of the record';


--
-- Name: COLUMN connector_tags.logs_token; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.connector_tags.logs_token IS 'Bearer token for accessing logs of the server-side operation';


--
-- Name: COLUMN connector_tags.connector_id; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.connector_tags.connector_id IS 'Connector which this record is a tag of';


--
-- Name: COLUMN connector_tags.documentation_url; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.connector_tags.documentation_url IS 'Documentation URL of the tagged connector';


--
-- Name: COLUMN connector_tags.endpoint_spec_schema; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.connector_tags.endpoint_spec_schema IS 'Endpoint specification JSON-Schema of the tagged connector';


--
-- Name: COLUMN connector_tags.image_tag; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.connector_tags.image_tag IS 'Image tag, in either ":v1.2.3", ":latest", or "@sha256:<a-sha256>" form';


--
-- Name: COLUMN connector_tags.protocol; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.connector_tags.protocol IS 'Protocol of the connector';


--
-- Name: COLUMN connector_tags.resource_spec_schema; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.connector_tags.resource_spec_schema IS 'Resource specification JSON-Schema of the tagged connector';


--
-- Name: COLUMN connector_tags.auto_discover_interval; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.connector_tags.auto_discover_interval IS 'Frequency at which to perform automatic discovery operations for captures, when autoDiscover is enabled';


--
-- Name: COLUMN connector_tags.resource_path_pointers; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.connector_tags.resource_path_pointers IS 'The resource_path that was returned from the connector spec response';


--
-- Name: COLUMN connector_tags.background; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.connector_tags.background IS 'indicates whether this is a background job, which will be processed with a lower priority than interactive jobs';


--
-- Name: COLUMN connector_tags.default_capture_interval; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.connector_tags.default_capture_interval IS 'The default value for the interval property for a Capture. This is normally used for non-streaming connectors';


--
-- Name: COLUMN connector_tags.disable_backfill; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.connector_tags.disable_backfill IS 'Controls if the UI will hide the backfill button for a connector';


--
-- Name: connectors; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.connectors (
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    detail text,
    id public.flowid NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    external_url text NOT NULL,
    image_name text NOT NULL,
    oauth2_client_id text,
    oauth2_client_secret text,
    oauth2_spec public.jsonb_obj,
    oauth2_injected_values public.jsonb_obj,
    short_description public.jsonb_internationalized_value,
    title public.jsonb_internationalized_value,
    logo_url public.jsonb_internationalized_value,
    recommended boolean NOT NULL,
    long_description public.jsonb_internationalized_value,
    CONSTRAINT "image_name must be a container image without a tag" CHECK ((image_name ~ '^(?:.+/)?([^:]+)$'::text))
);


ALTER TABLE public.connectors OWNER TO postgres;

--
-- Name: TABLE connectors; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON TABLE public.connectors IS '
Connectors are Docker / OCI images which implement a standard protocol,
and allow Flow to interface with an external system for the capture
or materialization of data.
';


--
-- Name: COLUMN connectors.created_at; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.connectors.created_at IS 'Time at which the record was created';


--
-- Name: COLUMN connectors.detail; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.connectors.detail IS 'Description of the record';


--
-- Name: COLUMN connectors.id; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.connectors.id IS 'ID of the record';


--
-- Name: COLUMN connectors.updated_at; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.connectors.updated_at IS 'Time at which the record was last updated';


--
-- Name: COLUMN connectors.external_url; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.connectors.external_url IS 'External URL which provides more information about the endpoint';


--
-- Name: COLUMN connectors.image_name; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.connectors.image_name IS 'Name of the connector''s container (Docker) image, for example "ghcr.io/estuary/source-postgres"';


--
-- Name: COLUMN connectors.oauth2_client_id; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.connectors.oauth2_client_id IS 'oauth client id';


--
-- Name: COLUMN connectors.oauth2_client_secret; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.connectors.oauth2_client_secret IS 'oauth client secret';


--
-- Name: COLUMN connectors.oauth2_spec; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.connectors.oauth2_spec IS 'OAuth2 specification of the connector';


--
-- Name: COLUMN connectors.oauth2_injected_values; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.connectors.oauth2_injected_values IS 'oauth additional injected values, these values will be made available in the credentials key of the connector, as well as when rendering oauth2_spec templates';


--
-- Name: COLUMN connectors.short_description; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.connectors.short_description IS 'A short description of this connector, at most a few sentences. Represented as a json object with IETF language tags as keys (https://en.wikipedia.org/wiki/IETF_language_tag), and the description string as values';


--
-- Name: COLUMN connectors.title; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.connectors.title IS 'The title of this connector. Represented as a json object with IETF language tags as keys (https://en.wikipedia.org/wiki/IETF_language_tag), and the title string as values';


--
-- Name: COLUMN connectors.logo_url; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.connectors.logo_url IS 'The url for this connector''s logo image. Represented as a json object with IETF language tags as keys (https://en.wikipedia.org/wiki/IETF_language_tag), and urls as values';


--
-- Name: COLUMN connectors.long_description; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.connectors.long_description IS 'A longform description of this connector. Represented as a json object with IETF language tags as keys (https://en.wikipedia.org/wiki/IETF_language_tag), and the description string as values';


--
-- Name: controller_jobs; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.controller_jobs (
    live_spec_id public.flowid NOT NULL,
    controller_version integer DEFAULT 0 NOT NULL,
    status json DEFAULT '{}'::json NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    logs_token uuid DEFAULT gen_random_uuid() NOT NULL,
    failures integer DEFAULT 0 NOT NULL,
    error text
);


ALTER TABLE public.controller_jobs OWNER TO postgres;

--
-- Name: TABLE controller_jobs; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON TABLE public.controller_jobs IS 'Controller jobs reflect the state of the automated background processes that
  manage live specs. Controllers are responsible for things like updating
  inferred schemas, activating and deleting shard and journal specs in the data
  plane, and any other type of background automation.';


--
-- Name: COLUMN controller_jobs.live_spec_id; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.controller_jobs.live_spec_id IS 'The id of the live_specs row that this contoller job pertains to.';


--
-- Name: COLUMN controller_jobs.controller_version; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.controller_jobs.controller_version IS 'The version of the controller that last ran. This number only increases
  monotonically, and only when a breaking change to the controller status
  is released. Every controller_job starts out with a controller_version of 0,
  and will subsequently be upgraded to the current controller version by the
  first controller run.';


--
-- Name: COLUMN controller_jobs.status; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.controller_jobs.status IS 'Contains type-specific information about the controller and the actions it
  has performed.';


--
-- Name: COLUMN controller_jobs.updated_at; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.controller_jobs.updated_at IS 'Timestamp of the last update to the controller_job.';


--
-- Name: COLUMN controller_jobs.logs_token; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.controller_jobs.logs_token IS 'Token that can be used to query logs from controller runs from
  internal.log_lines.';


--
-- Name: COLUMN controller_jobs.failures; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.controller_jobs.failures IS 'Count of consecutive failures of this controller. This is reset to 0 upon
  any successful controller run. If failures is > 0, then error will be set';


--
-- Name: COLUMN controller_jobs.error; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.controller_jobs.error IS 'The error from the most recent controller run, which will be null if the
  run was successful. If this is set, then failures will be > 0';


--
-- Name: data_planes; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.data_planes (
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    detail text,
    id public.flowid NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    data_plane_name public.catalog_name NOT NULL,
    data_plane_fqdn text NOT NULL,
    ops_logs_name public.catalog_name NOT NULL,
    ops_stats_name public.catalog_name NOT NULL,
    ops_l1_inferred_name public.catalog_name NOT NULL,
    ops_l1_stats_name public.catalog_name NOT NULL,
    ops_l2_inferred_transform text NOT NULL,
    ops_l2_stats_transform text NOT NULL,
    broker_address text NOT NULL,
    reactor_address text NOT NULL,
    config json DEFAULT '{}'::json NOT NULL,
    status json DEFAULT '{}'::json NOT NULL,
    logs_token uuid DEFAULT gen_random_uuid() NOT NULL,
    hmac_keys text[] NOT NULL,
    aws_iam_user_arn text,
    cidr_blocks cidr[] DEFAULT '{}'::cidr[] NOT NULL,
    enable_l2 boolean NOT NULL,
    gcp_service_account_email text,
    ssh_private_key text
);


ALTER TABLE public.data_planes OWNER TO postgres;

--
-- Name: COLUMN data_planes.created_at; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.data_planes.created_at IS 'Time at which the record was created';


--
-- Name: COLUMN data_planes.detail; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.data_planes.detail IS 'Description of the record';


--
-- Name: COLUMN data_planes.id; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.data_planes.id IS 'ID of the record';


--
-- Name: COLUMN data_planes.updated_at; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.data_planes.updated_at IS 'Time at which the record was last updated';


--
-- Name: discovers; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.discovers (
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    detail text,
    id public.flowid NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    job_status public.jsonb_obj DEFAULT '{"type": "queued"}'::jsonb NOT NULL,
    logs_token uuid DEFAULT gen_random_uuid() NOT NULL,
    capture_name public.catalog_name NOT NULL,
    connector_tag_id public.flowid NOT NULL,
    draft_id public.flowid NOT NULL,
    endpoint_config public.json_obj NOT NULL,
    update_only boolean DEFAULT false NOT NULL,
    auto_publish boolean DEFAULT false NOT NULL,
    auto_evolve boolean DEFAULT false NOT NULL,
    background boolean DEFAULT false NOT NULL,
    data_plane_name text DEFAULT 'ops/dp/public/gcp-us-central1-c1'::text NOT NULL
);


ALTER TABLE public.discovers OWNER TO postgres;

--
-- Name: TABLE discovers; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON TABLE public.discovers IS 'User-initiated connector discovery operations';


--
-- Name: COLUMN discovers.created_at; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.discovers.created_at IS 'Time at which the record was created';


--
-- Name: COLUMN discovers.detail; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.discovers.detail IS 'Description of the record';


--
-- Name: COLUMN discovers.id; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.discovers.id IS 'ID of the record';


--
-- Name: COLUMN discovers.updated_at; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.discovers.updated_at IS 'Time at which the record was last updated';


--
-- Name: COLUMN discovers.job_status; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.discovers.job_status IS 'Server-side job executation status of the record';


--
-- Name: COLUMN discovers.logs_token; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.discovers.logs_token IS 'Bearer token for accessing logs of the server-side operation';


--
-- Name: COLUMN discovers.capture_name; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.discovers.capture_name IS 'Intended name of the capture produced by this discover';


--
-- Name: COLUMN discovers.connector_tag_id; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.discovers.connector_tag_id IS 'Tagged connector which is used for discovery';


--
-- Name: COLUMN discovers.draft_id; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.discovers.draft_id IS 'Draft to be populated by this discovery operation';


--
-- Name: COLUMN discovers.endpoint_config; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.discovers.endpoint_config IS 'Endpoint configuration of the connector. May be protected by sops';


--
-- Name: COLUMN discovers.update_only; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.discovers.update_only IS '
If true, this operation will draft updates to existing bindings and their
target collections but will not add new bindings or collections.';


--
-- Name: COLUMN discovers.auto_publish; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.discovers.auto_publish IS 'whether to automatically publish the results of the discover, if successful';


--
-- Name: COLUMN discovers.auto_evolve; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.discovers.auto_evolve IS 'whether to automatically create an evolutions job if the automatic publication
fails due to incompatible collection schemas. This determines the value of `auto_evolve`
in the publications table when inserting a new row as a result of this discover.';


--
-- Name: COLUMN discovers.background; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.discovers.background IS 'indicates whether this is a background job, which will be processed with a lower priority than interactive jobs';


--
-- Name: draft_errors; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.draft_errors (
    draft_id public.flowid NOT NULL,
    scope text NOT NULL,
    detail text NOT NULL
);


ALTER TABLE public.draft_errors OWNER TO postgres;

--
-- Name: TABLE draft_errors; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON TABLE public.draft_errors IS 'Errors found while validating, testing or publishing a user draft';


--
-- Name: COLUMN draft_errors.draft_id; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.draft_errors.draft_id IS 'Draft which produed this error';


--
-- Name: COLUMN draft_errors.scope; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.draft_errors.scope IS 'Location scope of the error within the draft';


--
-- Name: COLUMN draft_errors.detail; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.draft_errors.detail IS 'Description of the error';


--
-- Name: draft_specs; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.draft_specs (
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    detail text,
    id public.flowid NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    draft_id public.flowid NOT NULL,
    catalog_name public.catalog_name NOT NULL,
    expect_pub_id public.flowid DEFAULT NULL::macaddr8,
    spec json,
    spec_type public.catalog_spec_type,
    built_spec json,
    validated json
);


ALTER TABLE public.draft_specs OWNER TO postgres;

--
-- Name: TABLE draft_specs; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON TABLE public.draft_specs IS 'Proposed catalog specifications of a draft';


--
-- Name: COLUMN draft_specs.created_at; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.draft_specs.created_at IS 'Time at which the record was created';


--
-- Name: COLUMN draft_specs.detail; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.draft_specs.detail IS 'Description of the record';


--
-- Name: COLUMN draft_specs.id; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.draft_specs.id IS 'ID of the record';


--
-- Name: COLUMN draft_specs.updated_at; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.draft_specs.updated_at IS 'Time at which the record was last updated';


--
-- Name: COLUMN draft_specs.draft_id; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.draft_specs.draft_id IS 'Draft which this specification belongs to';


--
-- Name: COLUMN draft_specs.catalog_name; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.draft_specs.catalog_name IS 'Catalog name of this specification';


--
-- Name: COLUMN draft_specs.expect_pub_id; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.draft_specs.expect_pub_id IS '
Draft specifications may be drawn from a current live specification,
and in this case it''s recommended that expect_pub_id is also set to the
last_pub_id of that inititializing live specification.

Or if there isn''t expected to be a live specification then
expect_pub_id can be the set to an explicit value of ''00:00:00:00:00:00:00:00''
to represent that no live specification is expected to exist.

Then when this draft is published, the publication will fail if the now-current
live specification has a different last_pub_id. This prevents inadvertent errors
where two users attempt to modify or create a catalog specification at the same time,
as the second user publication will fail rather than silently overwriting changes
made by the first user.

When NULL, expect_pub_id has no effect.
';


--
-- Name: COLUMN draft_specs.spec; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.draft_specs.spec IS '
Spec is a serialized catalog specification. Its schema depends on its spec_type:
either CollectionDef, CaptureDef, MaterializationDef, DerivationDef,
or an array of TestStep from the Flow catalog schema.

It may also be NULL, in which case `spec_type` must also be NULL
and the specification will be deleted when this draft is published.
';


--
-- Name: COLUMN draft_specs.spec_type; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.draft_specs.spec_type IS 'Type of this draft catalog specification';


--
-- Name: COLUMN draft_specs.built_spec; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.draft_specs.built_spec IS 'Built specification for this catalog';


--
-- Name: COLUMN draft_specs.validated; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.draft_specs.validated IS 'Serialized response from the connector Validate RPC as populated by a dry run of this draft specification';


--
-- Name: drafts; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.drafts (
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    detail text,
    id public.flowid NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    user_id uuid DEFAULT auth.uid() NOT NULL
);


ALTER TABLE public.drafts OWNER TO postgres;

--
-- Name: TABLE drafts; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON TABLE public.drafts IS 'Draft change-sets of Flow catalog specifications';


--
-- Name: COLUMN drafts.created_at; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.drafts.created_at IS 'Time at which the record was created';


--
-- Name: COLUMN drafts.detail; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.drafts.detail IS 'Description of the record';


--
-- Name: COLUMN drafts.id; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.drafts.id IS 'ID of the record';


--
-- Name: COLUMN drafts.updated_at; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.drafts.updated_at IS 'Time at which the record was last updated';


--
-- Name: COLUMN drafts.user_id; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.drafts.user_id IS 'User who owns this draft';


--
-- Name: inferred_schemas; Type: TABLE; Schema: public; Owner: stats_loader
--

CREATE TABLE public.inferred_schemas (
    collection_name public.catalog_name NOT NULL,
    schema json NOT NULL,
    flow_document json NOT NULL,
    md5 text GENERATED ALWAYS AS (md5(TRIM(BOTH FROM (schema)::text))) STORED
);


ALTER TABLE public.inferred_schemas OWNER TO stats_loader;

--
-- Name: TABLE inferred_schemas; Type: COMMENT; Schema: public; Owner: stats_loader
--

COMMENT ON TABLE public.inferred_schemas IS 'Generated for materialization ops.us-central1.v1/stats-view of collection ops.us-central1.v1/inferred-schemas/L2';


--
-- Name: COLUMN inferred_schemas.collection_name; Type: COMMENT; Schema: public; Owner: stats_loader
--

COMMENT ON COLUMN public.inferred_schemas.collection_name IS 'The name of the collection that this schema was inferred for
auto-generated projection of JSON at: /collection_name with inferred types: [string]';


--
-- Name: COLUMN inferred_schemas.schema; Type: COMMENT; Schema: public; Owner: stats_loader
--

COMMENT ON COLUMN public.inferred_schemas.schema IS 'The inferred schema
auto-generated projection of JSON at: /schema with inferred types: [object]';


--
-- Name: COLUMN inferred_schemas.flow_document; Type: COMMENT; Schema: public; Owner: stats_loader
--

COMMENT ON COLUMN public.inferred_schemas.flow_document IS 'auto-generated projection of JSON at:  with inferred types: [object]';


--
-- Name: COLUMN inferred_schemas.md5; Type: COMMENT; Schema: public; Owner: stats_loader
--

COMMENT ON COLUMN public.inferred_schemas.md5 IS 'hash of the inferred schema json, which is used to identify changes';


--
-- Name: publication_specs; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.publication_specs (
    live_spec_id public.flowid NOT NULL,
    pub_id public.flowid NOT NULL,
    detail text,
    published_at timestamp with time zone DEFAULT now() NOT NULL,
    spec json,
    spec_type public.catalog_spec_type,
    user_id uuid DEFAULT auth.uid() NOT NULL
);


ALTER TABLE public.publication_specs OWNER TO postgres;

--
-- Name: TABLE publication_specs; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON TABLE public.publication_specs IS '
publication_specs details the publication history of the `live_specs` catalog.
Each change to a live specification is recorded into `publication_specs`.
';


--
-- Name: COLUMN publication_specs.live_spec_id; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.publication_specs.live_spec_id IS 'Live catalog specification which was published';


--
-- Name: COLUMN publication_specs.pub_id; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.publication_specs.pub_id IS 'Publication ID which published to the catalog specification';


--
-- Name: COLUMN publication_specs.spec; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.publication_specs.spec IS '
Catalog specification which was published by this publication,
or NULL if this was a deletion.
';


--
-- Name: COLUMN publication_specs.spec_type; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.publication_specs.spec_type IS 'Type of the published catalog specification, or NULL if this was a deletion';


--
-- Name: COLUMN publication_specs.user_id; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.publication_specs.user_id IS 'User who performed this publication.';


--
-- Name: live_specs_ext; Type: VIEW; Schema: public; Owner: postgres
--

CREATE VIEW public.live_specs_ext AS
 WITH authorized_specs AS (
         SELECT l_1.id
           FROM public.auth_roles('read'::public.grant_capability) r(role_prefix, capability),
            public.live_specs l_1
          WHERE ((l_1.catalog_name)::text ^@ (r.role_prefix)::text)
        )
 SELECT l.created_at,
    l.detail,
    l.id,
    l.updated_at,
    l.catalog_name,
    l.connector_image_name,
    l.connector_image_tag,
    l.last_pub_id,
    l.reads_from,
    l.spec,
    l.spec_type,
    l.writes_to,
    l.last_build_id,
    l.md5,
    l.built_spec,
    l.inferred_schema_md5,
    l.controller_next_run,
    c.external_url AS connector_external_url,
    c.id AS connector_id,
    c.title AS connector_title,
    c.short_description AS connector_short_description,
    c.logo_url AS connector_logo_url,
    c.recommended AS connector_recommended,
    t.id AS connector_tag_id,
    t.documentation_url AS connector_tag_documentation_url,
    p.detail AS last_pub_detail,
    p.user_id AS last_pub_user_id,
    u.avatar_url AS last_pub_user_avatar_url,
    u.email AS last_pub_user_email,
    u.full_name AS last_pub_user_full_name,
    l.journal_template_name,
    l.shard_template_id,
    l.data_plane_id,
    d.broker_address,
    d.data_plane_name,
    d.reactor_address
   FROM (((((public.live_specs l
     LEFT JOIN public.publication_specs p ON ((((l.id)::macaddr8 = (p.live_spec_id)::macaddr8) AND ((l.last_pub_id)::macaddr8 = (p.pub_id)::macaddr8))))
     LEFT JOIN public.connectors c ON ((c.image_name = l.connector_image_name)))
     LEFT JOIN public.connector_tags t ON ((((c.id)::macaddr8 = (t.connector_id)::macaddr8) AND (l.connector_image_tag = t.image_tag))))
     LEFT JOIN internal.user_profiles u ON ((u.user_id = p.user_id)))
     LEFT JOIN public.data_planes d ON (((d.id)::macaddr8 = (l.data_plane_id)::macaddr8)))
  WHERE ((EXISTS ( SELECT 1
           FROM pg_roles
          WHERE ((pg_roles.rolname = CURRENT_ROLE) AND (pg_roles.rolbypassrls = true)))) OR ((l.id)::macaddr8 IN ( SELECT authorized_specs.id
           FROM authorized_specs)));


ALTER VIEW public.live_specs_ext OWNER TO postgres;

--
-- Name: VIEW live_specs_ext; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON VIEW public.live_specs_ext IS 'View of `live_specs` extended with metadata of its last publication';


--
-- Name: draft_specs_ext; Type: VIEW; Schema: public; Owner: postgres
--

CREATE VIEW public.draft_specs_ext AS
 WITH authorized_drafts AS (
         SELECT drafts.id
           FROM public.drafts
          WHERE (drafts.user_id = ( SELECT auth.uid() AS uid))
        )
 SELECT d.created_at,
    d.detail,
    d.id,
    d.updated_at,
    d.draft_id,
    d.catalog_name,
    d.expect_pub_id,
    d.spec,
    d.spec_type,
    d.built_spec,
    d.validated,
    l.last_pub_detail,
    l.last_pub_id,
    l.last_pub_user_id,
    l.last_pub_user_avatar_url,
    l.last_pub_user_email,
    l.last_pub_user_full_name,
    l.spec AS live_spec,
    l.spec_type AS live_spec_type,
    s.md5 AS inferred_schema_md5,
    l.inferred_schema_md5 AS live_inferred_schema_md5,
    l.md5 AS live_spec_md5,
    md5(TRIM(BOTH FROM (d.spec)::text)) AS draft_spec_md5
   FROM ((public.draft_specs d
     LEFT JOIN public.live_specs_ext l ON (((d.catalog_name)::text = (l.catalog_name)::text)))
     LEFT JOIN public.inferred_schemas s ON (((s.collection_name)::text = (l.catalog_name)::text)))
  WHERE ((EXISTS ( SELECT 1
           FROM pg_roles
          WHERE ((pg_roles.rolname = CURRENT_ROLE) AND (pg_roles.rolbypassrls = true)))) OR ((d.draft_id)::macaddr8 IN ( SELECT authorized_drafts.id
           FROM authorized_drafts)));


ALTER VIEW public.draft_specs_ext OWNER TO postgres;

--
-- Name: VIEW draft_specs_ext; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON VIEW public.draft_specs_ext IS 'View of `draft_specs` extended with metadata of its live specification';


--
-- Name: drafts_ext; Type: VIEW; Schema: public; Owner: authenticated
--

CREATE VIEW public.drafts_ext AS
 SELECT d.created_at,
    d.detail,
    d.id,
    d.updated_at,
    d.user_id,
    s.num_specs
   FROM public.drafts d,
    LATERAL ( SELECT count(*) AS num_specs
           FROM public.draft_specs
          WHERE ((draft_specs.draft_id)::macaddr8 = (d.id)::macaddr8)) s;


ALTER VIEW public.drafts_ext OWNER TO authenticated;

--
-- Name: VIEW drafts_ext; Type: COMMENT; Schema: public; Owner: authenticated
--

COMMENT ON VIEW public.drafts_ext IS 'View of `drafts` extended with metadata of its specifications';


--
-- Name: evolutions; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.evolutions (
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    detail text,
    id public.flowid NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    job_status public.jsonb_obj DEFAULT '{"type": "queued"}'::jsonb NOT NULL,
    logs_token uuid DEFAULT gen_random_uuid() NOT NULL,
    user_id uuid DEFAULT auth.uid() NOT NULL,
    draft_id public.flowid NOT NULL,
    collections json NOT NULL,
    auto_publish boolean DEFAULT false NOT NULL,
    background boolean DEFAULT false NOT NULL,
    CONSTRAINT evolutions_collections_check CHECK ((json_typeof(collections) = 'array'::text))
);


ALTER TABLE public.evolutions OWNER TO postgres;

--
-- Name: TABLE evolutions; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON TABLE public.evolutions IS 'evolutions are operations which test and publish drafts into live specifications';


--
-- Name: COLUMN evolutions.created_at; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.evolutions.created_at IS 'Time at which the record was created';


--
-- Name: COLUMN evolutions.detail; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.evolutions.detail IS 'Description of the record';


--
-- Name: COLUMN evolutions.id; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.evolutions.id IS 'ID of the record';


--
-- Name: COLUMN evolutions.updated_at; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.evolutions.updated_at IS 'Time at which the record was last updated';


--
-- Name: COLUMN evolutions.job_status; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.evolutions.job_status IS 'Server-side job executation status of the record';


--
-- Name: COLUMN evolutions.logs_token; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.evolutions.logs_token IS 'Bearer token for accessing logs of the server-side operation';


--
-- Name: COLUMN evolutions.user_id; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.evolutions.user_id IS 'User who created the evolution';


--
-- Name: COLUMN evolutions.draft_id; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.evolutions.draft_id IS 'Draft that is updated to affect the re-creation of the collections';


--
-- Name: COLUMN evolutions.collections; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.evolutions.collections IS 'The names of the collections to re-create';


--
-- Name: COLUMN evolutions.auto_publish; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.evolutions.auto_publish IS 'whether to automatically publish the results of the evolution, if successful';


--
-- Name: COLUMN evolutions.background; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.evolutions.background IS 'indicates whether this is a background job, which will be processed with a lower priority than interactive jobs';


--
-- Name: flow_watermarks; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.flow_watermarks (
    slot text NOT NULL,
    watermark text
);


ALTER TABLE public.flow_watermarks OWNER TO postgres;

--
-- Name: invoices_ext; Type: VIEW; Schema: public; Owner: postgres
--

CREATE VIEW public.invoices_ext AS
 WITH has_bypassrls AS (
         SELECT (EXISTS ( SELECT 1
                   FROM pg_roles
                  WHERE ((pg_roles.rolname = CURRENT_ROLE) AND (pg_roles.rolbypassrls = true)))) AS bypass
        ), authorized_tenants AS (
         SELECT tenants.tenant,
            tenants.created_at
           FROM ((public.tenants
             LEFT JOIN has_bypassrls ON (true))
             LEFT JOIN public.auth_roles('admin'::public.grant_capability) auth_roles(role_prefix, capability) ON (((tenants.tenant)::text ^@ (auth_roles.role_prefix)::text)))
          WHERE (has_bypassrls.bypass OR (auth_roles.role_prefix IS NOT NULL))
        )
 SELECT (date_trunc('month'::text, ((h.report ->> 'billed_month'::text))::timestamp with time zone))::date AS date_start,
    (((date_trunc('month'::text, ((h.report ->> 'billed_month'::text))::timestamp with time zone) + '1 mon'::interval) - '1 day'::interval))::date AS date_end,
    (authorized_tenants.tenant)::text AS billed_prefix,
    COALESCE(NULLIF((h.report -> 'line_items'::text), 'null'::jsonb), '[]'::jsonb) AS line_items,
    (COALESCE(NULLIF((h.report -> 'subtotal'::text), 'null'::jsonb), to_jsonb(0)))::integer AS subtotal,
    h.report AS extra,
    'final'::text AS invoice_type
   FROM (internal.billing_historicals h
     JOIN authorized_tenants ON (((h.tenant)::text ^@ (authorized_tenants.tenant)::text)))
UNION ALL
 SELECT (date_trunc('month'::text, ((report.report ->> 'billed_month'::text))::timestamp with time zone))::date AS date_start,
    (((date_trunc('month'::text, ((report.report ->> 'billed_month'::text))::timestamp with time zone) + '1 mon'::interval) - '1 day'::interval))::date AS date_end,
    (authorized_tenants.tenant)::text AS billed_prefix,
    COALESCE(NULLIF((report.report -> 'line_items'::text), 'null'::jsonb), '[]'::jsonb) AS line_items,
    (COALESCE(NULLIF((report.report -> 'subtotal'::text), 'null'::jsonb), to_jsonb(0)))::integer AS subtotal,
    report.report AS extra,
    'preview'::text AS invoice_type
   FROM ((authorized_tenants
     JOIN LATERAL generate_series((GREATEST('2023-08-01'::date, (date_trunc('month'::text, authorized_tenants.created_at))::date))::timestamp with time zone, date_trunc('month'::text, ((now())::date)::timestamp with time zone), '1 mon'::interval) invoice_month(invoice_month) ON ((NOT (EXISTS ( SELECT 1
           FROM internal.billing_historicals h
          WHERE (((h.tenant)::text ^@ (authorized_tenants.tenant)::text) AND ((date_trunc('month'::text, ((h.report ->> 'billed_month'::text))::timestamp with time zone))::date = invoice_month.invoice_month)))))))
     JOIN LATERAL internal.billing_report_202308((authorized_tenants.tenant)::public.catalog_prefix, invoice_month.invoice_month) report(report) ON (true))
UNION ALL
 SELECT manual_bills.date_start,
    manual_bills.date_end,
    (authorized_tenants.tenant)::text AS billed_prefix,
    jsonb_build_array(jsonb_build_object('description', manual_bills.description, 'count', 1, 'rate', manual_bills.usd_cents, 'subtotal', manual_bills.usd_cents)) AS line_items,
    manual_bills.usd_cents AS subtotal,
    'null'::jsonb AS extra,
    'manual'::text AS invoice_type
   FROM (internal.manual_bills
     JOIN authorized_tenants ON (((manual_bills.tenant)::text ^@ (authorized_tenants.tenant)::text)));


ALTER VIEW public.invoices_ext OWNER TO postgres;

--
-- Name: live_spec_flows; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.live_spec_flows (
    source_id public.flowid NOT NULL,
    target_id public.flowid NOT NULL,
    flow_type public.flow_type NOT NULL
);


ALTER TABLE public.live_spec_flows OWNER TO postgres;

--
-- Name: TABLE live_spec_flows; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON TABLE public.live_spec_flows IS 'Join table of directed data-flows between live specifications';


--
-- Name: COLUMN live_spec_flows.source_id; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.live_spec_flows.source_id IS 'Specification from which data originates';


--
-- Name: COLUMN live_spec_flows.target_id; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.live_spec_flows.target_id IS 'Specification to which data flows';


--
-- Name: lock_monitor; Type: VIEW; Schema: public; Owner: postgres
--

CREATE VIEW public.lock_monitor AS
 SELECT COALESCE(((blockingl.relation)::regclass)::text, blockingl.locktype) AS locked_item,
    (now() - blockeda.query_start) AS waiting_duration,
    blockeda.pid AS blocked_pid,
    blockeda.query AS blocked_query,
    blockedl.mode AS blocked_mode,
    blockinga.pid AS blocking_pid,
    blockinga.query AS blocking_query,
    blockingl.mode AS blocking_mode
   FROM (((pg_locks blockedl
     JOIN pg_stat_activity blockeda ON ((blockedl.pid = blockeda.pid)))
     JOIN pg_locks blockingl ON ((((blockingl.transactionid = blockedl.transactionid) OR ((blockingl.relation = blockedl.relation) AND (blockingl.locktype = blockedl.locktype))) AND (blockedl.pid <> blockingl.pid))))
     JOIN pg_stat_activity blockinga ON (((blockingl.pid = blockinga.pid) AND (blockinga.datid = blockeda.datid))))
  WHERE ((NOT blockedl.granted) AND (blockinga.datname = current_database()));


ALTER VIEW public.lock_monitor OWNER TO postgres;

--
-- Name: old_catalog_stats; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.old_catalog_stats (
    catalog_name public.catalog_name NOT NULL,
    grain text NOT NULL,
    bytes_written_by_me bigint NOT NULL,
    docs_written_by_me bigint NOT NULL,
    bytes_read_by_me bigint NOT NULL,
    docs_read_by_me bigint NOT NULL,
    bytes_written_to_me bigint NOT NULL,
    docs_written_to_me bigint NOT NULL,
    bytes_read_from_me bigint NOT NULL,
    docs_read_from_me bigint NOT NULL,
    ts timestamp with time zone NOT NULL,
    flow_document json NOT NULL,
    errors integer DEFAULT 0 NOT NULL,
    failures integer DEFAULT 0 NOT NULL,
    warnings integer DEFAULT 0 NOT NULL
)
PARTITION BY LIST (SUBSTRING(catalog_name FROM 1 FOR POSITION(('/'::text) IN (catalog_name))));


ALTER TABLE public.old_catalog_stats OWNER TO postgres;

--
-- Name: TABLE old_catalog_stats; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON TABLE public.old_catalog_stats IS 'Generated for materialization ops/Pompato/catalog-stats-view of collection ops/Pompato/catalog-stats';


--
-- Name: COLUMN old_catalog_stats.catalog_name; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.old_catalog_stats.catalog_name IS 'Name of the Flow catalog
user-provided projection of JSON at: /catalogName with inferred types: [string]';


--
-- Name: COLUMN old_catalog_stats.grain; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.old_catalog_stats.grain IS 'Time grain that the stats are aggregated over
auto-generated projection of JSON at: /grain with inferred types: [string]';


--
-- Name: COLUMN old_catalog_stats.bytes_written_by_me; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.old_catalog_stats.bytes_written_by_me IS 'Total number of bytes representing the JSON encoded documents
user-provided projection of JSON at: /statsSummary/writtenByMe/bytesTotal with inferred types: [integer]';


--
-- Name: COLUMN old_catalog_stats.docs_written_by_me; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.old_catalog_stats.docs_written_by_me IS 'Total number of documents
user-provided projection of JSON at: /statsSummary/writtenByMe/docsTotal with inferred types: [integer]';


--
-- Name: COLUMN old_catalog_stats.bytes_read_by_me; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.old_catalog_stats.bytes_read_by_me IS 'Total number of bytes representing the JSON encoded documents
user-provided projection of JSON at: /statsSummary/readByMe/bytesTotal with inferred types: [integer]';


--
-- Name: COLUMN old_catalog_stats.docs_read_by_me; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.old_catalog_stats.docs_read_by_me IS 'Total number of documents
user-provided projection of JSON at: /statsSummary/readByMe/docsTotal with inferred types: [integer]';


--
-- Name: COLUMN old_catalog_stats.bytes_written_to_me; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.old_catalog_stats.bytes_written_to_me IS 'Total number of bytes representing the JSON encoded documents
user-provided projection of JSON at: /statsSummary/writtenToMe/bytesTotal with inferred types: [integer]';


--
-- Name: COLUMN old_catalog_stats.docs_written_to_me; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.old_catalog_stats.docs_written_to_me IS 'Total number of documents
user-provided projection of JSON at: /statsSummary/writtenToMe/docsTotal with inferred types: [integer]';


--
-- Name: COLUMN old_catalog_stats.bytes_read_from_me; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.old_catalog_stats.bytes_read_from_me IS 'Total number of bytes representing the JSON encoded documents
user-provided projection of JSON at: /statsSummary/readFromMe/bytesTotal with inferred types: [integer]';


--
-- Name: COLUMN old_catalog_stats.docs_read_from_me; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.old_catalog_stats.docs_read_from_me IS 'Total number of documents
user-provided projection of JSON at: /statsSummary/readFromMe/docsTotal with inferred types: [integer]';


--
-- Name: COLUMN old_catalog_stats.ts; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.old_catalog_stats.ts IS 'Timestamp of the catalog stat aggregate
auto-generated projection of JSON at: /ts with inferred types: [string]';


--
-- Name: COLUMN old_catalog_stats.flow_document; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.old_catalog_stats.flow_document IS 'Flow catalog task stats
Statistics related to the processing of a Flow catalog.
user-provided projection of JSON at:  with inferred types: [object]';


--
-- Name: COLUMN old_catalog_stats.errors; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.old_catalog_stats.errors IS 'Total number of logged errors
user-provided projection of JSON at: /statsSummary/errors with inferred types: [integer]';


--
-- Name: COLUMN old_catalog_stats.failures; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.old_catalog_stats.failures IS 'Total number of shard failures
user-provided projection of JSON at: /statsSummary/failures with inferred types: [integer]';


--
-- Name: COLUMN old_catalog_stats.warnings; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.old_catalog_stats.warnings IS 'Total number of logged warnings
user-provided projection of JSON at: /statsSummary/warnings with inferred types: [integer]';


--
-- Name: publication_specs_ext; Type: VIEW; Schema: public; Owner: postgres
--

CREATE VIEW public.publication_specs_ext AS
 SELECT p.live_spec_id,
    p.pub_id,
    p.detail,
    p.published_at,
    p.spec,
    p.spec_type,
    p.user_id,
    ls.catalog_name,
    ls.last_pub_id,
    u.email AS user_email,
    u.full_name AS user_full_name,
    u.avatar_url AS user_avatar_url,
    ls.data_plane_id
   FROM ((public.live_specs ls
     JOIN public.publication_specs p ON (((ls.id)::macaddr8 = (p.live_spec_id)::macaddr8)))
     CROSS JOIN LATERAL public.view_user_profile(p.user_id) u(user_id, email, full_name, avatar_url))
  WHERE (EXISTS ( SELECT 1
           FROM public.auth_roles('read'::public.grant_capability) r(role_prefix, capability)
          WHERE ((ls.catalog_name)::text ^@ (r.role_prefix)::text)));


ALTER VIEW public.publication_specs_ext OWNER TO postgres;

--
-- Name: publications; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.publications (
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    detail text,
    id public.flowid NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    job_status public.jsonb_obj DEFAULT '{"type": "queued"}'::jsonb NOT NULL,
    logs_token uuid DEFAULT gen_random_uuid() NOT NULL,
    user_id uuid DEFAULT auth.uid() NOT NULL,
    draft_id public.flowid NOT NULL,
    dry_run boolean DEFAULT false NOT NULL,
    auto_evolve boolean DEFAULT false NOT NULL,
    background boolean DEFAULT false NOT NULL,
    data_plane_name text DEFAULT 'ops/dp/public/gcp-us-central1-c1'::text NOT NULL,
    pub_id public.flowid
);


ALTER TABLE public.publications OWNER TO postgres;

--
-- Name: TABLE publications; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON TABLE public.publications IS 'Publications are operations which test and publish drafts into live specifications';


--
-- Name: COLUMN publications.created_at; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.publications.created_at IS 'Time at which the record was created';


--
-- Name: COLUMN publications.detail; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.publications.detail IS 'Description of the record';


--
-- Name: COLUMN publications.id; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.publications.id IS 'ID of the record';


--
-- Name: COLUMN publications.updated_at; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.publications.updated_at IS 'Time at which the record was last updated';


--
-- Name: COLUMN publications.job_status; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.publications.job_status IS 'Server-side job executation status of the record';


--
-- Name: COLUMN publications.logs_token; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.publications.logs_token IS 'Bearer token for accessing logs of the server-side operation';


--
-- Name: COLUMN publications.user_id; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.publications.user_id IS 'User who created the publication';


--
-- Name: COLUMN publications.draft_id; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.publications.draft_id IS 'Draft which is published';


--
-- Name: COLUMN publications.dry_run; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.publications.dry_run IS 'A dry-run publication will test and verify a draft, but doesn''t publish into live specifications';


--
-- Name: COLUMN publications.auto_evolve; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.publications.auto_evolve IS 'Whether to automatically handle schema evolution if the publication fails due to incompatible collections.
  If true, then an evolutions job will be created automatically if needed, and the results will be published again.';


--
-- Name: COLUMN publications.background; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.publications.background IS 'indicates whether this is a background job, which will be processed with a lower priority than interactive jobs';


--
-- Name: COLUMN publications.pub_id; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.publications.pub_id IS 'The effective publication id that was used by the publications handler
to commit a successful publication. This will be null if the publication
did not commit. If non-null, then this is the publication id that would
exist in the publication_specs table, and would be used as the last_pub_id
for any drafted specs';


--
-- Name: refresh_tokens; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.refresh_tokens (
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    detail text,
    id public.flowid NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    user_id uuid NOT NULL,
    multi_use boolean DEFAULT false,
    valid_for interval NOT NULL,
    uses integer DEFAULT 0,
    hash text NOT NULL
);


ALTER TABLE public.refresh_tokens OWNER TO postgres;

--
-- Name: COLUMN refresh_tokens.created_at; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.refresh_tokens.created_at IS 'Time at which the record was created';


--
-- Name: COLUMN refresh_tokens.detail; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.refresh_tokens.detail IS 'Description of the record';


--
-- Name: COLUMN refresh_tokens.id; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.refresh_tokens.id IS 'ID of the record';


--
-- Name: COLUMN refresh_tokens.updated_at; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.refresh_tokens.updated_at IS 'Time at which the record was last updated';


--
-- Name: registered_avro_schemas; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.registered_avro_schemas (
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    detail text,
    id public.flowid NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    avro_schema json NOT NULL,
    avro_schema_md5 text GENERATED ALWAYS AS (md5(TRIM(BOTH FROM (avro_schema)::text))) STORED,
    catalog_name public.catalog_name NOT NULL,
    registry_id integer NOT NULL
);


ALTER TABLE public.registered_avro_schemas OWNER TO postgres;

--
-- Name: TABLE registered_avro_schemas; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON TABLE public.registered_avro_schemas IS '
Avro schemas registered with a globally unique, stable registery ID.

This is used to emulate the behavior of Confluent Schema Registry when
transcoding collection documents into Avro for use with Dekaf,
which must encode each message with an Avro schema ID (registry_id).
';


--
-- Name: COLUMN registered_avro_schemas.created_at; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.registered_avro_schemas.created_at IS 'Time at which the record was created';


--
-- Name: COLUMN registered_avro_schemas.detail; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.registered_avro_schemas.detail IS 'Description of the record';


--
-- Name: COLUMN registered_avro_schemas.id; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.registered_avro_schemas.id IS 'ID of the record';


--
-- Name: COLUMN registered_avro_schemas.updated_at; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.registered_avro_schemas.updated_at IS 'Time at which the record was last updated';


--
-- Name: registered_avro_schemas_registry_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.registered_avro_schemas_registry_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.registered_avro_schemas_registry_id_seq OWNER TO postgres;

--
-- Name: registered_avro_schemas_registry_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.registered_avro_schemas_registry_id_seq OWNED BY public.registered_avro_schemas.registry_id;


--
-- Name: storage_mappings; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.storage_mappings (
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    detail text,
    id public.flowid NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    catalog_prefix public.catalog_prefix NOT NULL,
    spec json NOT NULL
);


ALTER TABLE public.storage_mappings OWNER TO postgres;

--
-- Name: TABLE storage_mappings; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON TABLE public.storage_mappings IS 'Storage mappings which are applied to published specifications';


--
-- Name: COLUMN storage_mappings.created_at; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.storage_mappings.created_at IS 'Time at which the record was created';


--
-- Name: COLUMN storage_mappings.detail; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.storage_mappings.detail IS 'Description of the record';


--
-- Name: COLUMN storage_mappings.id; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.storage_mappings.id IS 'ID of the record';


--
-- Name: COLUMN storage_mappings.updated_at; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.storage_mappings.updated_at IS 'Time at which the record was last updated';


--
-- Name: COLUMN storage_mappings.catalog_prefix; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.storage_mappings.catalog_prefix IS 'Catalog prefix which this storage mapping prefixes';


--
-- Name: COLUMN storage_mappings.spec; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.storage_mappings.spec IS 'Specification of this storage mapping';


--
-- Name: test_publication_specs_ext; Type: VIEW; Schema: public; Owner: postgres
--

CREATE VIEW public.test_publication_specs_ext AS
 SELECT p.live_spec_id,
    p.pub_id,
    p.detail,
    p.published_at,
    p.spec,
    p.spec_type,
    p.user_id,
    ls.catalog_name,
    ls.last_pub_id,
    u.email AS user_email,
    u.full_name AS user_full_name,
    u.avatar_url AS user_avatar_url,
    ls.data_plane_id
   FROM ((public.live_specs ls
     JOIN public.publication_specs p ON (((ls.id)::macaddr8 = (p.live_spec_id)::macaddr8)))
     CROSS JOIN LATERAL public.view_user_profile(p.user_id) u(user_id, email, full_name, avatar_url))
  WHERE (EXISTS ( SELECT 1
           FROM public.auth_roles('read'::public.grant_capability) r(role_prefix, capability)
          WHERE ((ls.catalog_name)::text ^@ (r.role_prefix)::text)));


ALTER VIEW public.test_publication_specs_ext OWNER TO postgres;

--
-- Name: unchanged_draft_specs; Type: VIEW; Schema: public; Owner: postgres
--

CREATE VIEW public.unchanged_draft_specs AS
 SELECT d.draft_id,
    d.catalog_name,
    d.spec_type,
    d.live_spec_md5,
    d.draft_spec_md5,
    d.inferred_schema_md5,
    d.live_inferred_schema_md5
   FROM public.draft_specs_ext d
  WHERE (d.draft_spec_md5 = d.live_spec_md5);


ALTER VIEW public.unchanged_draft_specs OWNER TO postgres;

--
-- Name: VIEW unchanged_draft_specs; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON VIEW public.unchanged_draft_specs IS 'View of `draft_specs_ext` that is filtered to only include specs that are identical to the
 current `live_specs`.';


--
-- Name: catalog_stats_daily; Type: TABLE ATTACH; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.catalog_stats ATTACH PARTITION public.catalog_stats_daily FOR VALUES IN ('daily');


--
-- Name: catalog_stats_hourly; Type: TABLE ATTACH; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.catalog_stats ATTACH PARTITION public.catalog_stats_hourly FOR VALUES IN ('hourly');


--
-- Name: catalog_stats_monthly; Type: TABLE ATTACH; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.catalog_stats ATTACH PARTITION public.catalog_stats_monthly FOR VALUES IN ('monthly');


--
-- Name: registered_avro_schemas registry_id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.registered_avro_schemas ALTER COLUMN registry_id SET DEFAULT nextval('public.registered_avro_schemas_registry_id_seq'::regclass);


--
-- Name: _model_async _model_async_pkey; Type: CONSTRAINT; Schema: internal; Owner: postgres
--

ALTER TABLE ONLY internal._model_async
    ADD CONSTRAINT _model_async_pkey PRIMARY KEY (id);


--
-- Name: _model _model_pkey; Type: CONSTRAINT; Schema: internal; Owner: postgres
--

ALTER TABLE ONLY internal._model
    ADD CONSTRAINT _model_pkey PRIMARY KEY (id);


--
-- Name: billing_adjustments billing_adjustments_pkey; Type: CONSTRAINT; Schema: internal; Owner: postgres
--

ALTER TABLE ONLY internal.billing_adjustments
    ADD CONSTRAINT billing_adjustments_pkey PRIMARY KEY (id);


--
-- Name: billing_historicals billing_historicals_tenant_billed_month_key; Type: CONSTRAINT; Schema: internal; Owner: postgres
--

ALTER TABLE ONLY internal.billing_historicals
    ADD CONSTRAINT billing_historicals_tenant_billed_month_key UNIQUE (tenant, billed_month);

ALTER TABLE ONLY internal.billing_historicals REPLICA IDENTITY USING INDEX billing_historicals_tenant_billed_month_key;


--
-- Name: gateway_auth_keys gateway_auth_keys_pkey; Type: CONSTRAINT; Schema: internal; Owner: postgres
--

ALTER TABLE ONLY internal.gateway_auth_keys
    ADD CONSTRAINT gateway_auth_keys_pkey PRIMARY KEY (id);


--
-- Name: gateway_endpoints gateway_endpoints_pkey; Type: CONSTRAINT; Schema: internal; Owner: postgres
--

ALTER TABLE ONLY internal.gateway_endpoints
    ADD CONSTRAINT gateway_endpoints_pkey PRIMARY KEY (id);


--
-- Name: gcm_accounts gcm_accounts_pkey; Type: CONSTRAINT; Schema: internal; Owner: postgres
--

ALTER TABLE ONLY internal.gcm_accounts
    ADD CONSTRAINT gcm_accounts_pkey PRIMARY KEY (id);


--
-- Name: illegal_tenant_names illegal_tenant_names_pkey; Type: CONSTRAINT; Schema: internal; Owner: postgres
--

ALTER TABLE ONLY internal.illegal_tenant_names
    ADD CONSTRAINT illegal_tenant_names_pkey PRIMARY KEY (name);


--
-- Name: manual_bills manual_bills_pkey; Type: CONSTRAINT; Schema: internal; Owner: postgres
--

ALTER TABLE ONLY internal.manual_bills
    ADD CONSTRAINT manual_bills_pkey PRIMARY KEY (tenant, date_start, date_end);


--
-- Name: alert_data_processing alert_data_processing_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.alert_data_processing
    ADD CONSTRAINT alert_data_processing_pkey PRIMARY KEY (catalog_name);


--
-- Name: alert_history alert_history_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.alert_history
    ADD CONSTRAINT alert_history_pkey PRIMARY KEY (alert_type, catalog_name, fired_at);


--
-- Name: alert_subscriptions alert_subscriptions_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.alert_subscriptions
    ADD CONSTRAINT alert_subscriptions_pkey PRIMARY KEY (id);


--
-- Name: applied_directives applied_directives_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.applied_directives
    ADD CONSTRAINT applied_directives_pkey PRIMARY KEY (id);


--
-- Name: catalog_stats catalog_stats_pkey1; Type: CONSTRAINT; Schema: public; Owner: stats_loader
--

ALTER TABLE ONLY public.catalog_stats
    ADD CONSTRAINT catalog_stats_pkey1 PRIMARY KEY (catalog_name, grain, ts);


--
-- Name: catalog_stats_daily catalog_stats_daily_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.catalog_stats_daily
    ADD CONSTRAINT catalog_stats_daily_pkey PRIMARY KEY (catalog_name, grain, ts);


--
-- Name: catalog_stats_hourly catalog_stats_hourly_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.catalog_stats_hourly
    ADD CONSTRAINT catalog_stats_hourly_pkey PRIMARY KEY (catalog_name, grain, ts);


--
-- Name: catalog_stats_monthly catalog_stats_monthly_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.catalog_stats_monthly
    ADD CONSTRAINT catalog_stats_monthly_pkey PRIMARY KEY (catalog_name, grain, ts);


--
-- Name: connector_tags connector_tags_connector_id_image_tag_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.connector_tags
    ADD CONSTRAINT connector_tags_connector_id_image_tag_key UNIQUE (connector_id, image_tag);


--
-- Name: connector_tags connector_tags_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.connector_tags
    ADD CONSTRAINT connector_tags_pkey PRIMARY KEY (id);


--
-- Name: connectors connectors_image_name_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.connectors
    ADD CONSTRAINT connectors_image_name_key UNIQUE (image_name);


--
-- Name: connectors connectors_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.connectors
    ADD CONSTRAINT connectors_pkey PRIMARY KEY (id);


--
-- Name: controller_jobs controller_jobs_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.controller_jobs
    ADD CONSTRAINT controller_jobs_pkey PRIMARY KEY (live_spec_id);


--
-- Name: data_planes data_planes_data_plane_fqdn_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.data_planes
    ADD CONSTRAINT data_planes_data_plane_fqdn_key UNIQUE (data_plane_fqdn);


--
-- Name: data_planes data_planes_data_plane_name_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.data_planes
    ADD CONSTRAINT data_planes_data_plane_name_key UNIQUE (data_plane_name);


--
-- Name: data_planes data_planes_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.data_planes
    ADD CONSTRAINT data_planes_pkey PRIMARY KEY (id);


--
-- Name: directives directives_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.directives
    ADD CONSTRAINT directives_pkey PRIMARY KEY (id);


--
-- Name: directives directives_token_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.directives
    ADD CONSTRAINT directives_token_key UNIQUE (token);


--
-- Name: discovers discovers_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.discovers
    ADD CONSTRAINT discovers_pkey PRIMARY KEY (id);


--
-- Name: draft_specs draft_specs_draft_id_catalog_name_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.draft_specs
    ADD CONSTRAINT draft_specs_draft_id_catalog_name_key UNIQUE (draft_id, catalog_name);


--
-- Name: draft_specs draft_specs_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.draft_specs
    ADD CONSTRAINT draft_specs_pkey PRIMARY KEY (id);


--
-- Name: drafts drafts_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.drafts
    ADD CONSTRAINT drafts_pkey PRIMARY KEY (id);


--
-- Name: evolutions evolutions_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.evolutions
    ADD CONSTRAINT evolutions_pkey PRIMARY KEY (id);


--
-- Name: flow_watermarks flow_watermarks_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.flow_watermarks
    ADD CONSTRAINT flow_watermarks_pkey PRIMARY KEY (slot);


--
-- Name: inferred_schemas inferred_schemas_pkey; Type: CONSTRAINT; Schema: public; Owner: stats_loader
--

ALTER TABLE ONLY public.inferred_schemas
    ADD CONSTRAINT inferred_schemas_pkey PRIMARY KEY (collection_name);


--
-- Name: live_specs live_specs_catalog_name_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.live_specs
    ADD CONSTRAINT live_specs_catalog_name_key UNIQUE (catalog_name);


--
-- Name: live_specs live_specs_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.live_specs
    ADD CONSTRAINT live_specs_pkey PRIMARY KEY (id);


--
-- Name: publication_specs publication_specs_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.publication_specs
    ADD CONSTRAINT publication_specs_pkey PRIMARY KEY (live_spec_id, pub_id);


--
-- Name: publications publications_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.publications
    ADD CONSTRAINT publications_pkey PRIMARY KEY (id);


--
-- Name: refresh_tokens refresh_tokens_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.refresh_tokens
    ADD CONSTRAINT refresh_tokens_pkey PRIMARY KEY (id);


--
-- Name: registered_avro_schemas registered_avro_schemas_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.registered_avro_schemas
    ADD CONSTRAINT registered_avro_schemas_pkey PRIMARY KEY (id);


--
-- Name: registered_avro_schemas registered_avro_schemas_registry_id_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.registered_avro_schemas
    ADD CONSTRAINT registered_avro_schemas_registry_id_key UNIQUE (registry_id);


--
-- Name: role_grants role_grants_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.role_grants
    ADD CONSTRAINT role_grants_pkey PRIMARY KEY (id);


--
-- Name: role_grants role_grants_subject_role_object_role_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.role_grants
    ADD CONSTRAINT role_grants_subject_role_object_role_key UNIQUE (subject_role, object_role);


--
-- Name: storage_mappings storage_mappings_catalog_prefix_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.storage_mappings
    ADD CONSTRAINT storage_mappings_catalog_prefix_key UNIQUE (catalog_prefix);


--
-- Name: storage_mappings storage_mappings_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.storage_mappings
    ADD CONSTRAINT storage_mappings_pkey PRIMARY KEY (id);


--
-- Name: tenants tenants_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.tenants
    ADD CONSTRAINT tenants_pkey PRIMARY KEY (id);


--
-- Name: tenants tenants_tenant_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.tenants
    ADD CONSTRAINT tenants_tenant_key UNIQUE (tenant);


--
-- Name: user_grants user_grants_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.user_grants
    ADD CONSTRAINT user_grants_pkey PRIMARY KEY (id);


--
-- Name: user_grants user_grants_user_id_object_role_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.user_grants
    ADD CONSTRAINT user_grants_user_id_object_role_key UNIQUE (user_id, object_role);


--
-- Name: billing_historicals_tenant_starts_with; Type: INDEX; Schema: internal; Owner: postgres
--

CREATE INDEX billing_historicals_tenant_starts_with ON internal.billing_historicals USING btree (tenant COLLATE "C");


--
-- Name: idx_gcm_accounts_id_where_approved; Type: INDEX; Schema: internal; Owner: postgres
--

CREATE UNIQUE INDEX idx_gcm_accounts_id_where_approved ON internal.gcm_accounts USING btree (id) WHERE (approved = true);


--
-- Name: idx_logs_token; Type: INDEX; Schema: internal; Owner: postgres
--

CREATE INDEX idx_logs_token ON internal.log_lines USING btree (token);


--
-- Name: catalog_stats_catalog_index; Type: INDEX; Schema: public; Owner: stats_loader
--

CREATE INDEX catalog_stats_catalog_index ON ONLY public.catalog_stats USING btree (catalog_name);


--
-- Name: catalog_stats_catalog_index_spgist; Type: INDEX; Schema: public; Owner: stats_loader
--

CREATE INDEX catalog_stats_catalog_index_spgist ON ONLY public.catalog_stats USING spgist (((catalog_name)::text));


--
-- Name: catalog_stats_daily_catalog_name_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX catalog_stats_daily_catalog_name_idx ON public.catalog_stats_daily USING btree (catalog_name);


--
-- Name: catalog_stats_daily_catalog_name_idx3; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX catalog_stats_daily_catalog_name_idx3 ON public.catalog_stats_daily USING spgist (((catalog_name)::text));


--
-- Name: catalog_stats_hourly_catalog_name_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX catalog_stats_hourly_catalog_name_idx ON public.catalog_stats_hourly USING btree (catalog_name);


--
-- Name: catalog_stats_hourly_catalog_name_idx3; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX catalog_stats_hourly_catalog_name_idx3 ON public.catalog_stats_hourly USING spgist (((catalog_name)::text));


--
-- Name: catalog_stats_monthly_catalog_name_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX catalog_stats_monthly_catalog_name_idx ON public.catalog_stats_monthly USING btree (catalog_name);


--
-- Name: catalog_stats_monthly_catalog_name_idx3; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX catalog_stats_monthly_catalog_name_idx3 ON public.catalog_stats_monthly USING spgist (((catalog_name)::text));


--
-- Name: discovers_queued; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX discovers_queued ON public.discovers USING btree (id) WHERE (((job_status)::jsonb ->> 'type'::text) = 'queued'::text);


--
-- Name: evolutions_queued; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX evolutions_queued ON public.evolutions USING btree (id) WHERE (((job_status)::jsonb ->> 'type'::text) = 'queued'::text);


--
-- Name: idx_catalog_stats_catalog_name_grain_ts; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_catalog_stats_catalog_name_grain_ts ON ONLY public.old_catalog_stats USING btree (catalog_name, grain, ts DESC);


--
-- Name: idx_catalog_stats_grain_ts; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_catalog_stats_grain_ts ON ONLY public.old_catalog_stats USING btree (catalog_name, grain, ts DESC);


--
-- Name: idx_connector_tags_id_where_queued; Type: INDEX; Schema: public; Owner: postgres
--

CREATE UNIQUE INDEX idx_connector_tags_id_where_queued ON public.connector_tags USING btree (id) WHERE (((job_status)::jsonb ->> 'type'::text) = 'queued'::text);


--
-- Name: idx_directives_catalog_prefix; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_directives_catalog_prefix ON public.directives USING btree (catalog_prefix text_pattern_ops);


--
-- Name: idx_draft_errors_draft_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_draft_errors_draft_id ON public.draft_errors USING btree (draft_id);


--
-- Name: idx_drafts_user_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_drafts_user_id ON public.drafts USING btree (user_id);


--
-- Name: idx_live_spec_flows_forward; Type: INDEX; Schema: public; Owner: postgres
--

CREATE UNIQUE INDEX idx_live_spec_flows_forward ON public.live_spec_flows USING btree (source_id, target_id) INCLUDE (flow_type);


--
-- Name: idx_live_spec_flows_reverse; Type: INDEX; Schema: public; Owner: postgres
--

CREATE UNIQUE INDEX idx_live_spec_flows_reverse ON public.live_spec_flows USING btree (target_id, source_id) INCLUDE (flow_type);


--
-- Name: idx_live_specs_catalog_name_spgist; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_live_specs_catalog_name_spgist ON public.live_specs USING spgist (((catalog_name)::text));


--
-- Name: idx_live_specs_spec_type; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_live_specs_spec_type ON public.live_specs USING btree (spec_type);


--
-- Name: idx_live_specs_updated_at; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_live_specs_updated_at ON public.live_specs USING btree (updated_at DESC NULLS LAST);


--
-- Name: idx_registered_avro_schemas_avro_schema_md5; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_registered_avro_schemas_avro_schema_md5 ON public.registered_avro_schemas USING btree (avro_schema_md5);


--
-- Name: idx_role_grants_object_role_spgist; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_role_grants_object_role_spgist ON public.role_grants USING spgist (((object_role)::text));


--
-- Name: idx_role_grants_subject_role_spgist; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_role_grants_subject_role_spgist ON public.role_grants USING spgist (((subject_role)::text));


--
-- Name: idx_user_grants_object_role_spgist; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_user_grants_object_role_spgist ON public.user_grants USING spgist (((object_role)::text));


--
-- Name: live_specs_controller_next_run; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX live_specs_controller_next_run ON public.live_specs USING btree (controller_next_run) INCLUDE (id) WHERE (controller_next_run IS NOT NULL);


--
-- Name: publications_queued; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX publications_queued ON public.publications USING btree (id) WHERE (((job_status)::jsonb ->> 'type'::text) = 'queued'::text);


--
-- Name: catalog_stats_daily_catalog_name_idx; Type: INDEX ATTACH; Schema: public; Owner: stats_loader
--

ALTER INDEX public.catalog_stats_catalog_index ATTACH PARTITION public.catalog_stats_daily_catalog_name_idx;


--
-- Name: catalog_stats_daily_catalog_name_idx3; Type: INDEX ATTACH; Schema: public; Owner: stats_loader
--

ALTER INDEX public.catalog_stats_catalog_index_spgist ATTACH PARTITION public.catalog_stats_daily_catalog_name_idx3;


--
-- Name: catalog_stats_daily_pkey; Type: INDEX ATTACH; Schema: public; Owner: stats_loader
--

ALTER INDEX public.catalog_stats_pkey1 ATTACH PARTITION public.catalog_stats_daily_pkey;


--
-- Name: catalog_stats_hourly_catalog_name_idx; Type: INDEX ATTACH; Schema: public; Owner: stats_loader
--

ALTER INDEX public.catalog_stats_catalog_index ATTACH PARTITION public.catalog_stats_hourly_catalog_name_idx;


--
-- Name: catalog_stats_hourly_catalog_name_idx3; Type: INDEX ATTACH; Schema: public; Owner: stats_loader
--

ALTER INDEX public.catalog_stats_catalog_index_spgist ATTACH PARTITION public.catalog_stats_hourly_catalog_name_idx3;


--
-- Name: catalog_stats_hourly_pkey; Type: INDEX ATTACH; Schema: public; Owner: stats_loader
--

ALTER INDEX public.catalog_stats_pkey1 ATTACH PARTITION public.catalog_stats_hourly_pkey;


--
-- Name: catalog_stats_monthly_catalog_name_idx; Type: INDEX ATTACH; Schema: public; Owner: stats_loader
--

ALTER INDEX public.catalog_stats_catalog_index ATTACH PARTITION public.catalog_stats_monthly_catalog_name_idx;


--
-- Name: catalog_stats_monthly_catalog_name_idx3; Type: INDEX ATTACH; Schema: public; Owner: stats_loader
--

ALTER INDEX public.catalog_stats_catalog_index_spgist ATTACH PARTITION public.catalog_stats_monthly_catalog_name_idx3;


--
-- Name: catalog_stats_monthly_pkey; Type: INDEX ATTACH; Schema: public; Owner: stats_loader
--

ALTER INDEX public.catalog_stats_pkey1 ATTACH PARTITION public.catalog_stats_monthly_pkey;


--
-- Name: next_auto_discovers _RETURN; Type: RULE; Schema: internal; Owner: postgres
--

CREATE OR REPLACE VIEW internal.next_auto_discovers AS
 SELECT live_specs.id AS capture_id,
    live_specs.catalog_name AS capture_name,
    (((live_specs.spec -> 'endpoint'::text) -> 'connector'::text) -> 'config'::text) AS endpoint_json,
    COALESCE((((live_specs.spec -> 'autoDiscover'::text) ->> 'addNewBindings'::text))::boolean, false) AS add_new_bindings,
    COALESCE((((live_specs.spec -> 'autoDiscover'::text) ->> 'evolveIncompatibleCollections'::text))::boolean, false) AS evolve_incompatible_collections,
    connector_tags.id AS connector_tags_id,
    ((now() - GREATEST(max(discovers.updated_at), live_specs.updated_at)) + connector_tags.auto_discover_interval) AS overdue_interval
   FROM (((public.live_specs
     LEFT JOIN public.discovers ON (((live_specs.catalog_name)::text = (discovers.capture_name)::text)))
     JOIN public.connectors ON ((live_specs.connector_image_name = connectors.image_name)))
     JOIN public.connector_tags ON ((((connectors.id)::macaddr8 = (connector_tags.connector_id)::macaddr8) AND (live_specs.connector_image_tag = connector_tags.image_tag))))
  WHERE ((live_specs.spec_type = 'capture'::public.catalog_spec_type) AND (NOT COALESCE((((live_specs.spec -> 'shards'::text) ->> 'disable'::text))::boolean, false)) AND (COALESCE(json_typeof((live_specs.spec -> 'autoDiscover'::text)), 'null'::text) <> 'null'::text))
  GROUP BY live_specs.id, connector_tags.id
 HAVING ((now() - GREATEST(max(discovers.updated_at), live_specs.updated_at)) > connector_tags.auto_discover_interval)
  ORDER BY ((now() - GREATEST(max(discovers.updated_at), live_specs.updated_at)) + connector_tags.auto_discover_interval) DESC;


--
-- Name: tenants Grant support role access to tenants; Type: TRIGGER; Schema: public; Owner: postgres
--

CREATE TRIGGER "Grant support role access to tenants" AFTER INSERT OR UPDATE ON public.tenants FOR EACH STATEMENT EXECUTE FUNCTION internal.update_support_role();


--
-- Name: alert_history Send email after alert fired; Type: TRIGGER; Schema: public; Owner: postgres
--

CREATE TRIGGER "Send email after alert fired" AFTER INSERT ON public.alert_history FOR EACH ROW EXECUTE FUNCTION internal.send_alerts();


--
-- Name: alert_history Send email after alert resolved; Type: TRIGGER; Schema: public; Owner: postgres
--

CREATE TRIGGER "Send email after alert resolved" AFTER UPDATE ON public.alert_history FOR EACH ROW WHEN (((old.resolved_at IS NULL) AND (new.resolved_at IS NOT NULL))) EXECUTE FUNCTION internal.send_alerts();


--
-- Name: applied_directives Verify delete of applied directives; Type: TRIGGER; Schema: public; Owner: postgres
--

CREATE TRIGGER "Verify delete of applied directives" BEFORE DELETE ON public.applied_directives FOR EACH ROW EXECUTE FUNCTION internal.on_applied_directives_delete();


--
-- Name: applied_directives Verify update of applied directives; Type: TRIGGER; Schema: public; Owner: postgres
--

CREATE TRIGGER "Verify update of applied directives" BEFORE UPDATE ON public.applied_directives FOR EACH ROW EXECUTE FUNCTION internal.on_applied_directives_update();


--
-- Name: applied_directives applied_directives_agent_notifications; Type: TRIGGER; Schema: public; Owner: postgres
--

CREATE TRIGGER applied_directives_agent_notifications AFTER INSERT OR UPDATE ON public.applied_directives FOR EACH ROW WHEN ((((new.job_status)::jsonb ->> 'type'::text) = 'queued'::text)) EXECUTE FUNCTION internal.notify_agent();


--
-- Name: connector_tags connector_tags_agent_notifications; Type: TRIGGER; Schema: public; Owner: postgres
--

CREATE TRIGGER connector_tags_agent_notifications AFTER INSERT OR UPDATE ON public.connector_tags FOR EACH ROW WHEN ((((new.job_status)::jsonb ->> 'type'::text) = 'queued'::text)) EXECUTE FUNCTION internal.notify_agent();


--
-- Name: discovers discovers_agent_notifications; Type: TRIGGER; Schema: public; Owner: postgres
--

CREATE TRIGGER discovers_agent_notifications AFTER INSERT OR UPDATE ON public.discovers FOR EACH ROW WHEN ((((new.job_status)::jsonb ->> 'type'::text) = 'queued'::text)) EXECUTE FUNCTION internal.notify_agent();


--
-- Name: evolutions evolutions_agent_notifications; Type: TRIGGER; Schema: public; Owner: postgres
--

CREATE TRIGGER evolutions_agent_notifications AFTER INSERT OR UPDATE ON public.evolutions FOR EACH ROW WHEN ((((new.job_status)::jsonb ->> 'type'::text) = 'queued'::text)) EXECUTE FUNCTION internal.notify_agent();


--
-- Name: inferred_schemas inferred_schema_controller_insert; Type: TRIGGER; Schema: public; Owner: stats_loader
--

CREATE TRIGGER inferred_schema_controller_insert AFTER INSERT ON public.inferred_schemas FOR EACH ROW EXECUTE FUNCTION internal.on_inferred_schema_update();


--
-- Name: inferred_schemas inferred_schema_controller_update; Type: TRIGGER; Schema: public; Owner: stats_loader
--

CREATE TRIGGER inferred_schema_controller_update AFTER UPDATE ON public.inferred_schemas FOR EACH ROW WHEN ((old.md5 IS DISTINCT FROM new.md5)) EXECUTE FUNCTION internal.on_inferred_schema_update();


--
-- Name: publications publications_agent_notifications; Type: TRIGGER; Schema: public; Owner: postgres
--

CREATE TRIGGER publications_agent_notifications AFTER INSERT OR UPDATE ON public.publications FOR EACH ROW WHEN ((((new.job_status)::jsonb ->> 'type'::text) = 'queued'::text)) EXECUTE FUNCTION internal.notify_agent();


--
-- Name: connectors update-marketing-site-on-connector-change; Type: TRIGGER; Schema: public; Owner: postgres
--

CREATE TRIGGER "update-marketing-site-on-connector-change" AFTER INSERT OR DELETE OR UPDATE ON public.connectors FOR EACH ROW EXECUTE FUNCTION supabase_functions.http_request('https://strapi.estuary.dev/api', 'POST', '{"Authorization":"Bearer supersecretpassword"}', '{"event_type":"database_updated","repo":"estuary/marketing-site"}', '1000');


--
-- Name: connector_tags update-marketing-site-on-connector-tags-change; Type: TRIGGER; Schema: public; Owner: postgres
--

CREATE TRIGGER "update-marketing-site-on-connector-tags-change" AFTER INSERT OR DELETE OR UPDATE ON public.connector_tags FOR EACH ROW EXECUTE FUNCTION supabase_functions.http_request('https://strapi.estuary.dev/api', 'POST', '{"Authorization":"Bearer supersecretpassword"}', '{"event_type":"database_updated","repo":"estuary/marketing-site"}', '1000');


--
-- Name: billing_adjustments billing_adjustments_tenant_fkey; Type: FK CONSTRAINT; Schema: internal; Owner: postgres
--

ALTER TABLE ONLY internal.billing_adjustments
    ADD CONSTRAINT billing_adjustments_tenant_fkey FOREIGN KEY (tenant) REFERENCES public.tenants(tenant);


--
-- Name: manual_bills manual_bills_tenant_fkey; Type: FK CONSTRAINT; Schema: internal; Owner: postgres
--

ALTER TABLE ONLY internal.manual_bills
    ADD CONSTRAINT manual_bills_tenant_fkey FOREIGN KEY (tenant) REFERENCES public.tenants(tenant);


--
-- Name: applied_directives applied_directives_directive_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.applied_directives
    ADD CONSTRAINT applied_directives_directive_id_fkey FOREIGN KEY (directive_id) REFERENCES public.directives(id);


--
-- Name: applied_directives applied_directives_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.applied_directives
    ADD CONSTRAINT applied_directives_user_id_fkey FOREIGN KEY (user_id) REFERENCES auth.users(id);


--
-- Name: connector_tags connector_tags_connector_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.connector_tags
    ADD CONSTRAINT connector_tags_connector_id_fkey FOREIGN KEY (connector_id) REFERENCES public.connectors(id);


--
-- Name: controller_jobs controller_jobs_live_spec_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.controller_jobs
    ADD CONSTRAINT controller_jobs_live_spec_id_fkey FOREIGN KEY (live_spec_id) REFERENCES public.live_specs(id) ON DELETE CASCADE;


--
-- Name: discovers discovers_connector_tag_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.discovers
    ADD CONSTRAINT discovers_connector_tag_id_fkey FOREIGN KEY (connector_tag_id) REFERENCES public.connector_tags(id);


--
-- Name: discovers discovers_draft_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.discovers
    ADD CONSTRAINT discovers_draft_id_fkey FOREIGN KEY (draft_id) REFERENCES public.drafts(id) ON DELETE CASCADE;


--
-- Name: draft_errors draft_errors_draft_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.draft_errors
    ADD CONSTRAINT draft_errors_draft_id_fkey FOREIGN KEY (draft_id) REFERENCES public.drafts(id) ON DELETE CASCADE;


--
-- Name: draft_specs draft_specs_draft_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.draft_specs
    ADD CONSTRAINT draft_specs_draft_id_fkey FOREIGN KEY (draft_id) REFERENCES public.drafts(id) ON DELETE CASCADE;


--
-- Name: drafts drafts_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.drafts
    ADD CONSTRAINT drafts_user_id_fkey FOREIGN KEY (user_id) REFERENCES auth.users(id);


--
-- Name: evolutions evolutions_draft_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.evolutions
    ADD CONSTRAINT evolutions_draft_id_fkey FOREIGN KEY (draft_id) REFERENCES public.drafts(id) ON DELETE CASCADE;


--
-- Name: evolutions evolutions_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.evolutions
    ADD CONSTRAINT evolutions_user_id_fkey FOREIGN KEY (user_id) REFERENCES auth.users(id);


--
-- Name: live_spec_flows live_spec_flows_source_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.live_spec_flows
    ADD CONSTRAINT live_spec_flows_source_id_fkey FOREIGN KEY (source_id) REFERENCES public.live_specs(id) ON DELETE CASCADE;


--
-- Name: live_spec_flows live_spec_flows_target_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.live_spec_flows
    ADD CONSTRAINT live_spec_flows_target_id_fkey FOREIGN KEY (target_id) REFERENCES public.live_specs(id) ON DELETE CASCADE;


--
-- Name: publication_specs publication_specs_live_spec_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.publication_specs
    ADD CONSTRAINT publication_specs_live_spec_id_fkey FOREIGN KEY (live_spec_id) REFERENCES public.live_specs(id) ON DELETE CASCADE;


--
-- Name: publication_specs publication_specs_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.publication_specs
    ADD CONSTRAINT publication_specs_user_id_fkey FOREIGN KEY (user_id) REFERENCES auth.users(id);


--
-- Name: publications publications_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.publications
    ADD CONSTRAINT publications_user_id_fkey FOREIGN KEY (user_id) REFERENCES auth.users(id);


--
-- Name: refresh_tokens refresh_tokens_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.refresh_tokens
    ADD CONSTRAINT refresh_tokens_user_id_fkey FOREIGN KEY (user_id) REFERENCES auth.users(id);


--
-- Name: tenants tenants_gcm_account_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.tenants
    ADD CONSTRAINT tenants_gcm_account_id_fkey FOREIGN KEY (gcm_account_id) REFERENCES internal.gcm_accounts(id);


--
-- Name: user_grants user_grants_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.user_grants
    ADD CONSTRAINT user_grants_user_id_fkey FOREIGN KEY (user_id) REFERENCES auth.users(id);


--
-- Name: alert_history Users access alert history for admin-authorized tasks; Type: POLICY; Schema: public; Owner: postgres
--

CREATE POLICY "Users access alert history for admin-authorized tasks" ON public.alert_history USING ((EXISTS ( SELECT 1
   FROM public.auth_roles('admin'::public.grant_capability) r(role_prefix, capability)
  WHERE ((alert_history.catalog_name)::text ^@ (r.role_prefix)::text))));


--
-- Name: alert_data_processing Users access alerts for admin-authorized tasks; Type: POLICY; Schema: public; Owner: postgres
--

CREATE POLICY "Users access alerts for admin-authorized tasks" ON public.alert_data_processing USING ((EXISTS ( SELECT 1
   FROM public.auth_roles('admin'::public.grant_capability) r(role_prefix, capability)
  WHERE ((alert_data_processing.catalog_name)::text ^@ (r.role_prefix)::text))));


--
-- Name: alert_subscriptions Users access subscriptions for the prefixes they admin; Type: POLICY; Schema: public; Owner: postgres
--

CREATE POLICY "Users access subscriptions for the prefixes they admin" ON public.alert_subscriptions USING ((EXISTS ( SELECT 1
   FROM public.auth_roles('admin'::public.grant_capability) r(role_prefix, capability)
  WHERE ((alert_subscriptions.catalog_prefix)::text ^@ (r.role_prefix)::text))));


--
-- Name: discovers Users access their discovers; Type: POLICY; Schema: public; Owner: postgres
--

CREATE POLICY "Users access their discovers" ON public.discovers USING (((draft_id)::macaddr8 IN ( SELECT drafts.id
   FROM public.drafts)));


--
-- Name: draft_specs Users access their draft specs; Type: POLICY; Schema: public; Owner: postgres
--

CREATE POLICY "Users access their draft specs" ON public.draft_specs USING (((draft_id)::macaddr8 IN ( SELECT drafts.id
   FROM public.drafts
  WHERE (drafts.user_id = ( SELECT auth.uid() AS uid)))));


--
-- Name: directives Users can access and change directives which they administer; Type: POLICY; Schema: public; Owner: postgres
--

CREATE POLICY "Users can access and change directives which they administer" ON public.directives USING ((EXISTS ( SELECT 1
   FROM public.auth_roles('admin'::public.grant_capability) r(role_prefix, capability)
  WHERE ((directives.catalog_prefix)::text ^@ (r.role_prefix)::text))));


--
-- Name: draft_errors Users can access and delete errors of their drafts; Type: POLICY; Schema: public; Owner: postgres
--

CREATE POLICY "Users can access and delete errors of their drafts" ON public.draft_errors USING (((draft_id)::macaddr8 IN ( SELECT drafts.id
   FROM public.drafts
  WHERE (drafts.user_id = ( SELECT auth.uid() AS uid)))));


--
-- Name: applied_directives Users can access only their applied directives; Type: POLICY; Schema: public; Owner: postgres
--

CREATE POLICY "Users can access only their applied directives" ON public.applied_directives USING ((user_id = ( SELECT auth.uid() AS uid)));


--
-- Name: drafts Users can access only their created drafts; Type: POLICY; Schema: public; Owner: postgres
--

CREATE POLICY "Users can access only their created drafts" ON public.drafts USING ((user_id = ( SELECT auth.uid() AS uid)));


--
-- Name: evolutions Users can access only their initiated evolution operations; Type: POLICY; Schema: public; Owner: postgres
--

CREATE POLICY "Users can access only their initiated evolution operations" ON public.evolutions FOR SELECT USING ((user_id = ( SELECT auth.uid() AS uid)));


--
-- Name: publications Users can access only their initiated publish operations; Type: POLICY; Schema: public; Owner: postgres
--

CREATE POLICY "Users can access only their initiated publish operations" ON public.publications FOR SELECT USING ((user_id = ( SELECT auth.uid() AS uid)));


--
-- Name: refresh_tokens Users can access their own refresh tokens; Type: POLICY; Schema: public; Owner: postgres
--

CREATE POLICY "Users can access their own refresh tokens" ON public.refresh_tokens USING ((user_id = ( SELECT auth.uid() AS uid)));


--
-- Name: evolutions Users can insert evolutions from permitted drafts; Type: POLICY; Schema: public; Owner: postgres
--

CREATE POLICY "Users can insert evolutions from permitted drafts" ON public.evolutions FOR INSERT WITH CHECK (((draft_id)::macaddr8 IN ( SELECT drafts.id
   FROM public.drafts)));


--
-- Name: publications Users can insert publications from permitted drafts; Type: POLICY; Schema: public; Owner: postgres
--

CREATE POLICY "Users can insert publications from permitted drafts" ON public.publications FOR INSERT WITH CHECK (((draft_id)::macaddr8 IN ( SELECT drafts.id
   FROM public.drafts)));


--
-- Name: role_grants Users delete role grants where they admin the object or subject; Type: POLICY; Schema: public; Owner: postgres
--

CREATE POLICY "Users delete role grants where they admin the object or subject" ON public.role_grants FOR DELETE USING ((EXISTS ( SELECT 1
   FROM public.auth_roles('admin'::public.grant_capability) r(role_prefix, capability)
  WHERE (((role_grants.object_role)::text ^@ (r.role_prefix)::text) OR ((role_grants.subject_role)::text ^@ (r.role_prefix)::text)))));


--
-- Name: user_grants Users delete user grants they admin or are the subject; Type: POLICY; Schema: public; Owner: postgres
--

CREATE POLICY "Users delete user grants they admin or are the subject" ON public.user_grants FOR DELETE USING (((EXISTS ( SELECT 1
   FROM public.auth_roles('admin'::public.grant_capability) r(role_prefix, capability)
  WHERE ((user_grants.object_role)::text ^@ (r.role_prefix)::text))) OR (user_id = ( SELECT auth.uid() AS uid))));


--
-- Name: role_grants Users insert role grants where they admin the object; Type: POLICY; Schema: public; Owner: postgres
--

CREATE POLICY "Users insert role grants where they admin the object" ON public.role_grants FOR INSERT WITH CHECK ((EXISTS ( SELECT 1
   FROM public.auth_roles('admin'::public.grant_capability) r(role_prefix, capability)
  WHERE ((role_grants.object_role)::text ^@ (r.role_prefix)::text))));


--
-- Name: user_grants Users insert user grants they admin; Type: POLICY; Schema: public; Owner: postgres
--

CREATE POLICY "Users insert user grants they admin" ON public.user_grants FOR INSERT WITH CHECK ((EXISTS ( SELECT 1
   FROM public.auth_roles('admin'::public.grant_capability) r(role_prefix, capability)
  WHERE ((user_grants.object_role)::text ^@ (r.role_prefix)::text))));


--
-- Name: directives Users may select directives which they have applied; Type: POLICY; Schema: public; Owner: postgres
--

CREATE POLICY "Users may select directives which they have applied" ON public.directives FOR SELECT USING (((id)::macaddr8 IN ( SELECT applied_directives.directive_id
   FROM public.applied_directives)));


--
-- Name: controller_jobs Users must be authorized to live specifications; Type: POLICY; Schema: public; Owner: postgres
--

CREATE POLICY "Users must be authorized to live specifications" ON public.controller_jobs FOR SELECT USING (((live_spec_id)::macaddr8 IN ( SELECT live_specs.id
   FROM public.live_specs)));


--
-- Name: live_spec_flows Users must be authorized to one referenced specification; Type: POLICY; Schema: public; Owner: postgres
--

CREATE POLICY "Users must be authorized to one referenced specification" ON public.live_spec_flows FOR SELECT USING ((((source_id)::macaddr8 IN ( SELECT live_specs.id
   FROM public.live_specs)) OR ((target_id)::macaddr8 IN ( SELECT live_specs.id
   FROM public.live_specs))));


--
-- Name: catalog_stats Users must be authorized to the catalog name; Type: POLICY; Schema: public; Owner: stats_loader
--

CREATE POLICY "Users must be authorized to the catalog name" ON public.catalog_stats FOR SELECT USING ((EXISTS ( SELECT 1
   FROM public.auth_roles('read'::public.grant_capability) r(role_prefix, capability)
  WHERE ((catalog_stats.catalog_name)::text ^@ (r.role_prefix)::text))));


--
-- Name: old_catalog_stats Users must be authorized to the catalog name; Type: POLICY; Schema: public; Owner: postgres
--

CREATE POLICY "Users must be authorized to the catalog name" ON public.old_catalog_stats FOR SELECT USING ((EXISTS ( SELECT 1
   FROM public.auth_roles('read'::public.grant_capability) r(role_prefix, capability)
  WHERE ((old_catalog_stats.catalog_name)::text ^@ (r.role_prefix)::text))));


--
-- Name: inferred_schemas Users must be authorized to the collection name; Type: POLICY; Schema: public; Owner: stats_loader
--

CREATE POLICY "Users must be authorized to the collection name" ON public.inferred_schemas FOR SELECT USING ((EXISTS ( SELECT 1
   FROM public.auth_roles('read'::public.grant_capability) r(role_prefix, capability)
  WHERE ((inferred_schemas.collection_name)::text ^@ (r.role_prefix)::text))));


--
-- Name: storage_mappings Users must be authorized to the specification catalog prefix; Type: POLICY; Schema: public; Owner: postgres
--

CREATE POLICY "Users must be authorized to the specification catalog prefix" ON public.storage_mappings FOR SELECT USING ((EXISTS ( SELECT 1
   FROM public.auth_roles('read'::public.grant_capability) r(role_prefix, capability)
  WHERE ((storage_mappings.catalog_prefix)::text ^@ (r.role_prefix)::text))));


--
-- Name: tenants Users must be authorized to their catalog tenant; Type: POLICY; Schema: public; Owner: postgres
--

CREATE POLICY "Users must be authorized to their catalog tenant" ON public.tenants FOR SELECT USING ((EXISTS ( SELECT 1
   FROM public.auth_roles('admin'::public.grant_capability) r(role_prefix, capability)
  WHERE ((tenants.tenant)::text ^@ (r.role_prefix)::text))));


--
-- Name: data_planes Users must be read-authorized to data planes; Type: POLICY; Schema: public; Owner: postgres
--

CREATE POLICY "Users must be read-authorized to data planes" ON public.data_planes FOR SELECT USING ((EXISTS ( SELECT 1
   FROM public.auth_roles('read'::public.grant_capability) r(role_prefix, capability)
  WHERE ((data_planes.data_plane_name)::text ^@ (r.role_prefix)::text))));


--
-- Name: registered_avro_schemas Users must be read-authorized to the schema catalog name; Type: POLICY; Schema: public; Owner: postgres
--

CREATE POLICY "Users must be read-authorized to the schema catalog name" ON public.registered_avro_schemas USING ((EXISTS ( SELECT 1
   FROM public.auth_roles('read'::public.grant_capability) r(role_prefix, capability)
  WHERE ((registered_avro_schemas.catalog_name)::text ^@ (r.role_prefix)::text))));


--
-- Name: live_specs Users must be read-authorized to the specification catalog name; Type: POLICY; Schema: public; Owner: postgres
--

CREATE POLICY "Users must be read-authorized to the specification catalog name" ON public.live_specs FOR SELECT USING ((EXISTS ( SELECT 1
   FROM public.auth_roles('read'::public.grant_capability) r(role_prefix, capability)
  WHERE ((live_specs.catalog_name)::text ^@ (r.role_prefix)::text))));


--
-- Name: publication_specs Users must be read-authorized to the specification catalog name; Type: POLICY; Schema: public; Owner: postgres
--

CREATE POLICY "Users must be read-authorized to the specification catalog name" ON public.publication_specs FOR SELECT USING (((live_spec_id)::macaddr8 IN ( SELECT live_specs.id
   FROM public.live_specs)));


--
-- Name: role_grants Users select role grants where they admin the subject or object; Type: POLICY; Schema: public; Owner: postgres
--

CREATE POLICY "Users select role grants where they admin the subject or object" ON public.role_grants FOR SELECT USING ((EXISTS ( SELECT 1
   FROM public.auth_roles('admin'::public.grant_capability) r(role_prefix, capability)
  WHERE (((role_grants.object_role)::text ^@ (r.role_prefix)::text) OR ((role_grants.subject_role)::text ^@ (r.role_prefix)::text)))));


--
-- Name: user_grants Users select user grants they admin or are the subject; Type: POLICY; Schema: public; Owner: postgres
--

CREATE POLICY "Users select user grants they admin or are the subject" ON public.user_grants FOR SELECT USING (((EXISTS ( SELECT 1
   FROM public.auth_roles('admin'::public.grant_capability) r(role_prefix, capability)
  WHERE ((user_grants.object_role)::text ^@ (r.role_prefix)::text))) OR (user_id = ( SELECT auth.uid() AS uid))));


--
-- Name: role_grants Users update role grants where they admin the object; Type: POLICY; Schema: public; Owner: postgres
--

CREATE POLICY "Users update role grants where they admin the object" ON public.role_grants FOR UPDATE USING ((EXISTS ( SELECT 1
   FROM public.auth_roles('admin'::public.grant_capability) r(role_prefix, capability)
  WHERE ((role_grants.object_role)::text ^@ (r.role_prefix)::text))));


--
-- Name: user_grants Users update user grants they admin; Type: POLICY; Schema: public; Owner: postgres
--

CREATE POLICY "Users update user grants they admin" ON public.user_grants FOR UPDATE USING ((EXISTS ( SELECT 1
   FROM public.auth_roles('admin'::public.grant_capability) r(role_prefix, capability)
  WHERE ((user_grants.object_role)::text ^@ (r.role_prefix)::text))));


--
-- Name: alert_data_processing; Type: ROW SECURITY; Schema: public; Owner: postgres
--

ALTER TABLE public.alert_data_processing ENABLE ROW LEVEL SECURITY;

--
-- Name: alert_history; Type: ROW SECURITY; Schema: public; Owner: postgres
--

ALTER TABLE public.alert_history ENABLE ROW LEVEL SECURITY;

--
-- Name: alert_subscriptions; Type: ROW SECURITY; Schema: public; Owner: postgres
--

ALTER TABLE public.alert_subscriptions ENABLE ROW LEVEL SECURITY;

--
-- Name: applied_directives; Type: ROW SECURITY; Schema: public; Owner: postgres
--

ALTER TABLE public.applied_directives ENABLE ROW LEVEL SECURITY;

--
-- Name: catalog_stats; Type: ROW SECURITY; Schema: public; Owner: stats_loader
--

ALTER TABLE public.catalog_stats ENABLE ROW LEVEL SECURITY;

--
-- Name: controller_jobs; Type: ROW SECURITY; Schema: public; Owner: postgres
--

ALTER TABLE public.controller_jobs ENABLE ROW LEVEL SECURITY;

--
-- Name: data_planes; Type: ROW SECURITY; Schema: public; Owner: postgres
--

ALTER TABLE public.data_planes ENABLE ROW LEVEL SECURITY;

--
-- Name: directives; Type: ROW SECURITY; Schema: public; Owner: postgres
--

ALTER TABLE public.directives ENABLE ROW LEVEL SECURITY;

--
-- Name: discovers; Type: ROW SECURITY; Schema: public; Owner: postgres
--

ALTER TABLE public.discovers ENABLE ROW LEVEL SECURITY;

--
-- Name: draft_errors; Type: ROW SECURITY; Schema: public; Owner: postgres
--

ALTER TABLE public.draft_errors ENABLE ROW LEVEL SECURITY;

--
-- Name: draft_specs; Type: ROW SECURITY; Schema: public; Owner: postgres
--

ALTER TABLE public.draft_specs ENABLE ROW LEVEL SECURITY;

--
-- Name: drafts; Type: ROW SECURITY; Schema: public; Owner: postgres
--

ALTER TABLE public.drafts ENABLE ROW LEVEL SECURITY;

--
-- Name: evolutions; Type: ROW SECURITY; Schema: public; Owner: postgres
--

ALTER TABLE public.evolutions ENABLE ROW LEVEL SECURITY;

--
-- Name: inferred_schemas; Type: ROW SECURITY; Schema: public; Owner: stats_loader
--

ALTER TABLE public.inferred_schemas ENABLE ROW LEVEL SECURITY;

--
-- Name: live_spec_flows; Type: ROW SECURITY; Schema: public; Owner: postgres
--

ALTER TABLE public.live_spec_flows ENABLE ROW LEVEL SECURITY;

--
-- Name: live_specs; Type: ROW SECURITY; Schema: public; Owner: postgres
--

ALTER TABLE public.live_specs ENABLE ROW LEVEL SECURITY;

--
-- Name: tenants marketplace_integration can see all tenants; Type: POLICY; Schema: public; Owner: postgres
--

CREATE POLICY "marketplace_integration can see all tenants" ON public.tenants TO marketplace_integration USING (true) WITH CHECK (true);


--
-- Name: old_catalog_stats; Type: ROW SECURITY; Schema: public; Owner: postgres
--

ALTER TABLE public.old_catalog_stats ENABLE ROW LEVEL SECURITY;

--
-- Name: publication_specs; Type: ROW SECURITY; Schema: public; Owner: postgres
--

ALTER TABLE public.publication_specs ENABLE ROW LEVEL SECURITY;

--
-- Name: publications; Type: ROW SECURITY; Schema: public; Owner: postgres
--

ALTER TABLE public.publications ENABLE ROW LEVEL SECURITY;

--
-- Name: refresh_tokens; Type: ROW SECURITY; Schema: public; Owner: postgres
--

ALTER TABLE public.refresh_tokens ENABLE ROW LEVEL SECURITY;

--
-- Name: registered_avro_schemas; Type: ROW SECURITY; Schema: public; Owner: postgres
--

ALTER TABLE public.registered_avro_schemas ENABLE ROW LEVEL SECURITY;

--
-- Name: role_grants; Type: ROW SECURITY; Schema: public; Owner: postgres
--

ALTER TABLE public.role_grants ENABLE ROW LEVEL SECURITY;

--
-- Name: storage_mappings; Type: ROW SECURITY; Schema: public; Owner: postgres
--

ALTER TABLE public.storage_mappings ENABLE ROW LEVEL SECURITY;

--
-- Name: tenants; Type: ROW SECURITY; Schema: public; Owner: postgres
--

ALTER TABLE public.tenants ENABLE ROW LEVEL SECURITY;

--
-- Name: user_grants; Type: ROW SECURITY; Schema: public; Owner: postgres
--

ALTER TABLE public.user_grants ENABLE ROW LEVEL SECURITY;

--
-- Name: SCHEMA internal; Type: ACL; Schema: -; Owner: postgres
--

GRANT USAGE ON SCHEMA internal TO marketplace_integration;


--
-- Name: SCHEMA public; Type: ACL; Schema: -; Owner: postgres
--

REVOKE USAGE ON SCHEMA public FROM PUBLIC;
GRANT USAGE ON SCHEMA public TO anon;
GRANT USAGE ON SCHEMA public TO authenticated;
GRANT USAGE ON SCHEMA public TO service_role;
GRANT USAGE ON SCHEMA public TO gatsby_reader;
GRANT ALL ON SCHEMA public TO stats_loader;
GRANT USAGE ON SCHEMA public TO github_action_connector_refresh;
GRANT USAGE ON SCHEMA public TO marketplace_integration;
GRANT USAGE ON SCHEMA public TO wgd_automation;


--
-- Name: TABLE applied_directives; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.applied_directives TO service_role;
GRANT SELECT,DELETE ON TABLE public.applied_directives TO authenticated;
GRANT SELECT ON TABLE public.applied_directives TO reporting_user;


--
-- Name: COLUMN applied_directives.user_claims; Type: ACL; Schema: public; Owner: postgres
--

GRANT UPDATE(user_claims) ON TABLE public.applied_directives TO authenticated;


--
-- Name: TABLE directives; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.directives TO service_role;
GRANT ALL ON TABLE public.directives TO authenticated;
GRANT SELECT ON TABLE public.directives TO reporting_user;


--
-- Name: FUNCTION auth_roles(min_capability public.grant_capability); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.auth_roles(min_capability public.grant_capability) TO service_role;


--
-- Name: FUNCTION auth_uid(); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.auth_uid() TO service_role;


--
-- Name: FUNCTION billing_report_202308(billed_prefix public.catalog_prefix, billed_month timestamp with time zone, free_trial_range tstzrange); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.billing_report_202308(billed_prefix public.catalog_prefix, billed_month timestamp with time zone, free_trial_range tstzrange) TO service_role;


--
-- Name: FUNCTION create_refresh_token(multi_use boolean, valid_for interval, detail text); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.create_refresh_token(multi_use boolean, valid_for interval, detail text) TO service_role;


--
-- Name: FUNCTION draft_collections_eligible_for_deletion(capture_id public.flowid, draft_id public.flowid); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.draft_collections_eligible_for_deletion(capture_id public.flowid, draft_id public.flowid) TO service_role;


--
-- Name: FUNCTION exchange_directive_token(bearer_token uuid); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.exchange_directive_token(bearer_token uuid) TO service_role;


--
-- Name: FUNCTION gateway_auth_token(VARIADIC prefixes text[]); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.gateway_auth_token(VARIADIC prefixes text[]) TO service_role;


--
-- Name: FUNCTION generate_access_token(refresh_token_id public.flowid, secret text); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.generate_access_token(refresh_token_id public.flowid, secret text) TO service_role;


--
-- Name: FUNCTION generate_opengraph_value(opengraph_raw jsonb, opengraph_patch jsonb, field text); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.generate_opengraph_value(opengraph_raw jsonb, opengraph_patch jsonb, field text) TO service_role;


--
-- Name: FUNCTION prune_unchanged_draft_specs(prune_draft_id public.flowid); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.prune_unchanged_draft_specs(prune_draft_id public.flowid) TO service_role;


--
-- Name: FUNCTION republish_prefix(prefix public.catalog_prefix); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.republish_prefix(prefix public.catalog_prefix) TO service_role;


--
-- Name: FUNCTION tier_line_items(amount numeric, tiers integer[], name text, unit text); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.tier_line_items(amount numeric, tiers integer[], name text, unit text) TO service_role;


--
-- Name: FUNCTION user_info_summary(); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.user_info_summary() TO service_role;


--
-- Name: FUNCTION view_logs(bearer_token uuid); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.view_logs(bearer_token uuid) TO service_role;


--
-- Name: FUNCTION view_user_profile(bearer_user_id uuid); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.view_user_profile(bearer_user_id uuid) TO service_role;


--
-- Name: TABLE alert_data_processing; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.alert_data_processing TO service_role;
GRANT SELECT,INSERT,DELETE ON TABLE public.alert_data_processing TO authenticated;


--
-- Name: COLUMN alert_data_processing.evaluation_interval; Type: ACL; Schema: public; Owner: postgres
--

GRANT UPDATE(evaluation_interval) ON TABLE public.alert_data_processing TO authenticated;


--
-- Name: TABLE alert_subscriptions; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.alert_subscriptions TO service_role;
GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE public.alert_subscriptions TO authenticated;


--
-- Name: TABLE catalog_stats; Type: ACL; Schema: public; Owner: stats_loader
--

GRANT ALL ON TABLE public.catalog_stats TO service_role;
GRANT SELECT ON TABLE public.catalog_stats TO authenticated;


--
-- Name: TABLE catalog_stats_hourly; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.catalog_stats_hourly TO service_role;
GRANT SELECT ON TABLE public.catalog_stats_hourly TO wgd_automation;


--
-- Name: TABLE live_specs; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.live_specs TO service_role;
GRANT SELECT ON TABLE public.live_specs TO authenticated;
GRANT SELECT ON TABLE public.live_specs TO reporting_user;
GRANT SELECT ON TABLE public.live_specs TO wgd_automation;


--
-- Name: TABLE tenants; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.tenants TO service_role;
GRANT SELECT ON TABLE public.tenants TO authenticated;
GRANT SELECT ON TABLE public.tenants TO reporting_user;
GRANT SELECT,REFERENCES,UPDATE ON TABLE public.tenants TO marketplace_integration;


--
-- Name: TABLE billing_historicals; Type: ACL; Schema: internal; Owner: postgres
--

GRANT ALL ON TABLE internal.billing_historicals TO service_role;


--
-- Name: TABLE gcm_accounts; Type: ACL; Schema: internal; Owner: postgres
--

GRANT SELECT,INSERT,UPDATE ON TABLE internal.gcm_accounts TO marketplace_integration;


--
-- Name: TABLE manual_bills; Type: ACL; Schema: internal; Owner: postgres
--

GRANT SELECT,INSERT,UPDATE ON TABLE internal.manual_bills TO marketplace_integration;


--
-- Name: TABLE catalog_stats_daily; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.catalog_stats_daily TO service_role;
GRANT SELECT ON TABLE public.catalog_stats_daily TO wgd_automation;


--
-- Name: TABLE catalog_stats_monthly; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.catalog_stats_monthly TO service_role;


--
-- Name: TABLE user_profiles; Type: ACL; Schema: internal; Owner: postgres
--

GRANT SELECT ON TABLE internal.user_profiles TO authenticated;
GRANT SELECT ON TABLE internal.user_profiles TO reporting_user;


--
-- Name: TABLE alert_all; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.alert_all TO service_role;


--
-- Name: TABLE alert_history; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.alert_history TO service_role;
GRANT SELECT ON TABLE public.alert_history TO authenticated;


--
-- Name: TABLE role_grants; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.role_grants TO authenticated;
GRANT ALL ON TABLE public.role_grants TO service_role;
GRANT SELECT ON TABLE public.role_grants TO reporting_user;
GRANT ALL ON TABLE public.role_grants TO marketplace_integration;


--
-- Name: TABLE user_grants; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.user_grants TO service_role;
GRANT ALL ON TABLE public.user_grants TO authenticated;
GRANT SELECT ON TABLE public.user_grants TO reporting_user;


--
-- Name: TABLE combined_grants_ext; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.combined_grants_ext TO service_role;
GRANT SELECT ON TABLE public.combined_grants_ext TO authenticated;
GRANT SELECT ON TABLE public.combined_grants_ext TO reporting_user;


--
-- Name: TABLE connector_tags; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.connector_tags TO service_role;
GRANT SELECT ON TABLE public.connector_tags TO authenticated;
GRANT ALL ON TABLE public.connector_tags TO github_action_connector_refresh;
GRANT SELECT ON TABLE public.connector_tags TO gatsby_reader;
GRANT SELECT ON TABLE public.connector_tags TO reporting_user;


--
-- Name: TABLE connectors; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.connectors TO service_role;
GRANT SELECT ON TABLE public.connectors TO github_action_connector_refresh;
GRANT SELECT ON TABLE public.connectors TO gatsby_reader;
GRANT SELECT ON TABLE public.connectors TO reporting_user;


--
-- Name: COLUMN connectors.created_at; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT(created_at) ON TABLE public.connectors TO authenticated;


--
-- Name: COLUMN connectors.detail; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT(detail) ON TABLE public.connectors TO authenticated;


--
-- Name: COLUMN connectors.id; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT(id) ON TABLE public.connectors TO authenticated;


--
-- Name: COLUMN connectors.updated_at; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT(updated_at) ON TABLE public.connectors TO authenticated;


--
-- Name: COLUMN connectors.external_url; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT(external_url) ON TABLE public.connectors TO authenticated;


--
-- Name: COLUMN connectors.image_name; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT(image_name) ON TABLE public.connectors TO authenticated;


--
-- Name: COLUMN connectors.oauth2_client_id; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT(oauth2_client_id) ON TABLE public.connectors TO authenticated;


--
-- Name: COLUMN connectors.short_description; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT(short_description) ON TABLE public.connectors TO authenticated;


--
-- Name: COLUMN connectors.title; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT(title) ON TABLE public.connectors TO authenticated;


--
-- Name: COLUMN connectors.logo_url; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT(logo_url) ON TABLE public.connectors TO authenticated;


--
-- Name: COLUMN connectors.recommended; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT(recommended) ON TABLE public.connectors TO authenticated;


--
-- Name: TABLE controller_jobs; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.controller_jobs TO service_role;
GRANT SELECT ON TABLE public.controller_jobs TO reporting_user;


--
-- Name: TABLE data_planes; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.data_planes TO service_role;
GRANT SELECT ON TABLE public.data_planes TO reporting_user;


--
-- Name: COLUMN data_planes.created_at; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT(created_at) ON TABLE public.data_planes TO authenticated;


--
-- Name: COLUMN data_planes.id; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT(id) ON TABLE public.data_planes TO authenticated;


--
-- Name: COLUMN data_planes.updated_at; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT(updated_at) ON TABLE public.data_planes TO authenticated;


--
-- Name: COLUMN data_planes.data_plane_name; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT(data_plane_name) ON TABLE public.data_planes TO authenticated;


--
-- Name: COLUMN data_planes.data_plane_fqdn; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT(data_plane_fqdn) ON TABLE public.data_planes TO authenticated;


--
-- Name: COLUMN data_planes.ops_logs_name; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT(ops_logs_name) ON TABLE public.data_planes TO authenticated;


--
-- Name: COLUMN data_planes.ops_stats_name; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT(ops_stats_name) ON TABLE public.data_planes TO authenticated;


--
-- Name: COLUMN data_planes.broker_address; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT(broker_address) ON TABLE public.data_planes TO authenticated;


--
-- Name: COLUMN data_planes.reactor_address; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT(reactor_address) ON TABLE public.data_planes TO authenticated;


--
-- Name: COLUMN data_planes.config; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT(config) ON TABLE public.data_planes TO authenticated;


--
-- Name: COLUMN data_planes.status; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT(status) ON TABLE public.data_planes TO authenticated;


--
-- Name: COLUMN data_planes.aws_iam_user_arn; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT(aws_iam_user_arn) ON TABLE public.data_planes TO authenticated;


--
-- Name: COLUMN data_planes.cidr_blocks; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT(cidr_blocks) ON TABLE public.data_planes TO authenticated;


--
-- Name: COLUMN data_planes.gcp_service_account_email; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT(gcp_service_account_email) ON TABLE public.data_planes TO authenticated;


--
-- Name: TABLE discovers; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.discovers TO service_role;
GRANT SELECT ON TABLE public.discovers TO authenticated;
GRANT SELECT ON TABLE public.discovers TO reporting_user;


--
-- Name: COLUMN discovers.capture_name; Type: ACL; Schema: public; Owner: postgres
--

GRANT INSERT(capture_name) ON TABLE public.discovers TO authenticated;


--
-- Name: COLUMN discovers.connector_tag_id; Type: ACL; Schema: public; Owner: postgres
--

GRANT INSERT(connector_tag_id) ON TABLE public.discovers TO authenticated;


--
-- Name: COLUMN discovers.draft_id; Type: ACL; Schema: public; Owner: postgres
--

GRANT INSERT(draft_id) ON TABLE public.discovers TO authenticated;


--
-- Name: COLUMN discovers.endpoint_config; Type: ACL; Schema: public; Owner: postgres
--

GRANT INSERT(endpoint_config) ON TABLE public.discovers TO authenticated;


--
-- Name: COLUMN discovers.update_only; Type: ACL; Schema: public; Owner: postgres
--

GRANT INSERT(update_only) ON TABLE public.discovers TO authenticated;


--
-- Name: COLUMN discovers.data_plane_name; Type: ACL; Schema: public; Owner: postgres
--

GRANT INSERT(data_plane_name) ON TABLE public.discovers TO authenticated;


--
-- Name: TABLE draft_errors; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT,DELETE ON TABLE public.draft_errors TO authenticated;
GRANT ALL ON TABLE public.draft_errors TO service_role;
GRANT SELECT ON TABLE public.draft_errors TO reporting_user;


--
-- Name: TABLE draft_specs; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.draft_specs TO service_role;
GRANT ALL ON TABLE public.draft_specs TO authenticated;
GRANT SELECT ON TABLE public.draft_specs TO reporting_user;


--
-- Name: TABLE drafts; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.drafts TO service_role;
GRANT SELECT,DELETE ON TABLE public.drafts TO authenticated;
GRANT SELECT ON TABLE public.drafts TO reporting_user;


--
-- Name: COLUMN drafts.detail; Type: ACL; Schema: public; Owner: postgres
--

GRANT INSERT(detail),UPDATE(detail) ON TABLE public.drafts TO authenticated;


--
-- Name: TABLE inferred_schemas; Type: ACL; Schema: public; Owner: stats_loader
--

GRANT ALL ON TABLE public.inferred_schemas TO service_role;
GRANT SELECT ON TABLE public.inferred_schemas TO authenticated;


--
-- Name: TABLE publication_specs; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.publication_specs TO service_role;
GRANT SELECT ON TABLE public.publication_specs TO authenticated;
GRANT SELECT ON TABLE public.publication_specs TO reporting_user;


--
-- Name: TABLE live_specs_ext; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.live_specs_ext TO service_role;
GRANT SELECT ON TABLE public.live_specs_ext TO reporting_user;
GRANT SELECT ON TABLE public.live_specs_ext TO authenticated;


--
-- Name: TABLE draft_specs_ext; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.draft_specs_ext TO service_role;
GRANT SELECT ON TABLE public.draft_specs_ext TO reporting_user;
GRANT SELECT ON TABLE public.draft_specs_ext TO authenticated;


--
-- Name: TABLE drafts_ext; Type: ACL; Schema: public; Owner: authenticated
--

GRANT ALL ON TABLE public.drafts_ext TO service_role;
GRANT SELECT ON TABLE public.drafts_ext TO reporting_user;


--
-- Name: TABLE evolutions; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.evolutions TO service_role;
GRANT SELECT ON TABLE public.evolutions TO authenticated;


--
-- Name: COLUMN evolutions.detail; Type: ACL; Schema: public; Owner: postgres
--

GRANT INSERT(detail) ON TABLE public.evolutions TO authenticated;


--
-- Name: COLUMN evolutions.draft_id; Type: ACL; Schema: public; Owner: postgres
--

GRANT INSERT(draft_id) ON TABLE public.evolutions TO authenticated;


--
-- Name: COLUMN evolutions.collections; Type: ACL; Schema: public; Owner: postgres
--

GRANT INSERT(collections) ON TABLE public.evolutions TO authenticated;


--
-- Name: TABLE flow_watermarks; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.flow_watermarks TO service_role;
GRANT ALL ON TABLE public.flow_watermarks TO reporting_user;


--
-- Name: TABLE invoices_ext; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.invoices_ext TO service_role;
GRANT SELECT ON TABLE public.invoices_ext TO authenticated;


--
-- Name: TABLE live_spec_flows; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.live_spec_flows TO service_role;
GRANT SELECT ON TABLE public.live_spec_flows TO reporting_user;
GRANT SELECT ON TABLE public.live_spec_flows TO authenticated;


--
-- Name: TABLE lock_monitor; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.lock_monitor TO service_role;
GRANT SELECT ON TABLE public.lock_monitor TO reporting_user;


--
-- Name: TABLE old_catalog_stats; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.old_catalog_stats TO service_role;
GRANT SELECT ON TABLE public.old_catalog_stats TO authenticated;
GRANT SELECT ON TABLE public.old_catalog_stats TO reporting_user;


--
-- Name: TABLE publication_specs_ext; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.publication_specs_ext TO service_role;
GRANT SELECT ON TABLE public.publication_specs_ext TO reporting_user;
GRANT SELECT ON TABLE public.publication_specs_ext TO authenticated;


--
-- Name: TABLE publications; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.publications TO service_role;
GRANT SELECT ON TABLE public.publications TO authenticated;
GRANT SELECT ON TABLE public.publications TO reporting_user;


--
-- Name: COLUMN publications.detail; Type: ACL; Schema: public; Owner: postgres
--

GRANT INSERT(detail) ON TABLE public.publications TO authenticated;


--
-- Name: COLUMN publications.draft_id; Type: ACL; Schema: public; Owner: postgres
--

GRANT INSERT(draft_id) ON TABLE public.publications TO authenticated;


--
-- Name: COLUMN publications.dry_run; Type: ACL; Schema: public; Owner: postgres
--

GRANT INSERT(dry_run) ON TABLE public.publications TO authenticated;


--
-- Name: COLUMN publications.data_plane_name; Type: ACL; Schema: public; Owner: postgres
--

GRANT INSERT(data_plane_name) ON TABLE public.publications TO authenticated;


--
-- Name: TABLE refresh_tokens; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.refresh_tokens TO service_role;
GRANT DELETE ON TABLE public.refresh_tokens TO authenticated;
GRANT SELECT ON TABLE public.refresh_tokens TO reporting_user;


--
-- Name: COLUMN refresh_tokens.created_at; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT(created_at) ON TABLE public.refresh_tokens TO authenticated;


--
-- Name: COLUMN refresh_tokens.detail; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT(detail),UPDATE(detail) ON TABLE public.refresh_tokens TO authenticated;


--
-- Name: COLUMN refresh_tokens.id; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT(id) ON TABLE public.refresh_tokens TO authenticated;


--
-- Name: COLUMN refresh_tokens.updated_at; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT(updated_at) ON TABLE public.refresh_tokens TO authenticated;


--
-- Name: COLUMN refresh_tokens.user_id; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT(user_id) ON TABLE public.refresh_tokens TO authenticated;


--
-- Name: COLUMN refresh_tokens.multi_use; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT(multi_use),UPDATE(multi_use) ON TABLE public.refresh_tokens TO authenticated;


--
-- Name: COLUMN refresh_tokens.valid_for; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT(valid_for),UPDATE(valid_for) ON TABLE public.refresh_tokens TO authenticated;


--
-- Name: COLUMN refresh_tokens.uses; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT(uses) ON TABLE public.refresh_tokens TO authenticated;


--
-- Name: TABLE registered_avro_schemas; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.registered_avro_schemas TO service_role;
GRANT SELECT ON TABLE public.registered_avro_schemas TO reporting_user;
GRANT SELECT ON TABLE public.registered_avro_schemas TO authenticated;


--
-- Name: COLUMN registered_avro_schemas.updated_at; Type: ACL; Schema: public; Owner: postgres
--

GRANT UPDATE(updated_at) ON TABLE public.registered_avro_schemas TO authenticated;


--
-- Name: COLUMN registered_avro_schemas.avro_schema; Type: ACL; Schema: public; Owner: postgres
--

GRANT INSERT(avro_schema) ON TABLE public.registered_avro_schemas TO authenticated;


--
-- Name: COLUMN registered_avro_schemas.catalog_name; Type: ACL; Schema: public; Owner: postgres
--

GRANT INSERT(catalog_name) ON TABLE public.registered_avro_schemas TO authenticated;


--
-- Name: SEQUENCE registered_avro_schemas_registry_id_seq; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON SEQUENCE public.registered_avro_schemas_registry_id_seq TO service_role;
GRANT USAGE ON SEQUENCE public.registered_avro_schemas_registry_id_seq TO authenticated;


--
-- Name: TABLE storage_mappings; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.storage_mappings TO service_role;
GRANT SELECT ON TABLE public.storage_mappings TO authenticated;
GRANT SELECT ON TABLE public.storage_mappings TO reporting_user;


--
-- Name: TABLE test_publication_specs_ext; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.test_publication_specs_ext TO service_role;
GRANT SELECT ON TABLE public.test_publication_specs_ext TO reporting_user;
GRANT SELECT ON TABLE public.test_publication_specs_ext TO authenticated;


--
-- Name: TABLE unchanged_draft_specs; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.unchanged_draft_specs TO service_role;
GRANT SELECT ON TABLE public.unchanged_draft_specs TO reporting_user;
GRANT SELECT ON TABLE public.unchanged_draft_specs TO authenticated;


--
-- Name: DEFAULT PRIVILEGES FOR SEQUENCES; Type: DEFAULT ACL; Schema: public; Owner: postgres
--

ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public GRANT ALL ON SEQUENCES TO postgres;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public GRANT ALL ON SEQUENCES TO service_role;


--
-- Name: DEFAULT PRIVILEGES FOR SEQUENCES; Type: DEFAULT ACL; Schema: public; Owner: supabase_admin
--



--
-- Name: DEFAULT PRIVILEGES FOR FUNCTIONS; Type: DEFAULT ACL; Schema: public; Owner: postgres
--

ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public GRANT ALL ON FUNCTIONS TO postgres;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public GRANT ALL ON FUNCTIONS TO service_role;


--
-- Name: DEFAULT PRIVILEGES FOR FUNCTIONS; Type: DEFAULT ACL; Schema: public; Owner: supabase_admin
--



--
-- Name: DEFAULT PRIVILEGES FOR TABLES; Type: DEFAULT ACL; Schema: public; Owner: postgres
--

ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public GRANT ALL ON TABLES TO postgres;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public GRANT ALL ON TABLES TO service_role;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public GRANT SELECT ON TABLES TO reporting_user;


--
-- Name: DEFAULT PRIVILEGES FOR TABLES; Type: DEFAULT ACL; Schema: public; Owner: supabase_admin
--



--
-- PostgreSQL database dump complete
--

