-- We require that the following database roles are already created:
--  * An anonymous role, used by PostgREST when no credential is supplied.
--    CREATE ROLE api_anon nologin;
--  * A role used by PostgREST after verifying a JWT with role:"api_user".
--    CREATE ROLE api_user nologin;
--  * Role PostgREST uses to swith to another role post-authentication.
--    CREATE ROLE authenticator noinherit LOGIN PASSWORD 'SomeSecretPasswordChangeMe';
--
-- We also require the following grants to allow PostgREST to switch to api_anon/api_user:
--    GRANT api_anon TO authenticator;
--    GRANT api_user TO authenticator;
--
-- Roles are shared across an entire physical database, rather than being
-- encapsulated within a logical database, so they don't play super well
-- with infrastructure for creating and reverting lots of isolated copies
-- of our schemas for testing purposes.


-- flowid is a montonic, time-ordered ID with gaps that fits within 64 bits.
-- We use macaddr8 as its underlying storage type because:
--  * It's stored as exactly 8 bytes, with the same efficiency as BIGINT.
--  * It has a flexible, convienient to_json() behavior that (crucially)
--    is loss-less by default when parsed in JavaScript.
--    Postgres's to_json() serializes BIGINT as a bare integer,
--    which is subject to silent rounding by many parsers when values
--    exceed 53 bits (as is common with flowid).
--
-- The canonical encoding is lower-case hexidecimal with each byte
-- separated by ':', which is what's returned by Postgres & PostgREST.
-- Postgres (and PostgREST!) will accept any hex value of the correct
-- implied length, with bytes optionally separated by any arrangement
-- of ':' or '-'.
CREATE DOMAIN flowid AS macaddr8;
COMMENT ON DOMAIN flowid IS 'flowid is the common unique ID type of the Flow API';

CREATE DOMAIN catalog_name AS TEXT
  CONSTRAINT "Must be NFKC letters, numbers, -, _, ., separated by / and not end in /"
  CHECK (VALUE ~ '^([[:alpha:][:digit:]\-_.]+/)+[[:alpha:][:digit:]\-_.]+$' AND VALUE IS NFKC NORMALIZED);
COMMENT ON DOMAIN catalog_name IS 'catalog_name is a unique name within the Flow catalog namespace';

CREATE DOMAIN catalog_prefix AS TEXT
  CONSTRAINT "Must be NFKC letters, numbers, -, _, ., separated by / and end in /"
  CHECK (VALUE ~ '^([[:alpha:][:digit:]\-_.]+/)+$' AND VALUE IS NFKC NORMALIZED);
COMMENT ON DOMAIN catalog_prefix IS 'catalog_prefix is a unique prefix within the Flow catalog namespace';

CREATE DOMAIN json_obj AS JSON CHECK (json_typeof(VALUE) = 'object');
COMMENT ON DOMAIN json_obj IS 'json_obj is JSON which is restricted to the "object" type';

CREATE DOMAIN jsonb_obj AS JSONB CHECK (jsonb_typeof(VALUE) = 'object');
COMMENT ON DOMAIN jsonb_obj IS 'jsonb_obj is JSONB which is restricted to the "object" type';

CREATE TYPE jwt_access_token AS (
  "issuer" TEXT,
  "subject" TEXT,
  "issued_at" TIMESTAMPTZ,
  "expires_at" TIMESTAMPTZ
);
COMMENT ON TYPE jwt_access_token IS 'jwt_access_token is the claims of a JWT access token';

CREATE TYPE jwt_id_token AS (
  "access" jwt_access_token,

  "avatar_url" TEXT,
  "display_name" TEXT,
  "first_name" TEXT,
  "locale" TEXT,
  "organizations" TEXT[],
  "verified_email" TEXT
);
COMMENT ON TYPE jwt_id_token IS 'jwt_id_token is the claims of a JWT identity token, which is a super-set of a JWT access token.';


CREATE FUNCTION auth_access()
RETURNS jwt_access_token AS $$
DECLARE
  claims JSONB := current_setting('request.jwt.claims');
  raw_sub TEXT;
  pipe_ind INTEGER;
  issued_at  TIMESTAMPTZ;
  expires_at TIMESTAMPTZ;
BEGIN

  IF jsonb_path_match(claims, '$.role == "api_anon"') THEN
    RETURN NULL;
  ELSIF jsonb_path_match(claims, '$.role != "api_user"') THEN
    RAISE EXCEPTION 'expected "role" of "api_anon" or "api_user" in claims %', claims
      USING HINT = 'Please supply a valid Authorization: Bearer token with your request';
  END IF;

  -- `sub` of the claim is a composite of the issuer and issuer's subject for the user.
  -- For example, `google|100280645810032728608`.
  raw_sub = jsonb_path_query(claims, 'strict $.sub')->>0;
  pipe_ind = position('|' in raw_sub);
  issued_at  = to_timestamp(jsonb_path_query(claims, 'strict $.iat')::INTEGER);
  expires_at = to_timestamp(jsonb_path_query(claims, 'strict $.exp')::INTEGER);

  RETURN (
    substring(raw_sub for pipe_ind - 1),
    substring(raw_sub from pipe_ind + 1),
    issued_at,
    expires_at
  );
END;
$$ LANGUAGE PLPGSQL STABLE;
COMMENT ON FUNCTION auth_access IS 'auth_access returns a parsed jwt_access_token of the current user claims, which may be either an access or ID token';


CREATE FUNCTION auth_id()
RETURNS jwt_id_token AS $$
DECLARE
  claims JSONB := current_setting('request.jwt.claims');
  access_token jwt_access_token := auth_access();
  ext JSONB ;
BEGIN
  IF access_token IS NULL THEN
    RETURN NULL;
  END IF;

  ext = jsonb_path_query(current_setting('request.jwt.claims')::jsonb, '$.ext');
  IF ext IS NULL THEN
    RAISE EXCEPTION 'expected "ext" to be present in claims %', claims
      USING HINT = 'Please ensure the Authorization: Bearer token is an ID token (and not an access token)';
  END IF;

  RETURN (
    access_token,
    jsonb_path_query(ext, '$.avatarURL')->>0,
    jsonb_path_query(ext, 'strict $.displayName')->>0,
    jsonb_path_query(ext, '$.firstName')->>0,
    jsonb_path_query(ext, '$.locale')->>0,
    ARRAY(SELECT jsonb_array_elements_text(jsonb_path_query(ext, '$.orgs'))),
    jsonb_path_query(ext, 'strict $.email')->>0
  );
END;
$$ LANGUAGE PLPGSQL STABLE;
COMMENT ON FUNCTION auth_id IS 'auth_id returns a parsed jwt_id_token of the current user claims, which must be an identity token';


-- There's a circular dependency where `accounts` and `credentials` row-level
-- security policies each depend on this function being defined. This is fine,
-- because the body of this function isn't evaluated until after those tables
-- have been created.
CREATE FUNCTION auth_account_id()
RETURNS flowid AS $$
DECLARE
  t jwt_access_token := auth_access();
  account_id flowid;
BEGIN
  SELECT c.account_id INTO account_id
  FROM credentials AS c
  WHERE issuer = t.issuer AND subject = t.subject;

  IF FOUND THEN
    RETURN account_id;
  END IF;

  IF t IS NULL THEN
    RAISE EXCEPTION 'missing credential'
      USING DETAIL = 'This API may not be accessed without authentication',
            HINT = 'Ensure your request includes a valid Authorization: Bearer token';
  ELSE
    RAISE EXCEPTION 'credential not found'
      USING DETAIL = FORMAT('No matching credential for issuer:%s and subject:%s', t.issuer, t.subject),
            HINT = 'Try /rpc/auth_session to create an account and credential';
  END IF;

END;
$$ LANGUAGE PLPGSQL
SECURITY DEFINER
STABLE
;
COMMENT ON FUNCTION auth_account_id IS 'auth_account_id maps the user''s auth_access() token to a current, credentialed account';


-- id_generator produces 64bit unique, non-sequential identifiers. They:
--  * Have fixed storage that's 1/2 the size of a UUID.
--  * Have a monotonic generation order.
--  * Embed a wall-clock timestamp than can be extracted if needed.
--  * Avoid the leaky-ness of SERIAL id's.
--
-- Adapted from: https://rob.conery.io/2014/05/29/a-better-id-generator-for-postgresql/
-- Which itself was inspired by http://instagram-engineering.tumblr.com/post/10853187575/sharding-ids-at-instagram
CREATE SEQUENCE shard_0_id_sequence;
GRANT USAGE, SELECT ON SEQUENCE shard_0_id_sequence TO api_user;

CREATE FUNCTION id_generator()
RETURNS flowid AS $$
DECLARE
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
BEGIN
    -- We have 13 low bits of sequence ID, which allow us to generate
    -- up to 8,192 unique IDs within each given millisecond.
    SELECT nextval('shard_0_id_sequence') % 8192 INTO seq_no;

    SELECT FLOOR((EXTRACT(EPOCH FROM clock_timestamp()) - estuary_epoch) * 1000) INTO now_millis;
    RETURN LPAD(TO_HEX((now_millis << 23) | (seq_no << 10) | (shard_id)), 16, '0')::flowid;
END;
$$ LANGUAGE PLPGSQL;

-- Set id_generator as the DEFAULT value of a flowid whenever it's used in a table.
ALTER DOMAIN flowid SET DEFAULT id_generator();


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


-- Model table is not used directly, but is a model for other created tables.
CREATE TABLE _model (
  "created_at" TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  "description" TEXT,
  "id" flowid PRIMARY KEY NOT NULL,
  "updated_at" TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE _model IS 'Model table for the creation of other tables';
COMMENT ON COLUMN _model.created_at IS 'Time at which the record was created';
COMMENT ON COLUMN _model.description IS 'Description of the record';
COMMENT ON COLUMN _model.id IS 'ID of the record';
COMMENT ON COLUMN _model.updated_at IS 'Time at which the record was last updated';


-- Known connectors.
CREATE TABLE connectors (
  LIKE _model INCLUDING ALL,
  "image" TEXT UNIQUE NOT NULL
  -- TODO(johnny): constraint that "image" is a reasonable-looking docker image name?
);
-- api_user may select all connectors without restrictions.
GRANT SELECT ON TABLE connectors TO api_user;

-- Known connector images.
CREATE TABLE connector_images (
  LIKE _model INCLUDING ALL,
  "connector_id" flowid NOT NULL REFERENCES connectors("id"),
  "tag" TEXT NOT NULL,
  "state" jsonb_obj NOT NULL DEFAULT '{"type":"queued"}',
  "logs_token" UUID NOT NULL DEFAULT gen_random_uuid(),
  --
  CONSTRAINT "tag must start with : (as in :latest) or @sha256:<hash>"
    CHECK ("tag" LIKE ':%' OR "tag" LIKE '@sha256:')
);
-- api_user may select all connector_images without restrictions.
GRANT SELECT ON TABLE connector_images TO api_user;

CREATE INDEX idx_connector_images_connector_id ON connector_images("connector_id");

CREATE UNIQUE INDEX idx_connector_images_id_where_queued ON connector_images USING BTREE ("id")
WHERE "state"->>'type' = 'queued';

-- Accounts within the control plane.
-- This table is minimal, as most user information (their name, email, avatar),
-- is provided by associated credentials.
CREATE TABLE accounts (
  LIKE _model INCLUDING ALL
);
-- api_user may see (only) their own account.
ALTER TABLE accounts ENABLE ROW LEVEL SECURITY;
CREATE POLICY accounts_auth ON accounts USING ("id" = auth_account_id());
-- api_user may select all columns of their own account.
GRANT SELECT ON accounts TO api_user;

COMMENT ON TABLE accounts IS 'Accounts of the Estuary platform';

-- Credentials known to the control plane.
CREATE TABLE credentials (
  LIKE _model INCLUDING ALL,

  "account_id" flowid NOT NULL REFERENCES accounts("id"),
  "avatar_url" TEXT,
  "display_name" TEXT NOT NULL,
  "expires_at" TIMESTAMPTZ NOT NULL,
  "first_name" TEXT,
  "issuer" TEXT NOT NULL, -- For example, 'google' or 'github'.
  "locale" TEXT,
  "organizations" TEXT[],
  "subject" TEXT NOT NULL, -- Issuer's user ID, '109158819594457949823'.
  "verified_email" TEXT NOT NULL
);
-- api_user may see (only) their own account.
ALTER TABLE credentials ENABLE ROW LEVEL SECURITY;
CREATE POLICY credentials_auth ON credentials USING ("account_id" = auth_account_id());
-- api_user may select all columns of their own credentials.
GRANT SELECT ON credentials TO api_user;

COMMENT ON TABLE credentials IS 'Credentials of Estuary platform accounts';
COMMENT ON COLUMN credentials.account_id IS 'Account which is authenticated by this credential';
COMMENT ON COLUMN credentials.avatar_url IS 'User avatar (image) URL';
COMMENT ON COLUMN credentials.display_name IS 'User''s name for display';
COMMENT ON COLUMN credentials.expires_at IS 'Expiry of this credential''s last token';
COMMENT ON COLUMN credentials.first_name IS 'User''s first name, if known';
COMMENT ON COLUMN credentials.issuer IS 'Third-party issuer of the credential';
COMMENT ON COLUMN credentials.locale IS 'User''s locale, if known';
COMMENT ON COLUMN credentials.organizations IS 'User''s organizations within the third-party credential provider';
COMMENT ON COLUMN credentials.subject IS 'User''s unique ID within the third-party credential provider';
COMMENT ON COLUMN credentials.verified_email IS 'User''s email, as verified by the third-party credential provider';

CREATE UNIQUE INDEX idx_credentials_issuer_subject ON credentials("issuer", "subject");
CREATE INDEX idx_credentials_account_id ON credentials("account_id");


-- auth_session is invoked to create or update an authenticated account session.
CREATE FUNCTION auth_session()
RETURNS JSON AS $$
DECLARE
  t jwt_id_token := auth_id();
  account_id flowid;
BEGIN

  IF t IS NULL THEN
    RAISE EXCEPTION 'credential is missing'
      USING HINT = 'Please supply a valid Authorization: Bearer token with your request';
  END IF;

  -- Attempt to update an existing credential.
  UPDATE credentials AS c SET
    avatar_url = t.avatar_url,
    display_name = t.display_name,
    expires_at = (t.access).expires_at,
    first_name = t.first_name,
    locale = t.locale,
    organizations = t.organizations,
    updated_at = NOW(),
    verified_email = t.verified_email
  WHERE issuer = (t.access).issuer AND subject = (t.access).subject
  RETURNING c.account_id INTO account_id;

  -- Did we find and update a credential? If so, touch its account and return.
  IF FOUND THEN
    UPDATE accounts AS a SET updated_at = NOW() WHERE a.id = account_id;

    RETURN json_build_object(
      'account_id', account_id,
      'expires_at', (t.access).expires_at,
      'status', 'updated'
    );
  END IF;

  -- We must create a new account and credential.
  INSERT INTO accounts (id) VALUES (DEFAULT)
  RETURNING id INTO account_id;

  INSERT INTO credentials (
    account_id,
    avatar_url,
    display_name,
    expires_at,
    first_name,
    issuer,
    locale,
    organizations,
    subject,
    verified_email
  ) VALUES (
    account_id,
    t.avatar_url,
    t.display_name,
    (t.access).expires_at,
    t.first_name,
    (t.access).issuer,
    t.locale,
    t.organizations,
    (t.access).subject,
    t.verified_email
  );

  RETURN json_build_object(
    'account_id', account_id,
    'expires_at', (t.access).expires_at,
    'status', 'created'
  );
END;
$$ LANGUAGE PLPGSQL
SECURITY DEFINER;
COMMENT ON FUNCTION auth_session IS 'auth_session updates or creates a credential and account for the user''s current auth_id claims.';


-- Logs are newline-delimited outputs from server-side jobs.
CREATE TABLE logs (
  "token" UUID NOT NULL,
  "stream" TEXT NOT NULL,
  "logged_at" TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  "line" TEXT NOT NULL
);
-- api_user may *not* directly select from logs.
-- Instead, they must present a bearer token which is matched to select from a specific set of logs.

COMMENT ON TABLE logs IS 'Logs produced by Flow';
COMMENT ON COLUMN logs.token IS 'Bearer token which demarks and provides accesss to a set of logs';
COMMENT ON COLUMN logs.stream IS 'Identifier of the log stream within the job';
COMMENT ON COLUMN logs.logged_at IS 'Time at which the log was collected';
COMMENT ON COLUMN logs.line IS 'Logged line';

CREATE INDEX logs_token_logged_at ON logs USING BRIN(token, logged_at) WITH (autosummarize = ON);


-- We cannot provide direct SELECT access to logs, but we *can* provide
-- a view on logs so long as the user always provides a bearer token.
CREATE FUNCTION view_logs(bearer_token UUID)
RETURNS SETOF logs AS $$
BEGIN
  RETURN QUERY SELECT * FROM logs WHERE logs.token = bearer_token;
END;
$$ LANGUAGE PLPGSQL
SECURITY DEFINER
;
COMMENT ON FUNCTION view_logs IS 'view_logs returns logs of the provided logs token';


-- User-initiated discover operations.
CREATE TABLE discovers (
  LIKE _model INCLUDING ALL,
  "account_id" flowid NOT NULL REFERENCES accounts("id"),
  "capture_name" catalog_name NOT NULL,
  "endpoint_config" json_obj NOT NULL,
  "image_id" flowid NOT NULL REFERENCES connector_images("id"),
  "logs_token" UUID NOT NULL DEFAULT gen_random_uuid(),
  "state" jsonb_obj NOT NULL DEFAULT '{"type":"queued"}'
);
-- api_user may see (only) their own discovers.
ALTER TABLE discovers ENABLE ROW LEVEL SECURITY;
CREATE POLICY discovers_auth ON discovers USING ("account_id" = auth_account_id());
-- api_user may select all columns of their owned discovers.
GRANT SELECT ON discovers TO api_user;
-- api_user may insert new discovers under their account ID.
GRANT INSERT ("account_id","capture_name","endpoint_config","image_id") ON discovers TO api_user;

COMMENT ON TABLE discovers IS 'Connector discovery operations';
COMMENT ON COLUMN discovers.account_id IS 'Account which created this discovery operation';
COMMENT ON COLUMN discovers.capture_name IS 'Intended name of the capture produced by this discover';
COMMENT ON COLUMN discovers.endpoint_config IS 'Endpoint configuration of the connector. May be protected by sops';
COMMENT ON COLUMN discovers.image_id IS 'Connector image which is used for discovery';
COMMENT ON COLUMN discovers.logs_token IS 'Bearer token for accessing logs of this discovery operation';
COMMENT ON COLUMN discovers.state IS 'State of this discover';

CREATE INDEX idx_discovers_account_id ON discovers USING BTREE ("account_id");


-- User-initiated builds.
-- TODO(johnny): Rename to drafts?
CREATE TABLE builds (
  LIKE _model INCLUDING ALL,
  "account_id" flowid NOT NULL REFERENCES accounts("id"),
  "hidden" BOOL NOT NULL DEFAULT FALSE,
  "logs_token" UUID NOT NULL DEFAULT gen_random_uuid(),
  "spec" json_obj NOT NULL,
  "state" jsonb_obj NOT NULL DEFAULT '{"type":"queued"}'
);
-- api_user may see (only) their own builds.
ALTER TABLE builds ENABLE ROW LEVEL SECURITY;
CREATE POLICY builds_auth ON builds USING ("account_id" = auth_account_id());
-- api_user may select all columns of their owned builds.
GRANT SELECT ON builds TO api_user;
-- api_user may insert new builds under their account ID.
GRANT INSERT ("account_id","spec","hidden") ON builds TO api_user;

COMMENT ON TABLE builds IS 'Builds of user catalogs';
COMMENT ON COLUMN builds.account_id IS 'Account which created this build';
COMMENT ON COLUMN builds.spec IS 'Flow catalog specification of this build';
COMMENT ON COLUMN builds.hidden IS 'Hide this build by default';
COMMENT ON COLUMN builds.logs_token IS 'Bearer token for accessing logs of this build';
COMMENT ON COLUMN builds.state IS 'State of this build';

CREATE INDEX idx_builds_account_id ON builds USING BTREE ("account_id");

-- Index for efficiently identifying builds that are queued,
-- which is a small subset of the overall builds that exist.
CREATE UNIQUE INDEX idx_builds_id_where_queued ON builds USING BTREE ("id")
WHERE "state"->>'type' = 'queued';


-- Seed with some initial data.
CREATE PROCEDURE seed_data()
AS $$
DECLARE
  connector_id flowid;
BEGIN

  INSERT INTO connectors ("image", "description") VALUES (
    'ghcr.io/estuary/source-hello-world',
    'A flood of greetings'
  )
  RETURNING id STRICT INTO connector_id;
  INSERT INTO connector_images ("connector_id", "tag") VALUES (connector_id, ':01fb856');

  INSERT INTO connectors ("image", "description") VALUES (
    'ghcr.io/estuary/source-postgres',
    'CDC connector for PostgreSQL'
  )
  RETURNING id STRICT INTO connector_id;
  INSERT INTO connector_images ("connector_id", "tag") VALUES (connector_id, ':f1bd86a');

  INSERT INTO connectors ("image", "description") VALUES (
    'ghcr.io/estuary/materialize-postgres',
    'Materialize views into PostgreSQL'
  )
  RETURNING id STRICT INTO connector_id;
  INSERT INTO connector_images ("connector_id", "tag") VALUES (connector_id, ':898776b');

END;
$$ LANGUAGE PLPGSQL;

CALL seed_data();
DROP PROCEDURE seed_data;