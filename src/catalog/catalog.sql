PRAGMA foreign_keys = ON;

-- resources enumerates the unique resources (eg, files) from which this catalog
-- is built. It exists to facilitate the tracking of derived catalog entities back
-- to the authoritative resources which produced them, such as when outputting
-- descriptive error messages from an encountered build error.
CREATE TABLE resources
(
    id  INTEGER PRIMARY KEY NOT NULL,
    -- Canonical URI of this resource. Eg `file:///local/file/path` or `https://remote.host/path`.
    uri TEXT UNIQUE         NOT NULL
);

CREATE TABLE resource_imports
(
    -- ID of resource which imports another resource.
    source_id INTEGER NOT NULL REFERENCES resources,
    -- ID of the imported resource.
    import_id INTEGER NOT NULL REFERENCES resources,

    PRIMARY KEY (source_id, import_id)
);

-- View which derives all transitive resource imports
CREATE VIEW resource_transitive_imports AS
WITH RECURSIVE cte(source_id, import_id) AS (
    SELECT id, id
    FROM resources
    UNION ALL
    SELECT cte.source_id, ri.import_id
    FROM resource_imports AS ri
             JOIN cte ON ri.source_id = cte.import_id
)
SELECT *
FROM cte;

CREATE TRIGGER assert_resource_imports_are_acyclic
    BEFORE INSERT
    ON resource_imports
BEGIN
    -- Don't allow a resource import which is already transitively imported
    -- in the opposite direction. To do so would allow a cycle in the import graph.
    SELECT CASE
               WHEN ((SELECT source_id
                      FROM resource_transitive_imports
                      WHERE source_id = NEW.import_id
                        AND import_id = NEW.source_id) NOT NULL)
                   THEN RAISE(ABORT, 'Attempt to insert a cyclic resource import') END;
END;

-- JSON-Schema documents used by collections of the catalog. Note that each document
-- may root *many* sub-schemas, and each sub-schema may be individually referenced
-- by a JSON-Pointer URI fragment or even by a completely different base URI (if the
-- sub-schema uses the "$id" keyword).
CREATE TABLE schemas
(
    -- JSON-Schema document, as content-type "application/schema+json".
    document_json BLOB CHECK (JSON_TYPE(document_json) IN ('object', 'true', 'false')),
    -- Resource which produced this schema.
    resource_id   INTEGER PRIMARY KEY NOT NULL REFERENCES resources (id)
);

-- Lambdas are function definitions.
CREATE TABLE lambdas
(
    id          INTEGER PRIMARY KEY NOT NULL,
    -- Runtime of this lambda.
    runtime     TEXT                NOT NULL,
    -- Function body (used by: jq, sqlite).
    body        BLOB,
    -- Resource which produced this lambda.
    resource_id INTEGER REFERENCES resources (id),

    CHECK (runtime IN ('typescript', 'sqlite', 'remote'))
);

-- Collections of the catalog.
CREATE TABLE collections
(
    id          INTEGER PRIMARY KEY NOT NULL,
    -- Unique name of this collection.
    name        TEXT UNIQUE         NOT NULL CHECK (name REGEXP '[\pL\pN\-_+/.]{1,}'),
    -- Canonical URI of the collection's JSON-Schema.
    schema_uri  TEXT                NOT NULL,
    -- Composite key extractors of the collection, as `[JSON-Pointer]`.
    key_json    TEXT                NOT NULL CHECK (JSON_TYPE(key_json) == 'array'),
    -- Resource which produced this collection.
    resource_id INTEGER             NOT NULL REFERENCES resources (id)
);

-- Projections are locations within collection documents which may be projected
-- into a flattened (i.e. columnar) attribute/value space.
CREATE TABLE projections
(
    -- Collection to which this projection pertains.
    collection_id        INTEGER NOT NULL REFERENCES collections (id),
    -- Name of this projection.
    field                TEXT    NOT NULL CHECK (field REGEXP '[\pL\pN_]{1,}'),
    -- Collection document location, as a JSON-Pointer.
    location_ptr         TEXT    NOT NULL,
    -- Use this projection to logically partition the collection?
    is_logical_partition BOOLEAN,

    PRIMARY KEY (collection_id, field)
);

-- Derivations details collections of the catalog which are derived from other collections.
CREATE TABLE derivations
(
    -- Collection to which this derivation applies.
    collection_id INTEGER PRIMARY KEY NOT NULL REFERENCES collections (id),
    -- Number of parallel derivation processors.
    parallelism   INTEGER CHECK (parallelism > 0)
);

-- Bootstraps relate a derivation and lambdas which are invoked to initialize it.
CREATE TABLE bootstraps
(
    bootstrap_id  INTEGER PRIMARY KEY NOT NULL,
    -- Derivation to which this bootstrap lambda applies.
    derivation_id INTEGER             NOT NULL REFERENCES derivations (collection_id),
    -- Lambda expression to invoke.
    lambda_id     INTEGER             NOT NULL REFERENCES lambdas (id)
);

-- Transforms relate a source collection, an applied lambda, and a derived
-- collection into which transformed documents are produced.
CREATE TABLE transforms
(
    transform_id         INTEGER PRIMARY KEY NOT NULL,
    -- Derivation to which this transform applies.
    derivation_id        INTEGER             NOT NULL REFERENCES derivations (collection_id),
    -- Collection being read from.
    source_collection_id INTEGER             NOT NULL REFERENCES collections (id),
    -- Optional JSON-Schema to verify against documents of the source collection.
    source_schema_uri    TEXT,
    -- Composite key extractor for shuffling source documents to shards, as
    -- `[JSON-Pointer]`. Often this is simply `key_json` of the source collection.
    shuffle_key_json     TEXT                NOT NULL CHECK (JSON_TYPE(shuffle_key_json) == 'array'),
    -- Number of ranked shards by which each document is read.
    shuffle_broadcast    INTEGER CHECK (shuffle_broadcast > 0),
    -- Number of ranked shards from which a shard is randomly selected.
    shuffle_choose       INTEGER CHECK (shuffle_choose > 0),
    -- Code block which consumes source documents and emits target documents.
    lambda_id            INTEGER             NOT NULL REFERENCES lambdas (id)

    -- Only one of shuffle_broadcast or shuffle_choose may be set.
    CHECK ((shuffle_broadcast > 0) <> (shuffle_choose > 0))
);

-- Fixtures of catalog collections.
CREATE TABLE fixtures
(
    -- Collection to which this fixture pertains.
    collection_id    INTEGER NOT NULL REFERENCES collections (id),
    -- Fixture document, as `application/json`.
    document_json    TEXT CHECK (JSON_VALID(document_json)),
    -- Expected composite key extracted from the collection document.
    key_json         TEXT    NOT NULL CHECK (JSON_TYPE(key_json) == 'array'),
    -- Expected projections extracted from the collection document,
    -- as {name: value}. This may be a subset.
    projections_json TEXT    NOT NULL CHECK (JSON_TYPE(projections_json) == 'object'),
    -- Resource which produced this fixture.
    resource_id      INTEGER NOT NULL REFERENCES resources (id),

    PRIMARY KEY (collection_id, key_json)
);

-- Inferences are locations of collection documents and associated attributes
-- which are statically provable solely from the collection's JSON-Schema.
CREATE TABLE inferences
(
    -- Collection to which this inference pertains.
    collection_id                     INTEGER NOT NULL REFERENCES collections (id),
    -- Inferred collection document location, as a JSON-Pointer.
    location_ptr                      TEXT    NOT NULL,
    -- Is |location_ptr| a regex pattern over applicable JSON-Pointers?
    is_pattern                        BOOLEAN,
    -- Possible types for this location.
    -- Subset of ["null", "true", "false", "object", "array", "integer", "numeric", "string"].
    types_json                        TEXT    NOT NULL CHECK (JSON_TYPE(types_json) == 'array'),
    -- When the location is a "string" type, the format which the string must take.
    string_format                     TEXT,
    -- If of type "string", media MIME type of its content.
    string_content_type               TEXT,
    -- If of type "string", is the value base64-encoded ?
    string_content_encoding_is_base64 BOOLEAN,
    -- Is this location the message UUID?
    is_message_uuid                   BOOLEAN,

    PRIMARY KEY (collection_id, location_ptr)
);

CREATE TABLE materializations
(
    id            INTEGER PRIMARY KEY NOT NULL,
    -- Collection to be materialized.
    collection_id INTEGER             NOT NULL REFERENCES collections (id),
    -- Resource which produced this materialization.
    resource_id   INTEGER             NOT NULL REFERENCES resources (id)
);

CREATE TABLE materializations_postgres
(
    id          INTEGER PRIMARY KEY NOT NULL REFERENCES materializations,
    address     TEXT                NOT NULL,
    schema_name TEXT                NOT NULL,
    table_name  TEXT                NOT NULL
);
