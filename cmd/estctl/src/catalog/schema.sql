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
    id       INTEGER NOT NULL REFERENCES resources,
    -- ID of the imported resource.
    import_id INTEGER NOT NULL REFERENCES resources,

    PRIMARY KEY (id, import_id)
);

-- View which derives all transitive resource imports
CREATE VIEW resource_transitive_imports AS
WITH RECURSIVE cte(id, import_id) AS (
    SELECT id, import_id
    FROM resource_imports
    UNION ALL
    SELECT cte.id, ri.import_id
    FROM resource_imports AS ri
             JOIN cte ON ri.id = cte.import_id
)
SELECT * FROM cte;

CREATE TRIGGER assert_resource_imports_are_acyclic
    BEFORE INSERT
    ON resource_imports
BEGIN
    -- Don't allow a resource import which is already transitively imported
    -- in the opposite direction. To do so would allow a cycle in the import graph.
    SELECT CASE
               WHEN ((SELECT id
                      FROM resource_transitive_imports
                      WHERE id = NEW.import_id
                        AND import_id = NEW.id) NOT NULL)
                   THEN RAISE(ABORT, 'Attempt to insert a cyclic resource import') END;
END;

-- JSON-Schema documents used by collections of the catalog. Note that each document
-- may root *many* sub-schemas, and each sub-schema may be individually referenced
-- by a JSON-Pointer URI fragment or even by a completely different base URI (if the
-- sub-schema uses the "$id" keyword).
CREATE TABLE schema_documents
(
    -- JSON-Schema document, as content-type "application/schema+json".
    document    TEXT CHECK (JSON_TYPE(document) IN ('object', 'true', 'false')),
    -- Resource which produced this schema.
    resource_id INTEGER PRIMARY KEY NOT NULL REFERENCES resources (id)
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
    key_json    TEXT                NOT NULL CHECK (JSON_TYPE(key) == 'array'),
    -- Resource which produced this collection.
    resource_id INTEGER             NOT NULL REFERENCES resources (id)
);

-- Projections are locations of collection documents which may be projected
-- into a flattened columnar attribute/value space.
CREATE TABLE projections
(
    -- Collection to which this projection pertains.
    collection_id INTEGER NOT NULL REFERENCES collections (id),
    -- Name of this projection's column.
    name          TEXT    NOT NULL CHECK (name REGEXP '[\pL\pN_]{1,}'),
    -- Collection document location, as a JSON-Pointer.
    ptr           TEXT    NOT NULL,
    -- Use this projection to logically partition the collection?
    is_logical_partition  BOOLEAN,

    PRIMARY KEY (collection_id, name)
);

-- Inferences are locations of collection documents and associated attributes
-- which are statically provable solely from the collection's JSON-Schema.
CREATE TABLE inferences
(
    -- Collection to which this inference pertains.
    collection_id    INTEGER NOT NULL REFERENCES collections (id),
    -- Inferred collection document location, as a JSON-Pointer.
    ptr              TEXT NOT NULL,
    -- Is this a regex pattern over JSON-Pointers?
    is_pattern       BOOLEAN,
    -- Possible types for this location.
    -- Subset of ["null", "true", "false", "object", "array", "integer", "numeric", "string"].
    types_json       TEXT    NOT NULL CHECK (JSON_TYPE(types) == 'array'),
    -- When the location is a "string" type, the format which the string must take.
    string_format    TEXT,
    -- Is this location a scalar (i.e., is never an "object" or "array")?
    is_scalar        BOOLEAN,
    -- If of type "string", is the value base64-encoded ?
    is_base64        BOOLEAN,
    -- Is this location the message UUID?
    is_message_uuid BOOLEAN,
    -- Media MIME type of this location's content.
    content_type     TEXT,

    PRIMARY KEY (collection_id, ptr)
);

-- Fixtures of catalog collections.
CREATE TABLE fixtures
(
    -- Collection to which this fixture pertains.
    collection_id INTEGER NOT NULL REFERENCES collections (id),
    -- JSON-encoded fixture document.
    document      TEXT CHECK (JSON_VALID(document)),
    -- Expected composite key extracted from the collection document.
    key           TEXT    NOT NULL CHECK (JSON_TYPE(key) == 'array'),
    -- Expected projections extracted from the collection document,
    -- as {name: value}. This may be a subset.
    projections    TEXT    NOT NULL CHECK (JSON_TYPE(projections) == 'object'),
    -- Resource which produced this fixture.
    resource_id   INTEGER NOT NULL REFERENCES resources (id),

    PRIMARY KEY (collection_id, key)
);

CREATE TABLE lambdas
(
    id                    INTEGER PRIMARY KEY NOT NULL,
    -- Type of this lambda.
    type                  TEXT                NOT NULL,

    -- Function body (used by: jq, sqlite).
    body                  TEXT,
    -- Resource which produced this body.
    body_resource_id      INTEGER REFERENCES resources (id),
    -- Bootstrap / prelude (used by: sqlite).
    bootstrap             TEXT,
    -- Resource which produced this bootstrap.
    bootstrap_resource_id INTEGER REFERENCES resources (id),

    CHECK (type IN ('jq', 'sqlite'))
);

-- Derivations details collections of the catalog which are derived from other collections.
CREATE TABLE derivations
(
    collection_id INTEGER PRIMARY KEY NOT NULL REFERENCES collections (id),
    -- If non-null, the collection is derived via a durable closure
    -- having a fixed number of shards.
    fixed_shards  INTEGER CHECK (fixed_shards > 0),
    -- Resource which produced this derivation.
    resource_id   INTEGER             NOT NULL REFERENCES resources (id)
);

CREATE TABLE transforms
(
    transform_id INTEGER PRIMARY KEY NOT NULL,
    -- Collection being read from.
    source_id         TEXT    NOT NULL REFERENCES collections (id),
    -- Alternative JSON-Schema to apply to the source collection.
    -- Optional: if NULL, the source collection's schema is used.
    source_schema_uri TEXT,
    -- Alternative key extractor for shuffling source documents to shards.
    -- Optional: if null, the key extractor of the source collection is used.
    shuffle_key_json  TEXT CHECK (JSON_TYPE(shuffle_key) == 'array'),
    -- Number of ranked shards by which each document is read.
    shuffle_broadcast INTEGER CHECK (shuffle_broadcast > 0),
    -- Number of ranked shards from which a shard is randomly selected.
    shuffle_choose    INTEGER CHECK (shuffle_choose > 0),
    -- Collection being derived into.
    target_id         TEXT    NOT NULL REFERENCES derivations (collection_id),
    -- Code block which consumes source documents and emits target documents.
    lambda_id         INTEGER NOT NULL REFERENCES lambdas (id),
    -- Resource which produced this transform.
    resource_id       INTEGER NOT NULL REFERENCES resources (id),

    UNIQUE KEY (target_id, source_id)
);

CREATE TABLE keys
(
    -- Collection to which this shuffle key pertains.
    collection_id    INTEGER NOT NULL REFERENCES transforms (source_id),
    -- Location of key component within collection documents, as a JSON-Pointer.
    ptr              TEXT NOT NULL,
    -- Index of this field within the composite key.
    index            INTEGER NOT NULL,
    -- If transform_id is NULL, then this is the primary key of the collection.
    -- Otherwise, this is a shuffle key of the collection under the given transform.
    transform_id     INTEGER REFERENCES transforms (transform_id),

    PRIMARY KEY (transform_id, collection_id, index),
    FOREIGN KEY (collection_id, ptr) REFERENCES inferences (collection_id, ptr)
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