PRAGMA foreign_keys = ON;

-- Unique resources (eg, files) from which this catalog is built.
--
-- :content_type:
--      MIME type of the resource.
-- :content:
--      Content of this resource.
-- :is_processed:
--      Marks the resource as having been processed.
CREATE TABLE resources
(
    resource_id  INTEGER PRIMARY KEY NOT NULL,
    content_type TEXT    NOT NULL,
    content      BLOB    NOT NULL,
    is_processed BOOLEAN NOT NULL,

    CONSTRAINT "Invalid resource content-type" CHECK (content_type IN (
        'application/vnd.estuary.dev-catalog-spec+yaml',
        'application/vnd.estuary.dev-catalog-fixtures+yaml',
        'application/schema+yaml',
        'application/sql',
        'application/vnd.estuary.dev-catalog-npm-pack'
    ))
);

-- Import relationships between resources.
-- Every resource which references another explicitly records the relationship in
-- this table, to facilitate understanding of the transitive "A uses B"
-- relationships between catalog resources.
--
-- :resource_id:
--      ID of resource which imports another resource.
-- :import_id:
--      ID of the imported resource.
CREATE TABLE resource_imports
(
    resource_id INTEGER NOT NULL REFERENCES resources (resource_id),
    import_id   INTEGER NOT NULL REFERENCES resources (resource_id),

    PRIMARY KEY (resource_id, import_id)
);

--View which derives all transitive resource imports
CREATE VIEW resource_transitive_imports AS
WITH RECURSIVE cte(resource_id, import_id) AS (
    SELECT resource_id, resource_id
        FROM resources
    UNION ALL
    SELECT cte.resource_id, ri.import_id
        FROM resource_imports AS ri
        JOIN cte ON ri.resource_id = cte.import_id
)
SELECT * FROM cte;

-- Don't allow a resource import which is already transitively imported
-- in the opposite direction. To do so would allow a cycle in the import graph.
CREATE TRIGGER assert_resource_imports_are_acyclic
    BEFORE INSERT
    ON resource_imports
    FOR EACH ROW
    WHEN (
        SELECT 1 FROM resource_transitive_imports
            WHERE resource_id = NEW.import_id AND import_id = NEW.resource_id
    ) NOT NULL
BEGIN
    SELECT RAISE(ABORT, 'Import creates an cycle (imports must be acyclic)');
END;

-- Universal resource locators of resources. Each resource may have more than one
-- associated URL by which it can be addressed. A prominent use-case is that
-- JSON schemas may have '$id' properties at arbitrary locations which change the
-- canonical base URI of that schema. So long as '$id's are first indexed here,
-- alternate URLs can then be referenced and correctly resolved to the resource.
-- 
-- :resource_id:
--      Resource having an associated URL.
-- :url:
--      URL of this resource. Eg `file:///local/path` or `https://remote/path?query`.
--      Must be a base URL without a fragment component.
-- :is_primary: 
--      A resource's primary URL is the URL at which that resource was originally
--      fetched, and serves as the base URL when resolving relative sub-resources.
--      Every resource has exactly one primary URL.
--      Note SQLite doesn't enforce uniqueness where is_primary IS NULL.
CREATE TABLE resource_urls
(
    resource_id INTEGER     NOT NULL REFERENCES resources (resource_id),
    url         TEXT UNIQUE NOT NULL,
    is_primary  BOOLEAN,

    UNIQUE (resource_id, is_primary),

    CONSTRAINT "URL must be a valid, base (non-relative) URL"
        CHECK (url LIKE '_%://_%'),
    CONSTRAINT "URL cannot have a fragment component"
        CHECK (url NOT LIKE '%#%'),
    CONSTRAINT "is_primary should be 'true' or NULL"
        CHECK (is_primary IS TRUE OR is_primary IS NULL)
);

-- Lambdas are invokable expressions within an associated lambda runtime.
--
-- :runtime:
--      Lambda runtime (nodeJS, sqlite, or sqliteFile).
-- :inline: 
--      Inline function expression, with semantics that depend on the runtime:
--      * If 'nodeJS', this is a Typescript / JavaScript expression (i.e. an arrow
--        expression, or a named function to invoke).
--      * If 'remote', this is a remote HTTP endpoint URL to invoke.
--      * If 'sqlite', this is an inline SQL script.
--      * If 'sqliteFile', this is NULL (and resource_id is set instead).
-- :resource_id:
--      Resource holding the lambda's content. Set only iff runtime is 'sqliteFile'.
CREATE TABLE lambdas
(
    lambda_id   INTEGER PRIMARY KEY NOT NULL,
    runtime     TEXT                NOT NULL,
    inline      TEXT,
    resource_id INTEGER REFERENCES resources (resource_id),

    CONSTRAINT "Unknown Lambda runtime" CHECK (
        runtime IN ('nodeJS', 'sqlite', 'sqliteFile', 'remote')),
    CONSTRAINT "NodeJS lambda must provide an inline expression" CHECK (
        runtime != 'nodeJS' OR (inline NOT NULL AND resource_id IS NULL)),
    CONSTRAINT "SQLite lambda must provide an inline expression" CHECK (
        runtime != 'sqlite' OR (inline NOT NULL AND resource_id IS NULL)),
    CONSTRAINT "SQLiteFile lambda must provide a file resource" CHECK (
        runtime != 'sqliteFile' OR (inline IS NULL AND resource_id IS NOT NULL)),
    CONSTRAINT "Remote lambda must provide an HTTP endpoint URL" CHECK (
        runtime != 'remote' OR (inline LIKE '_%://_%' AND resource_id IS NULL))
);

-- Collections of the catalog.
--
-- :name:
--      Unique name of this collection.
-- :schema_uri: 
--      Canonical URI of the collection's JSON-Schema. This may include a fragment
--      component which references a sub-schema of the document.
-- :key_json:
--     Composite key extractors of the collection, as `[JSON-Pointer]`.
-- :resource_id:
--      Catalog source spec which defines this collection.
CREATE TABLE collections
(
    collection_id INTEGER PRIMARY KEY NOT NULL,
    name          TEXT UNIQUE         NOT NULL,
    schema_uri    TEXT                NOT NULL,
    key_json      TEXT                NOT NULL,
    resource_id   INTEGER             NOT NULL REFERENCES resources (resource_id),

    CONSTRAINT "Collection name format isn't valid" CHECK (
        name REGEXP '^[\pL\pN\-_+/.]+$'),
    CONSTRAINT "Schema must be a valid base (non-relative) URI" CHECK (
        schema_uri LIKE '_%://_%'),
    CONSTRAINT "Key must be non-empty JSON array of JSON-Pointers" CHECK (
        JSON_ARRAY_LENGTH(key_json) > 0)
);

-- Projections are locations within collection documents which may be projected
-- into a flattened (i.e. columnar) attribute/value space.
--
-- :collection_id:
--      Collection to which this projection pertains.
-- :field:
--      Name of this projection.
-- :location_ptr:
--      Location of field within collection documents, as a JSON-Pointer.
-- :is_logical_partition:
--      Use this projection to logically partition the collection?
CREATE TABLE projections
(
    collection_id        INTEGER NOT NULL REFERENCES collections (collection_id),
    field                TEXT    NOT NULL,
    location_ptr         TEXT    NOT NULL,
    is_logical_partition BOOLEAN NOT NULL,

    PRIMARY KEY (collection_id, field),

    CONSTRAINT "Field name format isn't valid" CHECK (
        field REGEXP '^[\pL\pN_]+$'),
    CONSTRAINT "Location must be a valid JSON-Pointer" CHECK (
        location_ptr REGEXP '^(/[^/]+)*$')
);

-- Fixtures of catalog collections.
--
-- :collection_id:
--      Collection to which this fixture pertains.
-- :resource_id:
--      Fixture resource.
CREATE TABLE fixtures
(
    collection_id INTEGER NOT NULL REFERENCES collections (collection_id),
    resource_id   INTEGER NOT NULL REFERENCES resources (resource_id),

    PRIMARY KEY (collection_id, resource_id)
);

-- Derivations details collections of the catalog which are derived from other collections.
--
-- :collection_id:
--      Collection to which this derivation applies.
-- :parallelism:
--      Number of parallel derivation processors.
CREATE TABLE derivations
(
    collection_id INTEGER PRIMARY KEY NOT NULL REFERENCES collections (collection_id),
    parallelism   INTEGER CHECK (parallelism > 0)
);

-- Bootstraps relate a derivation and lambdas which are invoked to initialize it.
--
-- :derivation_id:
--      Derivation to which this bootstrap lambda applies.
-- :lambda_id:
--      Lambda expression to invoke on processor bootstrap.
CREATE TABLE bootstraps
(
    bootstrap_id  INTEGER PRIMARY KEY NOT NULL,
    derivation_id INTEGER             NOT NULL REFERENCES derivations (collection_id),
    lambda_id     INTEGER             NOT NULL REFERENCES lambdas (lambda_id)
);

-- Transforms relate a source collection, an applied lambda, and a derived
-- collection into which transformed documents are produced.
--
-- :derivation_id:
--      Derivation to which this transform applies.
-- :source_collection_id:
--      Collection being read from.
-- :lambda_id:
--      Lambda expression which consumes source documents and emits target documents.
-- :source_schema_uri:
--      Optional JSON-Schema to verify against documents of the source collection.
-- :source_partitions_json:
--      Optional partition fields to read of the source collection.
-- :shuffle_key_json:
--      Composite key extractor for shuffling source documents to shards, as
--      `[JSON-Pointer]`. If null, the `key_json` of the source collection is used.
-- :shuffle_broadcast:
--      Number of ranked shards by which each document is read. If both
--      `shuffle_broadcast` and `shuffle_choose` are NULL, then `shuffle_broadcast`
--      is implicitly treated as `1`.
-- :shuffle_choose:
--      Number of ranked shards from which a shard is randomly selected.
CREATE TABLE transforms
(
    transform_id           INTEGER PRIMARY KEY NOT NULL,
    derivation_id          INTEGER             NOT NULL REFERENCES derivations (collection_id),
    source_collection_id   INTEGER             NOT NULL REFERENCES collections (collection_id),
    lambda_id              INTEGER             NOT NULL REFERENCES lambdas (lambda_id),
    source_schema_uri      TEXT,
    source_partitions_json TEXT,
    shuffle_key_json       TEXT,
    shuffle_broadcast      INTEGER CHECK (shuffle_broadcast > 0),
    shuffle_choose         INTEGER CHECK (shuffle_choose > 0),

    CONSTRAINT "Source schema must be NULL or a valid base (non-relative) URI" CHECK (
        source_schema_uri LIKE '_%://_%'),
    CONSTRAINT "Cannot set both shuffle 'broadcast' and 'choose'" CHECK (
        (shuffle_broadcast IS NULL) OR (shuffle_choose IS NULL)),
    CONSTRAINT "Shuffle key must be NULL or non-empty JSON array of JSON-Pointers" CHECK (
        JSON_ARRAY_LENGTH(shuffle_key_json) > 0),
    CONSTRAINT "Source partitions must be a valid JSON Object" CHECK (
        JSON_TYPE(source_partitions_json) == 'object')
);

-- All transforms of a derivation reading from the same source, must also use the same source schema.
CREATE TRIGGER transforms_use_consistent_source_schema
    BEFORE INSERT
    ON transforms
    FOR EACH ROW
    WHEN (
        SELECT 1 FROM transforms
            WHERE derivation_id = NEW.derivation_id
            AND source_collection_id = NEW.source_collection_id
            AND COALESCE(source_schema_uri, '') != COALESCE(NEW.source_schema_uri, '')
    ) NOT NULL
BEGIN
    SELECT RAISE(ABORT, 'Transforms of a derived collection which read from the same source collection must use the same source schema URI');
END;

-- All named partitions of a transform source must exist as logically partitioned fields of the source collection.
CREATE TRIGGER transform_source_partitions_exist
    BEFORE INSERT
    ON transforms
    FOR EACH ROW
    WHEN (
        WITH expect AS (
            SELECT key AS field
                FROM JSON_EACH(NEW.source_partitions_json, '$.include')
            UNION
            SELECT key AS field
                FROM JSON_EACH(NEW.source_partitions_json, '$.exclude')
        ), actual AS (
            SELECT field FROM projections
                WHERE collection_id = NEW.source_collection_id AND is_logical_partition
        )
        SELECT 1 FROM expect WHERE field NOT IN (SELECT * FROM actual)
    ) NOT NULL
BEGIN
    SELECT RAISE(ABORT, 'Transform source has a partition which is not logical partition field of the source collection');
END;

-- View over all schemas which apply to a collection.
CREATE VIEW collection_schemas AS
SELECT collection_id,
       name,
       schema_uri,
       FALSE AS is_alternate
FROM collections
UNION
SELECT source_collection_id,
       source_name,
       source_schema_uri,
       TRUE AS is_alternate
FROM transform_details
    WHERE is_alt_source_schema;

-- Require that the specification resource which defines a collection transform,
-- also imports the specification which contains the referenced source collection.
CREATE TRIGGER transforms_import_source_collection
    BEFORE INSERT
    ON transforms
    FOR EACH ROW
    WHEN (
        SELECT 1 FROM
            collections AS src,
            collections AS tgt,
            resource_transitive_imports AS rti
            WHERE tgt.collection_id = NEW.derivation_id
            AND src.collection_id = NEW.source_collection_id
            AND tgt.resource_id = rti.resource_id
            AND src.resource_id = rti.import_id
    ) IS NULL
BEGIN
    SELECT RAISE(ABORT, 'Transform references a source collection which is not imported by this catalog spec');
END;

-- Detail view of transforms joined with collection and lambda details,
-- and flattening NULL-able fields into their assumed defaults.
CREATE VIEW transform_details AS
SELECT transforms.transform_id,
       -- Source collection details.
       transforms.source_collection_id,
       src.name                                                                AS source_name,
       src.resource_id                                                         AS source_resource_id,
       COALESCE(transforms.source_schema_uri, src.schema_uri)                  AS source_schema_uri,
       transforms.source_partitions_json                                       AS source_partitions_json,
       transforms.source_schema_uri IS NOT NULL                                AS is_alt_source_schema,
       COALESCE(transforms.shuffle_key_json, src.key_json)                     AS shuffle_key_json,

       -- Derived collection details.
       transforms.derivation_id,
       der.name                                                                AS derivation_name,
       der.resource_id                                                         AS derivation_resource_id,
       der.schema_uri                                                          AS derivation_schema_uri,
       der.key_json                                                            AS derivation_key_json,

       -- Shuffle details. Convert broadcast/choose NULL's to 0.
       -- Default to broadcast: 1, choose: 0 if both are NULL.
       COALESCE(transforms.shuffle_broadcast,
                CASE WHEN transforms.shuffle_choose IS NULL THEN 1 ELSE 0 END) AS shuffle_broadcast,
       COALESCE(transforms.shuffle_choose, 0)                                  AS shuffle_choose,

       -- Lambda fields.
       transforms.lambda_id                                                    AS lambda_id,
       lambdas.runtime                                                         AS lambda_runtime,
       lambdas.inline                                                          AS lambda_inline,
       lambdas.resource_id                                                     AS lambda_resource_id,
       lambda_resources.content                                                AS lambda_resource_content

FROM transforms
         JOIN collections AS src
              ON transforms.source_collection_id = src.collection_id
         JOIN collections AS der
              ON transforms.derivation_id = der.collection_id
         JOIN lambdas
              ON transforms.lambda_id = lambdas.lambda_id
         LEFT JOIN resources AS lambda_resources
                   ON lambdas.resource_id = lambda_resources.resource_id
;

-- View over schema URIs and their extracted fields, with context.
CREATE VIEW schema_extracted_fields AS
SELECT
    c.schema_uri,
    k.value AS ptr,
    TRUE AS is_key,
    PRINTF('key of collection %Q', c.name) AS context
FROM collections AS c, JSON_EACH(c.key_json) AS k
UNION
SELECT
    t.source_schema_uri,
    k.value,
    TRUE AS is_key,
    PRINTF('shuffle key of source %Q by derivation %Q',
        t.source_name, t.derivation_name)
FROM transform_details AS t, JSON_EACH(t.shuffle_key_json) AS k
UNION
SELECT
    c.schema_uri,
    p.location_ptr,
    p.is_logical_partition AS is_key,
    PRINTF('projected field %Q of collection %Q', p.field, c.name)
FROM collections AS c NATURAL JOIN projections AS p
;

-- Map of NodeJS dependencies to bundle with the catalog's built NodeJS package.
-- :package: 
--      Name of the NPM package depended on.
-- :version:
--      Version string, as understood by NPM.
--      See https://docs.npmjs.com/files/package.json#dependencies
CREATE TABLE nodejs_dependencies
(
    package TEXT PRIMARY KEY NOT NULL,
    version TEXT             NOT NULL
);

-- Abort an INSERT of a package that's already registered with a different version.
CREATE TRIGGER nodejs_dependencies_disagree
    BEFORE INSERT
    ON nodejs_dependencies
    FOR EACH ROW
BEGIN
    SELECT CASE
        WHEN (
            SELECT 1 FROM nodejs_dependencies
                WHERE package = NEW.package AND version != NEW.version
        ) THEN
            RAISE(ABORT, 'A dependency on this nodeJS package at a different package version already exists')
        WHEN (
            SELECT 1 FROM nodejs_dependencies
                WHERE package = NEW.package AND version = NEW.version
        ) THEN
            RAISE(IGNORE)
    END;
END;

/*
-- Inferences are locations of collection documents and associated attributes
-- which are statically provable solely from the collection's JSON-Schema.
CREATE TABLE inferences
(
    -- Collection to which this inference pertains.
    collection_id                     INTEGER NOT NULL REFERENCES collections (collection_id),
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
    material_id   INTEGER PRIMARY KEY NOT NULL,
    -- Collection to be materialized.
    collection_id INTEGER             NOT NULL REFERENCES collections (collection_id),
    -- Catalog source spec which defines this collection.
    resource_id   INTEGER             NOT NULL REFERENCES resources (resource_id)
);

CREATE TABLE materializations_postgres
(
    material_id INTEGER PRIMARY KEY NOT NULL REFERENCES materializations (material_id),
    address     TEXT                NOT NULL,
    schema_name TEXT                NOT NULL,
    table_name  TEXT                NOT NULL
);
*/
