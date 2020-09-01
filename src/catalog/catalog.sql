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

-- Resource schemas is a view over all JSON-Schemas which are transitively
-- imported or referenced from a given resource_id. In other words, this is
-- the set of JSON-Schemas which must be compiled and indexed when validating
-- on behalf of the given resource. 
CREATE VIEW resource_schemas AS
SELECT
	rti.resource_id AS resource_id,
	resource_urls.url AS schema_uri,
   	resources.content AS schema_content
FROM
	resource_transitive_imports AS rti
    JOIN resources ON rti.import_id = resources.resource_id
    JOIN resource_urls ON rti.import_id = resource_urls.resource_id
    WHERE
    	resources.content_type = 'application/schema+yaml' AND
        resource_urls.is_primary
	GROUP BY rti.resource_id, rti.import_id
;

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
-- :collection_name:
--      Unique name of this collection.
-- :schema_uri: 
--      Canonical URI of the collection's JSON-Schema. This may include a fragment
--      component which references a sub-schema of the document.
-- :key_json:
--     Composite key extractors of the collection, as `[JSON-Pointer]`.
-- :resource_id:
--      Catalog source spec which defines this collection.
-- :default_projections_max_depth:
--      Generate default projections up to this given depth of nesting. A value of 0 will disable
--      generation of default projections.
CREATE TABLE collections
(
    collection_id   INTEGER PRIMARY KEY NOT NULL,
    collection_name TEXT UNIQUE         NOT NULL,
    schema_uri      TEXT                NOT NULL,
    key_json        TEXT                NOT NULL,
    resource_id     INTEGER             NOT NULL REFERENCES resources (resource_id),
    default_projections_max_depth INTEGER NOT NULL,

    CONSTRAINT "Collection name format isn't valid" CHECK (
        collection_name REGEXP '^[\pL\pN\-_+/.]+$'),
    CONSTRAINT "Schema must be a valid base (non-relative) URI" CHECK (
        schema_uri LIKE '_%://_%'),
    CONSTRAINT "Key must be non-empty JSON array of JSON-Pointers" CHECK (
        JSON_ARRAY_LENGTH(key_json) > 0),
    CONSTRAINT "Default Projections maximum depth must be between 0 and 255 inclusive" CHECK (
        default_projections_max_depth >= 0 AND default_projections_max_depth <= 255)
);


-- Materialization targets for a collection
-- 
-- :materialization_name:
--     Human-readable name of the materialization that was provided in the catalog spec
-- :collection_id:
--     Collection that is being materialized
-- :target_type:
--     The type of database to materialize into. This must be either 'postgres' or 'sqlite'
-- :target_uri:
--     The (database) connection uri provided in the catalog spec
-- :table_name:
--     The name of the table in the target system to materialize to. This may not always be relevant
--     for all types of systems, but for now it's required since we only support sql databases.
-- :config_json:
--     A JSON configuration object that is specific to the given :target_type:. This object is
--     currently generated by estctl and hard-coded in there. This may be a candidate for future
--     inclusion in a pre-populated table, which gets referenced by this table.
CREATE TABLE materializations
(
    materialization_id INTEGER PRIMARY KEY NOT NULL,
    materialization_name TEXT NOT NULL,
    collection_id INTEGER NOT NULL REFERENCES collections (collection_id),
    target_type TEXT NOT NULL CONSTRAINT 'target_type must be a recognized type' CHECK(target_type IN ('postgres', 'sqlite')),
    target_uri TEXT NOT NULL,
    table_name TEXT NOT NULL,
    config_json TEXT CONSTRAINT 'config_json must be valid json' CHECK(JSON_VALID(config_json)),

    UNIQUE(collection_id, materialization_name COLLATE NOCASE)
);

-- Holds the DDL for each materialization. This may be a candidate for removal in favor of a
-- nullable column on the materializations table, since there can be at most one materialization_ddl
-- row for each row in materializations.
CREATE TABLE materialization_ddl
(
    materialization_id INTEGER NOT NULL PRIMARY KEY,
    ddl TEXT,
    FOREIGN KEY (materialization_id) REFERENCES materializations (materialization_id)
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
CREATE TABLE projections
(
    collection_id        INTEGER NOT NULL REFERENCES collections (collection_id),
    field                TEXT    NOT NULL,
    location_ptr         TEXT    NOT NULL,
    user_provided        BOOLEAN NOT NULL CHECK (user_provided IN(0,1)),

    PRIMARY KEY (collection_id, field),
    UNIQUE (collection_id, field COLLATE NOCASE),

    CONSTRAINT "Field name format isn't valid" CHECK (
        field REGEXP '^[\pL\pN_]+$'),
    CONSTRAINT "Location must be a valid JSON-Pointer" CHECK (
        location_ptr REGEXP '^(/[^/]+)*$')
);

-- Partitions are projections which logically partition the collection.
--
-- :collection_id:
--      Collection to which this projection pertains.
-- :field:
--      Field of this partition.
CREATE TABLE partitions
(
    collection_id INTEGER NOT NULL,
    field         TEXT    NOT NULL,

    PRIMARY KEY (collection_id, field),
    FOREIGN KEY (collection_id, field)
        REFERENCES projections(collection_id, field)
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
-- :register_schema_uri:
--      JSON-Schema to verify and supply reduction annotations over the
--      derivation's register documents.
-- :register_initial_json:
--      JSON value which is used as the initial value of a register,
--      before any user updates have been reduced in.
CREATE TABLE derivations
(
    collection_id          INTEGER PRIMARY KEY NOT NULL REFERENCES collections (collection_id),
    register_schema_uri    TEXT NOT NULL,
    register_initial_json  TEXT NOT NULL,

    CONSTRAINT "Register schema must be a valid base (non-relative) URI" CHECK (
        register_schema_uri LIKE '_%://_%')
    CONSTRAINT "Initial Register must be valid JSON" CHECK (JSON_VALID(register_initial_json))
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

-- Transforms relate a source collection, applied lambda(s), and a derived
-- collection into which transformed documents are produced.
--
-- :derivation_id:
--      Derivation produced (in part) by this transform, and to which this transform belongs.
-- :transform_name:
--      Name of this transform, which must be unique amoung transforms of its derivation.
-- :update_id:
--      Lambda expression which takes a source document and associated register,
--      and emits documents to be combined back into the register.
-- :publish_id:
--      Lambda expression which takes a source document and associated register,
--      and emits documents to be published into the derived collection.
-- :source_collection_id:
--      Collection being read from.
-- :source_schema_uri:
--      Optional JSON-Schema to verify against documents of the source collection.
-- :shuffle_key_json:
--      Composite key extractor for shuffling source documents to shards, as
--      `[JSON-Pointer]`. If null, the `key_json` of the source collection is used.
-- :read_delay_seconds:
--      Number of seconds by which documents read by this transform should be delayed,
--      both with respect to other documents and transforms, and also with respect to
--      the current wall-clock time.
CREATE TABLE transforms
(
    transform_id           INTEGER PRIMARY KEY NOT NULL,
    derivation_id          INTEGER             NOT NULL REFERENCES derivations (collection_id),
    transform_name         TEXT                NOT NULL,
    source_collection_id   INTEGER             NOT NULL REFERENCES collections (collection_id),
    update_id              INTEGER                      REFERENCES lambdas (lambda_id),
    publish_id             INTEGER                      REFERENCES lambdas (lambda_id),
    source_schema_uri      TEXT,
    shuffle_key_json       TEXT,
    read_delay_seconds     INTEGER CHECK (read_delay_seconds > 0),

    -- Name must be unique amoung transforms of the derivation.
    UNIQUE(transform_name, derivation_id),
    -- Required index of the transform_source_partitions foreign-key.
    UNIQUE(transform_id, source_collection_id),

    CONSTRAINT "Source schema must be NULL or a valid base (non-relative) URI" CHECK (
        source_schema_uri LIKE '_%://_%'),
    CONSTRAINT "Shuffle key must be NULL or non-empty JSON array of JSON-Pointers" CHECK (
        JSON_ARRAY_LENGTH(shuffle_key_json) > 0),
    CONSTRAINT "Must set at least one of 'update' or 'publish' lambdas" CHECK (
        (update_id NOT NULL) OR (publish_id NOT NULL))
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

-- If the source_schema_uri is the same as the schema_uri of the source collection, then we'll raise
-- an error. This condition would not necessarily affect correctness, but it would essentially have
-- no effect, and so we'll assume that this isn't what the user intended and raise an error.
CREATE TRIGGER transforms_source_schema_different_from_collection_schema
    BEFORE INSERT
    ON transforms
    FOR EACH ROW
    WHEN (
        SELECT schema_uri FROM collections
            WHERE collection_id = NEW.source_collection_id
    ) = NEW.source_schema_uri
    BEGIN
        SELECT RAISE(ABORT, 'Transforms that specify a source schema may not use the same schema as the source collection');
    END;

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

-- Partitions of the transform source which the transform is restricted to.
--
-- :transform_id:
--      Transform to which the partition restriction applies.
-- :collection_id:
--      Source collection which is partitioned.
-- :field:
--      Partitioned field of the source collection.
-- :value_json:
--      JSON-encoded value to be matched.
-- :is_exclude:
--      If true, this record is a partition exclusion (as opposed to an inclusion).
CREATE TABLE transform_source_partitions
(
    transform_id  INTEGER NOT NULL,
    collection_id INTEGER NOT NULL,
    field         TEXT    NOT NULL,
    value_json    TEXT    NOT NULL,
    is_exclude    BOOLEAN NOT NULL,

    FOREIGN KEY(transform_id, collection_id)
        REFERENCES transforms(transform_id, source_collection_id),
    FOREIGN KEY(collection_id, field)
        REFERENCES partitions(collection_id, field),

    CONSTRAINT "Value must be valid JSON" CHECK (JSON_VALID(value_json))
);

-- View over transform_source_partitions which groups partitions on
-- transform_id, and aggregates partition selectors into a flat JSON array.
CREATE VIEW transform_source_partitions_json AS
SELECT
    transform_id,
    collection_id,
    JSON_GROUP_ARRAY(JSON_OBJECT(
        'field', field,
        'value', value_json,
        'exclude', is_exclude
    )) AS json
FROM transform_source_partitions GROUP BY transform_id, collection_id;

-- Detail view of transforms joined with collection and lambda details,
-- and flattening NULL-able fields into their assumed defaults.
CREATE VIEW transform_details AS
SELECT transforms.transform_id,
       transforms.transform_name,

       -- Derivation details.
       derivations.register_schema_uri,

       -- Source collection details.
       transforms.source_collection_id,
       src.collection_name                                                     AS source_name,
       src.resource_id                                                         AS source_resource_id,
       COALESCE(transforms.source_schema_uri, src.schema_uri)                  AS source_schema_uri,
       source_partitions.json                                                  AS source_partitions_json,
       transforms.source_schema_uri IS NOT NULL                                AS is_alt_source_schema,
       COALESCE(transforms.shuffle_key_json, src.key_json)                     AS shuffle_key_json,
       transforms.read_delay_seconds,

       -- Derived collection details.
       transforms.derivation_id,
       der.collection_name                                                     AS derivation_name,
       der.resource_id                                                         AS derivation_resource_id,
       der.schema_uri                                                          AS derivation_schema_uri,
       der.key_json                                                            AS derivation_key_json,

       -- Update lambda fields.
       transforms.update_id                                                    AS update_id,
       updates.runtime                                                         AS update_runtime,
       updates.inline                                                          AS update_inline,
       updates.resource_id                                                     AS update_resource_id,
       update_resources.content                                                AS update_resource_content,

       -- Publish lambda fields.
       transforms.publish_id                                                   AS publish_id,
       publish.runtime                                                         AS publish_runtime,
       publish.inline                                                          AS publish_inline,
       publish.resource_id                                                     AS publish_resource_id,
       publish_resources.content                                               AS publish_resource_content

FROM transforms
         JOIN collections AS src
              ON transforms.source_collection_id = src.collection_id
         JOIN collections AS der
              ON transforms.derivation_id = der.collection_id
         JOIN derivations
              ON transforms.derivation_id = derivations.collection_id
         LEFT JOIN lambdas AS updates
              ON transforms.update_id = updates.lambda_id
         LEFT JOIN resources AS update_resources
              ON updates.resource_id = update_resources.resource_id
         LEFT JOIN lambdas AS publish
              ON transforms.publish_id = publish.lambda_id
         LEFT JOIN resources AS publish_resources
              ON publish.resource_id = publish_resources.resource_id
         LEFT JOIN transform_source_partitions_json as source_partitions
              ON transforms.transform_id = source_partitions.transform_id
              AND transforms.source_collection_id = source_partitions.collection_id
;

-- Detail view of collections joined with projections, partitions,
-- derivations, and alternate source schemas.
CREATE VIEW collection_details AS
WITH
collection_partitions AS (
SELECT
	collection_id,
	JSON_GROUP_ARRAY(
		JSON_OBJECT(
			'field', field,
			'ptr', location_ptr
	)) AS partitions_json
	FROM projections
	NATURAL JOIN partitions
	GROUP BY collection_id
),
collection_projections AS (
SELECT
	collection_id,
	JSON_GROUP_ARRAY(
		JSON_OBJECT(
			'field', field,
			'ptr', location_ptr
	)) AS projections_json
	FROM projections
	GROUP BY collection_id
),
collection_alt_schemas AS (
SELECT
	source_collection_id AS collection_id,
	JSON_GROUP_ARRAY(DISTINCT source_schema_uri) AS alt_schemas_json
	FROM transforms
	WHERE source_schema_uri IS NOT NULL
	GROUP BY collection_id
)
SELECT
    collection_id,
    collection_name,
    schema_uri,
    key_json,
    resource_id,
	IFNULL(partitions_json,  '[]') AS partitions_json,
	IFNULL(projections_json, '[]') AS projections_json,
    IFNULL(alt_schemas_json, '[]') AS alt_schemas_json,
    derivations.collection_id IS NOT NULL AS is_derivation,
    register_schema_uri,
    register_initial_json
FROM collections
NATURAL LEFT JOIN collection_partitions
NATURAL LEFT JOIN collection_projections
NATURAL LEFT JOIN collection_alt_schemas
NATURAL LEFT JOIN derivations
;

-- View over all schemas which apply to a collection.
-- DEPRECATED. Use `collection_details` instead.
CREATE VIEW collection_schemas AS
SELECT collection_id,
       collection_name,
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

-- View over schema URIs and their extracted fields, with context.
CREATE VIEW schema_extracted_fields AS
SELECT
    c.schema_uri as schema_uri,
    k.value AS ptr,
    TRUE AS is_key,
    PRINTF('key of collection %Q', c.collection_name) AS context
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
    TRUE AS is_key,
    PRINTF('partitioned field %Q of collection %Q', p.field, c.collection_name)
FROM collections AS c NATURAL JOIN projections AS p NATURAL JOIN partitions
UNION
SELECT
    c.schema_uri,
    p.location_ptr,
    FALSE AS is_key,
    PRINTF('automatically projected field %Q of collection %Q', p.field, c.collection_name)
FROM collections AS c NATURAL JOIN projections AS p
WHERE p.user_provided = FALSE
UNION
SELECT
    c.schema_uri,
    p.location_ptr,
    FALSE AS is_key,
    PRINTF('user-specified projected field %Q of collection %Q', p.field, c.collection_name)
FROM collections AS c NATURAL JOIN projections AS p
WHERE p.user_provided = TRUE
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

-- Inferences are locations of collection documents and associated attributes
-- which are statically provable solely from the collection's JSON-Schema.
CREATE TABLE inferences
(
    -- Collection to which this inference pertains.
    collection_id                     INTEGER NOT NULL REFERENCES collections (collection_id),
    -- Field name for the projection
    field                             TEXT NOT NULL,
    -- Possible types for this location.
    -- Subset of ["null", "boolean", "object", "array", "integer", "numeric", "string"].
    types_json                        TEXT    NOT NULL CHECK (JSON_TYPE(types_json) == 'array'),

    -- Strings end up being used to represent a variety of different things, e.g. dates, xml, or binary
    -- content, which may benefit for specialized storage in other systems. So we store a lot more
    -- metadata on strings than we do for other types in case we're able to use it during
    -- materialization.
    -- If of type "string", media MIME type of its content.
    string_content_type               TEXT,
    -- If of type "string", is the value base64-encoded ?
    string_content_encoding_is_base64 BOOLEAN CHECK (string_content_encoding_is_base64 IN (0,1)),
    -- If the location is a "string" type and has a maximum length, it will be here
    string_max_length                 INTEGER,

    FOREIGN KEY (collection_id, field)
        REFERENCES projections(collection_id, field)
);

CREATE TABLE build_info
(
    time TEXT DEFAULT(datetime('now', 'utc')) NOT NULL,
    description TEXT NOT NULL
);

INSERT INTO build_info (description) VALUES ('db schema initialized');

