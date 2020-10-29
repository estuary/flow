PRAGMA foreign_keys = ON;

-- Unique resources (eg, files) from which this catalog is built.
--
-- :content_type:
--      MIME type of the resource.
-- :content:
--      Content of this resource.
-- :is_processed:
--      Marks the resource as having been processed.
CREATE TABLE resources (
    resource_id INTEGER PRIMARY KEY NOT NULL,
    content_type TEXT NOT NULL,
    content BLOB NOT NULL,
    is_processed BOOLEAN NOT NULL,
    CONSTRAINT "Invalid resource content-type" CHECK (
        content_type IN (
            'application/vnd.estuary.dev-catalog-spec+yaml',
            'application/schema+yaml',
            'application/sql',
            'application/vnd.estuary.dev-catalog-npm-pack'
        )
    )
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
CREATE TABLE resource_imports (
    resource_id INTEGER NOT NULL REFERENCES resources (resource_id),
    import_id INTEGER NOT NULL REFERENCES resources (resource_id),
    PRIMARY KEY (resource_id, import_id)
);

--View which derives all transitive resource imports
CREATE VIEW resource_transitive_imports AS WITH RECURSIVE cte(resource_id, import_id) AS (
    SELECT resource_id,
        resource_id
    FROM resources
    UNION ALL
    SELECT cte.resource_id,
        ri.import_id
    FROM resource_imports AS ri
        JOIN cte ON ri.resource_id = cte.import_id
)
SELECT *
FROM cte;

-- Don't allow a resource import which is already transitively imported
-- in the opposite direction. To do so would allow a cycle in the import graph.
CREATE TRIGGER assert_resource_imports_are_acyclic BEFORE
INSERT ON resource_imports FOR EACH ROW
    WHEN (
        SELECT 1
        FROM resource_transitive_imports
        WHERE resource_id = NEW.import_id
            AND import_id = NEW.resource_id
    ) NOT NULL BEGIN
SELECT RAISE(
        ABORT,
        'Import creates an cycle (imports must be acyclic)'
    );

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
CREATE TABLE resource_urls (
    resource_id INTEGER NOT NULL REFERENCES resources (resource_id),
    url TEXT UNIQUE NOT NULL,
    is_primary BOOLEAN,
    UNIQUE (resource_id, is_primary),
    CONSTRAINT "URL must be a valid, base (non-relative) URL" CHECK (url LIKE '_%://_%'),
    CONSTRAINT "URL cannot have a fragment component" CHECK (url NOT LIKE '%#%'),
    CONSTRAINT "is_primary should be 'true' or NULL" CHECK (
        is_primary IS TRUE
        OR is_primary IS NULL
    )
);

-- Resource schemas is a view over all JSON-Schemas which are transitively
-- imported or referenced from a given resource_id. In other words, this is
-- the set of JSON-Schemas which must be compiled and indexed when validating
-- on behalf of the given resource.
CREATE VIEW resource_schemas AS
SELECT rti.resource_id AS resource_id,
    resource_urls.url AS schema_uri,
    resources.content AS schema_content
FROM resource_transitive_imports AS rti
    JOIN resources ON rti.import_id = resources.resource_id
    JOIN resource_urls ON rti.import_id = resource_urls.resource_id
WHERE resources.content_type = 'application/schema+yaml'
    AND resource_urls.is_primary
GROUP BY rti.resource_id,
    rti.import_id;

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
CREATE TABLE lambdas (
    lambda_id INTEGER PRIMARY KEY NOT NULL,
    runtime TEXT NOT NULL,
    inline TEXT,
    resource_id INTEGER REFERENCES resources (resource_id),
    CONSTRAINT "Unknown Lambda runtime" CHECK (
        runtime IN ('nodeJS', 'sqlite', 'sqliteFile', 'remote')
    ),
    CONSTRAINT "NodeJS lambda must provide an inline expression" CHECK (
        runtime != 'nodeJS'
        OR (
            inline NOT NULL
            AND resource_id IS NULL
        )
    ),
    CONSTRAINT "SQLite lambda must provide an inline expression" CHECK (
        runtime != 'sqlite'
        OR (
            inline NOT NULL
            AND resource_id IS NULL
        )
    ),
    CONSTRAINT "SQLiteFile lambda must provide a file resource" CHECK (
        runtime != 'sqliteFile'
        OR (
            inline IS NULL
            AND resource_id IS NOT NULL
        )
    ),
    CONSTRAINT "Remote lambda must provide an HTTP endpoint URL" CHECK (
        runtime != 'remote'
        OR (
            inline LIKE '_%://_%'
            AND resource_id IS NULL
        )
    )
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
CREATE TABLE collections (
    collection_id INTEGER PRIMARY KEY NOT NULL,
    collection_name TEXT NOT NULL,
    schema_uri TEXT NOT NULL,
    key_json TEXT NOT NULL,
    resource_id INTEGER NOT NULL REFERENCES resources (resource_id),
    UNIQUE(collection_name COLLATE NOCASE) CONSTRAINT "Collection name isn't valid (may include Unicode letters, numbers, -, _, ., or /)" CHECK (
        collection_name REGEXP '^[\pL\pN\-_./]+$'
    ),
    CONSTRAINT "Collection name isn't valid (may not end in '/')" CHECK (collection_name NOT LIKE '%/'),
    CONSTRAINT "Schema must be a valid base (non-relative) URI" CHECK (schema_uri LIKE '_%://_%'),
    CONSTRAINT "Key must be non-empty JSON array of JSON-Pointers" CHECK (JSON_ARRAY_LENGTH(key_json) > 0)
);

-- No collection name may prefix any other collection name.
CREATE TRIGGER one_collection_cannot_prefix_another BEFORE
INSERT ON collections FOR EACH ROW
    WHEN (
        SELECT 1
        FROM collections
        WHERE collection_name LIKE NEW.collection_name || '/%' COLLATE NOCASE
            OR NEW.collection_name LIKE collection_name || '/%' COLLATE NOCASE
    ) NOT NULL BEGIN
SELECT RAISE(
        ABORT,
        'A collection name cannot be a prefix of another collection name'
    );

END;

-- Materialization targets for a collection
--
-- :target_name:
--     Human-readable name of the materialization that was provided in the catalog spec
-- :target_type:
--     The type of database to materialize into. This must be either 'postgres' or 'sqlite'
-- :target_uri:
--     The (database) connection uri provided in the catalog spec
CREATE TABLE materialization_targets (
    target_id INTEGER PRIMARY KEY NOT NULL,
    target_name TEXT NOT NULL,
    target_type TEXT NOT NULL CONSTRAINT 'target_type must be a recognized type' CHECK(target_type IN ('postgres', 'sqlite')),
    target_uri TEXT NOT NULL,
    UNIQUE (target_name COLLATE NOCASE),
    CONSTRAINT "Materialization Target name isn't valid (may include Unicode letters, numbers, -, _, ., or /)" CHECK (target_name REGEXP '^[\pL\pN\-_./]+$')
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
-- :user_provided:
--      Whether the projection was provided by the user in the catalog spec. If false, then
--      the projection was generated automatically.
CREATE TABLE projections (
    collection_id INTEGER NOT NULL REFERENCES collections (collection_id),
    field TEXT NOT NULL,
    location_ptr TEXT NOT NULL,
    user_provided BOOLEAN NOT NULL,
    PRIMARY KEY (collection_id, field),
    CONSTRAINT "Location must be a valid JSON-Pointer" CHECK (location_ptr REGEXP '^(/[^/]+)*$')
);

-- Partitions are projections which logically partition the collection.
--
-- :collection_id:
--      Collection to which this projection pertains.
-- :field:
--      Field of this partition.
CREATE TABLE PARTITIONS (
    collection_id INTEGER NOT NULL,
    field TEXT NOT NULL,
    CONSTRAINT "Projection field isn't valid for use as a partition (may include Unicode letters, numbers, -, _, .)" CHECK (field REGEXP '^[\pL\pN\-_.]+$'),
    PRIMARY KEY (collection_id, field),
    FOREIGN KEY (collection_id, field) REFERENCES projections(collection_id, field)
);

-- Type information that's been extracted from collection schemas.
--
-- :schema_uri:
--     The URI of the schema that the inference was derived from.
-- :location_ptr:
--     Json pointer of the location within the document to which this inference pertains
-- :types_json:
--     The possible types for this location.
--     Subset of ["null", "boolean", "object", "array", "integer", "numeric", "string"].
-- :must_exist:
--     Whether location pointer references a field that must always exist. This will be false if any
--     parent object or array is not required in the schema. The _value_ of the field may still be
--     null, even if must_exist is true. So to check whether a field is nullable, you need to check
--     both this field and types_json.
-- :title:
--     The title from the schema, if specified.
-- :description:
--     The description from the schema, if specified.
-- :string_content_type:
--     Strings end up being used to represent a variety of different things, e.g. dates, xml, or binary
--     content, which may benefit for specialized storage in other systems. So we store a lot more
--     metadata on strings than we do for other types in case we're able to use it during
--     materialization. If of type "string", media MIME type of its content.
-- :string_format:
--     If the json schema had a "format" annotation, then its value should be persisted here.
--     Examples of common formats are "email", "date-time", "hostname", "uri", and "ipv4".
-- :string_content_encoding_is_base64:
--     If of type "string", is the value base64-encoded?
-- :string_max_length:
--     If the location is a "string" type and has a maximum length, it will be here.
CREATE TABLE inferences (
    schema_uri TEXT NOT NULL,
    location_ptr TEXT NOT NULL,
    types_json TEXT NOT NULL CHECK (JSON_TYPE(types_json) == 'array'),
    must_exist BOOLEAN NOT NULL,
    title TEXT,
    description TEXT,
    string_content_type TEXT,
    string_format TEXT,
    string_content_encoding_is_base64 BOOLEAN,
    string_max_length INTEGER,
    PRIMARY KEY (schema_uri, location_ptr),
    CONSTRAINT "Schema must be a valid base (non-relative) URI" CHECK (schema_uri LIKE '_%://_%') CONSTRAINT "Location must be a valid JSON-Pointer" CHECK (location_ptr REGEXP '^(/[^/]+)*$')
);

-- Partition selectors express a selection of partitions of a collection.
CREATE TABLE partition_selectors (
    selector_id INTEGER PRIMARY KEY NOT NULL,
    collection_id INTEGER NOT NULL,
    UNIQUE(selector_id, collection_id)
);

-- Individual field values which constitute a partition selector.
--
-- :selector_id:
--      ID of this selector.
-- :collection_id:
--      Collection which is selected over.
-- :field:
--      Partitioned field of the collection.
-- :value_json:
--      JSON-encoded value to be matched.
-- :is_exclude:
--      If true, this record is a selector exclusion (as opposed to an inclusion).
-- TODO(johnny): Rename to `partition_selector_values`
CREATE TABLE partition_selector_labels (
    selector_id INTEGER NOT NULL,
    collection_id INTEGER NOT NULL,
    field TEXT NOT NULL,
    value_json TEXT NOT NULL,
    is_exclude BOOLEAN NOT NULL,
    FOREIGN KEY(selector_id, collection_id) REFERENCES partition_selectors(selector_id, collection_id),
    FOREIGN KEY(collection_id, field) REFERENCES PARTITIONS(collection_id, field),
    CONSTRAINT "Value must be a key-able type (null, boolean, integer, or text)" CHECK (
        JSON_TYPE(value_json) IN ('null', 'true', 'false', 'integer', 'text')
    ),
    CONSTRAINT "Value cannot be the empty string" CHECK (
        JSON_TYPE(value_json) != 'text'
        OR JSON_EXTRACT(value_json, '$') != ''
    )
);

-- View over partition_selectors which groups into a JSON object
-- matching the schema of specs::PartitionSelector.
CREATE VIEW partition_selectors_json AS WITH grouped_fields AS (
    SELECT selector_id,
        collection_id,
        field,
        is_exclude,
        JSON_GROUP_ARRAY(JSON(value_json)) AS values_json
    FROM partition_selector_labels
    GROUP BY selector_id,
        collection_id,
        field,
        is_exclude
),
grouped_include_exclude AS (
    SELECT selector_id,
        collection_id,
        JSON_GROUP_OBJECT(field, JSON(values_json)) FILTER (
            WHERE NOT is_exclude
        ) AS include_json,
        JSON_GROUP_OBJECT(field, JSON(values_json)) FILTER (
            WHERE is_exclude
        ) AS exclude_json
    FROM grouped_fields
    GROUP BY selector_id,
        collection_id
)
SELECT selector_id,
    collection_id,
    JSON_OBJECT(
        'include',
        JSON(include_json),
        'exclude',
        JSON(exclude_json)
    ) AS selector_json
FROM grouped_include_exclude;

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
CREATE TABLE derivations (
    collection_id INTEGER PRIMARY KEY NOT NULL REFERENCES collections (collection_id),
    register_schema_uri TEXT NOT NULL,
    register_initial_json TEXT NOT NULL,
    CONSTRAINT "Register schema must be a valid base (non-relative) URI" CHECK (register_schema_uri LIKE '_%://_%') CONSTRAINT "Initial Register must be valid JSON" CHECK (JSON_VALID(register_initial_json))
);

-- Bootstraps relate a derivation and lambdas which are invoked to initialize it.
--
-- :derivation_id:
--      Derivation to which this bootstrap lambda applies.
-- :lambda_id:
--      Lambda expression to invoke on processor bootstrap.
CREATE TABLE bootstraps (
    bootstrap_id INTEGER PRIMARY KEY NOT NULL,
    derivation_id INTEGER NOT NULL REFERENCES derivations (collection_id),
    lambda_id INTEGER NOT NULL REFERENCES lambdas (lambda_id)
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
CREATE TABLE transforms (
    transform_id INTEGER PRIMARY KEY NOT NULL,
    derivation_id INTEGER NOT NULL REFERENCES derivations (collection_id),
    transform_name TEXT NOT NULL,
    source_collection_id INTEGER NOT NULL REFERENCES collections (collection_id),
    source_selector_id INTEGER,
    update_id INTEGER REFERENCES lambdas (lambda_id),
    publish_id INTEGER REFERENCES lambdas (lambda_id),
    source_schema_uri TEXT,
    shuffle_key_json TEXT,
    read_delay_seconds INTEGER CHECK (read_delay_seconds > 0),
    -- Name must be unique amoung transforms of the derivation.
    UNIQUE(transform_name COLLATE NOCASE, derivation_id),
    FOREIGN KEY(source_selector_id, source_collection_id) REFERENCES partition_selectors(selector_id, collection_id),
    CONSTRAINT "Transform name isn't valid (may include Unicode letters, numbers, -, _, .)" CHECK (transform_name REGEXP '^[\pL\pN\-_.]+$'),
    CONSTRAINT "Source schema must be NULL or a valid base (non-relative) URI" CHECK (source_schema_uri LIKE '_%://_%'),
    CONSTRAINT "Shuffle key must be NULL or non-empty JSON array of JSON-Pointers" CHECK (JSON_ARRAY_LENGTH(shuffle_key_json) > 0),
    CONSTRAINT "Must set at least one of 'update' or 'publish' lambdas" CHECK (
        (update_id NOT NULL)
        OR (publish_id NOT NULL)
    )
);

-- All transforms of a derivation reading from the same source, must also use the same source schema.
CREATE TRIGGER transforms_use_consistent_source_schema BEFORE
INSERT ON transforms FOR EACH ROW
    WHEN (
        SELECT 1
        FROM transforms
        WHERE derivation_id = NEW.derivation_id
            AND source_collection_id = NEW.source_collection_id
            AND COALESCE(source_schema_uri, '') != COALESCE(NEW.source_schema_uri, '')
    ) NOT NULL BEGIN
SELECT RAISE(
        ABORT,
        'Transforms of a derived collection which read from the same source collection must use the same source schema URI'
    );

END;

-- If the source_schema_uri is the same as the schema_uri of the source collection, then we'll raise
-- an error. This condition would not necessarily affect correctness, but it would essentially have
-- no effect, and so we'll assume that this isn't what the user intended and raise an error.
CREATE TRIGGER transforms_source_schema_different_from_collection_schema BEFORE
INSERT ON transforms FOR EACH ROW
    WHEN (
        SELECT schema_uri
        FROM collections
        WHERE collection_id = NEW.source_collection_id
    ) = NEW.source_schema_uri BEGIN
SELECT RAISE(
        ABORT,
        "Transforms source schema is the same as the source collection schema. This is disallowed, as it's redundant and would have no effect"
    );

END;

-- If the shuffle_key is the same as the key of the source collection, then we'll raise
-- an error. Much like the source_schema_uri restriction above, the explicit shuffle key
-- has no effect.
CREATE TRIGGER transforms_shuffle_key_different_from_collection_key BEFORE
INSERT ON transforms FOR EACH ROW
    WHEN (
        SELECT key_json
        FROM collections
        WHERE collection_id = NEW.source_collection_id
    ) = NEW.shuffle_key_json BEGIN
SELECT RAISE(
        ABORT,
        "Transform shuffle key is the same as the source collection key. This is disallowed, as it's redundant and would have no effect"
    );

END;

-- Require that the specification resource which defines a collection transform,
-- also imports the specification which contains the referenced source collection.
CREATE TRIGGER transforms_import_source_collection BEFORE
INSERT ON transforms FOR EACH ROW
    WHEN (
        SELECT 1
        FROM collections AS src,
            collections AS tgt,
            resource_transitive_imports AS rti
        WHERE tgt.collection_id = NEW.derivation_id
            AND src.collection_id = NEW.source_collection_id
            AND tgt.resource_id = rti.resource_id
            AND src.resource_id = rti.import_id
    ) IS NULL BEGIN
SELECT RAISE(
        ABORT,
        'Transform references a source collection which is not imported by this catalog spec'
    );

END;

-- View which derives all transitive collection dependencies.
CREATE VIEW collection_transitive_dependencies AS WITH RECURSIVE cte(derivation_id, source_collection_id) AS (
    SELECT derivation_id,
        derivation_id
    FROM transforms
    UNION
    SELECT cte.derivation_id,
        t.source_collection_id
    FROM transforms AS t
        JOIN cte ON t.derivation_id = cte.source_collection_id
)
SELECT cte.derivation_id,
    c1.collection_name AS derivation_name,
    cte.source_collection_id,
    c2.collection_name AS source_name
FROM cte
    JOIN collections AS c1 ON cte.derivation_id = c1.collection_id
    JOIN collections AS c2 ON cte.source_collection_id = c2.collection_id;

-- Detail view of transforms joined with collection and lambda details,
-- and flattening NULL-able fields into their assumed defaults.
CREATE VIEW transform_details AS
SELECT transforms.transform_id,
    transforms.transform_name,
    -- Derivation details.
    derivations.register_schema_uri,
    -- Source collection details.
    transforms.source_collection_id,
    src.collection_name AS source_name,
    src.resource_id AS source_resource_id,
    COALESCE(transforms.source_schema_uri, src.schema_uri) AS source_schema_uri,
    source_selector.selector_json AS source_selector_json,
    transforms.source_schema_uri IS NOT NULL AS is_alt_source_schema,
    COALESCE(transforms.shuffle_key_json, src.key_json) AS shuffle_key_json,
    transforms.shuffle_key_json IS NULL AS uses_source_key,
    transforms.read_delay_seconds,
    -- Derived collection details.
    transforms.derivation_id,
    der.collection_name AS derivation_name,
    der.resource_id AS derivation_resource_id,
    der.schema_uri AS derivation_schema_uri,
    der.key_json AS derivation_key_json,
    -- Update lambda fields.
    transforms.update_id AS update_id,
    updates.runtime AS update_runtime,
    updates.inline AS update_inline,
    updates.resource_id AS update_resource_id,
    update_resources.content AS update_resource_content,
    -- Publish lambda fields.
    transforms.publish_id AS publish_id,
    publish.runtime AS publish_runtime,
    publish.inline AS publish_inline,
    publish.resource_id AS publish_resource_id,
    publish_resources.content AS publish_resource_content
FROM transforms
    JOIN collections AS src ON transforms.source_collection_id = src.collection_id
    JOIN collections AS der ON transforms.derivation_id = der.collection_id
    JOIN derivations ON transforms.derivation_id = derivations.collection_id
    LEFT JOIN lambdas AS updates ON transforms.update_id = updates.lambda_id
    LEFT JOIN resources AS update_resources ON updates.resource_id = update_resources.resource_id
    LEFT JOIN lambdas AS publish ON transforms.publish_id = publish.lambda_id
    LEFT JOIN resources AS publish_resources ON publish.resource_id = publish_resources.resource_id
    LEFT JOIN partition_selectors_json AS source_selector ON transforms.source_selector_id = source_selector.selector_id;

-- DEPRECATED: use collections_json instead
-- Detail view of collections joined with projections, partitions,
-- derivations, inferences, and alternate source schemas.
CREATE VIEW collection_details AS WITH collection_alt_schemas AS (
    SELECT source_collection_id AS collection_id,
        JSON_GROUP_ARRAY(DISTINCT source_schema_uri) AS alt_schemas_json
    FROM transforms
    WHERE source_schema_uri IS NOT NULL
    GROUP BY collection_id
)
SELECT collection_id,
    collection_name,
    schema_uri,
    key_json,
    resource_id,
    IFNULL(partition_fields_json, '[]') AS partition_fields_json,
    IFNULL(projections_json, '{}') AS projections_json,
    IFNULL(alt_schemas_json, '[]') AS alt_schemas_json,
    derivations.collection_id IS NOT NULL AS is_derivation,
    register_schema_uri,
    register_initial_json
FROM collections NATURAL
    LEFT JOIN projected_fields_json NATURAL
    LEFT JOIN collection_alt_schemas NATURAL
    LEFT JOIN derivations;

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

-- View of all the projected fields, and their inferred type information.
CREATE VIEW projected_fields AS
SELECT c.collection_id,
    c.schema_uri,
    c.collection_name,
    p.field,
    p.location_ptr,
    user_provided,
    types_json,
    must_exist,
    title,
    description,
    string_content_type,
    string_content_encoding_is_base64,
    string_format,
    string_max_length,
    CASE
        WHEN part.field IS NULL THEN FALSE
        ELSE TRUE
    END AS is_partition_key,
    CASE
        WHEN KEYS.value IS NULL THEN FALSE
        ELSE TRUE
    END AS is_primary_key
FROM collections AS c
    JOIN projections AS p ON c.collection_id = p.collection_id
    LEFT JOIN inferences ON c.schema_uri = inferences.schema_uri
    AND p.location_ptr = inferences.location_ptr
    LEFT JOIN PARTITIONS AS part ON p.collection_id = part.collection_id
    AND p.field = part.field
    LEFT JOIN json_each(c.key_json) AS KEYS ON KEYS.value = p.location_ptr;

-- View of all the projected fields and inferences, grouped as a JSON object.
-- These objects conform to the shape of the Projection type defined by the flow protocol.
CREATE VIEW projected_fields_json AS
SELECT collection_id,
    JSON_GROUP_ARRAY(
        -- JSON_PATCH here serves the purpose of trimming off any keys that have null values
        JSON_PATCH('{}',
            JSON_OBJECT(
                'field',
                field,
                'ptr',
                location_ptr,
                'user_provided',
                CASE
                    WHEN user_provided THEN JSON('true')
                    ELSE JSON('false')
                END,
                'is_partition_key',
                CASE
                    WHEN is_partition_key THEN JSON('true')
                    ELSE JSON('false')
                END,
                'is_primary_key',
                CASE
                    WHEN is_primary_key THEN JSON('true')
                    ELSE JSON('false')
                END,
                'inference',
                CASE
                    WHEN types_json IS NULL THEN NULL
                    ELSE JSON_OBJECT(
                        'types',
                        JSON(types_json),
                        'must_exist',
                        CASE
                            WHEN must_exist THEN JSON('true')
                            ELSE JSON('false')
                        END,
                        'string',
                        CASE
                            -- I know this is terrible, but it kinda seems preferable to creating a
                            -- CTE for this.
                            WHEN types_json LIKE '%"string"%' THEN
                                JSON_OBJECT(
                                    'content_type',
                                    string_content_type,
                                    'format',
                                    string_format,
                                    'is_base64',
                                    CASE
                                        WHEN string_content_encoding_is_base64 THEN JSON('true')
                                        ELSE JSON('false')
                                    END,
                                    'max_length',
                                    string_max_length
                                )
                            ELSE NULL
                        END
                    )
                END
            )
        )
    ) AS projections_json,
    JSON_GROUP_ARRAY(field) FILTER (
        WHERE is_partition_key
    ) AS partition_fields_json
FROM projected_fields
GROUP BY collection_id;

-- View of all collections as json objects that conform to the shape of a CollectionSpec as defined
-- by the flow protocol.
CREATE VIEW collections_json AS
SELECT
    c.collection_id,
    c.collection_name,
    derivations.collection_id IS NOT NULL AS is_derivation,
    JSON_OBJECT(
        'name',
        c.collection_name,
        'schema_uri',
        c.schema_uri,
        'key_ptrs',
        JSON(c.key_json),
        'partition_fields',
        JSON_GROUP_ARRAY(PARTITIONS.field) FILTER (WHERE PARTITIONS.field IS NOT NULL),
        'projections',
        JSON(projected_fields_json.projections_json)
    ) AS spec_json
FROM collections AS c
    NATURAL JOIN projected_fields_json
    NATURAL LEFT JOIN PARTITIONS
    NATURAL LEFT JOIN derivations
GROUP BY collection_id;


-- View of all the collection primary keys, partition keys, and shuffle keys. These keys have
-- constraints on the types of values that are used. The schema must ensure all of the following:
-- - The location always exists (cannot be undefined). It IS allowed to hold a null value though.
-- - The value must be of type string, integer, or boolean. No objects, arrays, or floats.
-- - The value must only have one possible type (besides null). Locations that may hold several
--   different types (e.g. string or integer) may not be used.
--
-- The query is a bit long and hairy, but the overall structure is to have CTEs for each error
-- condition that we can check just by looking at the inferences table, and then union a separate
-- query for each type of key that we're checking against those CTEs.
CREATE VIEW collection_keys AS WITH all_keys AS (
    -- collection primary keys
    SELECT c.collection_id,
        schema_uri,
        KEYS.value AS location_ptr,
        printf(
            'primary key of collection "%s"',
            c.collection_name
        ) AS source
    FROM collections AS c,
        json_each(c.key_json) AS KEYS -- partition keys
    UNION
    SELECT c.collection_id,
        schema_uri,
        projections.location_ptr,
        printf(
            'partition key for collection "%s"',
            c.collection_name
        ) AS source
    FROM collections AS c
        NATURAL JOIN PARTITIONS AS part
        NATURAL JOIN projections -- shuffle keys
    UNION
    SELECT c.collection_id,
        COALESCE(t.source_schema_uri, c.schema_uri) AS schema_uri,
        KEYS.value AS location_ptr,
        printf(
            'shuffle key from transform "%s"',
            t.transform_name
        ) AS source
    FROM transforms AS t,
        json_each(t.shuffle_key_json) AS KEYS
        JOIN collections AS c ON t.source_collection_id = c.collection_id
),
inferences_with_errors AS (
    SELECT schema_uri,
        location_ptr,
        types_json,
        must_exist,
        SUM(1) FILTER (
            WHERE one_type.value != 'null'
        ) AS num_non_null_types,
        CASE
            WHEN one_type.value IN ('object', 'array', 'number') THEN one_type.value
        END AS disallowed_type
    FROM inferences
        LEFT JOIN json_each(inferences.types_json) AS one_type
    GROUP BY schema_uri,
        location_ptr
)
SELECT all_keys.collection_id,
    all_keys.schema_uri,
    all_keys.location_ptr,
    i.types_json,
    i.must_exist,
    all_keys.source,
    CASE
        WHEN i.location_ptr IS NULL THEN 'No inferrence for this location (internal error).'
        WHEN types_json == '[]' THEN 'Schema is constrained such that the location cannot exist.'
        WHEN i.must_exist == FALSE THEN 'Location may not exist in all documents. Consider using "required" or "minItems".'
        WHEN i.num_non_null_types > 1 THEN printf(
            'Location may be %s, but locations used as keys may only have one possible type besides null.',
            i.types_json
        )
        WHEN i.disallowed_type NOT NULL THEN printf(
            'Location may be %s, but locations used as keys may not be objects, arrays, or floats.',
            i.types_json
        )
    END AS error
FROM all_keys
    LEFT JOIN inferences_with_errors AS i ON all_keys.schema_uri = i.schema_uri
    AND all_keys.location_ptr = i.location_ptr;

-- Map of NodeJS dependencies to bundle with the catalog's built NodeJS package.
-- :package:
--      Name of the NPM package depended on.
-- :version:
--      Version string, as understood by NPM.
--      See https://docs.npmjs.com/files/package.json#dependencies
CREATE TABLE nodejs_dependencies (
    package TEXT PRIMARY KEY NOT NULL,
    version TEXT NOT NULL
);

-- Abort an INSERT of a package that's already registered with a different version.
CREATE TRIGGER nodejs_dependencies_disagree BEFORE
INSERT ON nodejs_dependencies FOR EACH ROW BEGIN
SELECT CASE
        WHEN (
            SELECT 1
            FROM nodejs_dependencies
            WHERE package = NEW.package
                AND version != NEW.version
        ) THEN RAISE(
            ABORT,
            'A dependency on this nodeJS package at a different package version already exists'
        )
        WHEN (
            SELECT 1
            FROM nodejs_dependencies
            WHERE package = NEW.package
                AND version = NEW.version
        ) THEN RAISE(IGNORE)
    END;

END;

-- Test cases of the catalog.
--
-- :test_name:
--      Unique name of this test case.
-- :steps_json:
--      Encoded JSON array of steps of this test case.
-- :resource_id:
--      Catalog source spec which defines this test case.
CREATE TABLE test_cases (
    test_case_id INTEGER PRIMARY KEY NOT NULL,
    test_case_name TEXT UNIQUE NOT NULL,
    resource_id INTEGER NOT NULL REFERENCES resources (resource_id)
);

-- Ingest test case steps ingest documents into a collection.
--
-- :test_case_id:
--      ID of the test case.
-- :step_index:
--      Index of this test step within the overall test case.
-- :collection_id:
--      Collection into which the test step will ingest.
-- :documents_json:
--      Fixture of documents to ingest into the collection.
CREATE TABLE test_step_ingests (
    test_case_id INTEGER NOT NULL REFERENCES test_cases (test_case_id),
    step_index INTEGER NOT NULL,
    collection_id INTEGER NOT NULL REFERENCES collections (collection_id),
    documents_json TEXT NOT NULL,
    CONSTRAINT "Documents must be a JSON array" CHECK (JSON_TYPE(documents_json) == 'array')
);

-- Verify test case steps verify documents of a collection match a fixture expectation.
--
-- :test_case_id:
--      ID of the test case.
-- :step_index:
--      Index of this test step within the overall test case.
-- :collection_id:
--      Collection into which the test step will ingest.
-- :selector_id:
--      Optional selector of collection partitions which fixture documents must match.
-- :documents_json:
--      Fixture of documents to verify against the collection.
CREATE TABLE test_step_verifies (
    test_case_id INTEGER NOT NULL REFERENCES test_cases (test_case_id),
    step_index INTEGER NOT NULL,
    collection_id INTEGER NOT NULL REFERENCES collections (collection_id),
    selector_id INTEGER,
    documents_json TEXT NOT NULL,
    FOREIGN KEY(selector_id, collection_id) REFERENCES partition_selectors(selector_id, collection_id),
    CONSTRAINT "Documents must be a JSON array" CHECK (JSON_TYPE(documents_json) == 'array')
);

-- View which unions test step variant tables into a JSON object with
-- unified fields, where appropriate.
CREATE VIEW test_steps_json AS WITH steps AS (
    SELECT test_case_id,
        step_index,
        JSON_OBJECT(
            'ingest',
            JSON_OBJECT(
                'collection',
                collection_name,
                'documents',
                JSON(documents_json)
            )
        ) AS step_json
    FROM test_step_ingests
        NATURAL JOIN collections
    UNION ALL
    SELECT test_case_id,
        step_index,
        JSON_OBJECT(
            'verify',
            JSON_OBJECT(
                'collection',
                collection_name,
                'documents',
                JSON(documents_json),
                'partitions',
                JSON(selector_json)
            )
        ) AS step_json
    FROM test_step_verifies
        NATURAL JOIN collections NATURAL
        LEFT JOIN partition_selectors_json
)
SELECT *
FROM steps
ORDER BY test_case_id,
    step_index ASC;

-- View of test cases, with test steps grouped into an ordered JSON array.
CREATE VIEW test_cases_json AS
SELECT test_case_id,
    test_case_name,
    JSON_GROUP_ARRAY(JSON(step_json)) AS steps_json
FROM test_steps_json
    NATURAL JOIN test_cases
GROUP BY test_case_id
ORDER BY step_index ASC;

-- Contains informational and diagnostic data about the build itself. This is intended to be used
-- somewhat like a log.
CREATE TABLE build_info (
    time TEXT DEFAULT(datetime('now', 'utc')) NOT NULL,
    description TEXT NOT NULL
);

INSERT INTO build_info (description)
VALUES ('db schema initialized');
