-- Valid resources.
INSERT INTO resources (content_type, content, is_processed)
VALUES (
        'application/vnd.estuary.dev-catalog-spec+yaml',
        'catalog spec content',
        FALSE
    ),
    (
        'application/vnd.estuary.dev-catalog-spec+yaml',
        'included catalog content',
        TRUE
    ),
    (
        'application/schema+yaml',
        'json-schema content',
        FALSE
    ),
    ('application/schema+yaml', 'other schema', TRUE),
    (
        'application/schema+yaml',
        'yet more schema content',
        FALSE
    ),
    ('application/sql', 'bootstrap content', TRUE),
    ('application/sql', 'transform content', FALSE);

-- Invalid resource (unknown content type).
INSERT INTO resources (content_type, content, is_processed)
VALUES ('application/unknown', 'unknown content', FALSE);

-- Valid imports, which form a directed acyclic graph.
INSERT INTO resource_imports (resource_id, import_id)
VALUES -- 1 to 2 to 3 to 4.
    (1, 2),
    (3, 4),
    (2, 3),
    -- Disconnected 5 to 6.
    (5, 6);

-- Verify transitive relationships (one row for each path).
SELECT *
FROM resource_transitive_imports;

-- Invalid import (direct cycle).
INSERT INTO resource_imports (resource_id, import_id)
VALUES (4, 3);

-- Invalid import (indirect cycle).
INSERT INTO resource_imports (resource_id, import_id)
VALUES (4, 1);

-- Invalid import (implicit cycle).
INSERT INTO resource_imports (resource_id, import_id)
VALUES (6, 6);

-- Valid imports which include duplicates and alternate resource paths.
INSERT INTO resource_imports (resource_id, import_id)
VALUES (2, 3),
    -- Repeat.
    (1, 3),
    -- Alternate 1 to 3 to 4.
    (2, 4),
    -- Alternate 1 to 2 to 4.
    (5, 6) -- Repeat.
    ON CONFLICT DO NOTHING;

SELECT DISTINCT *
FROM resource_transitive_imports;

-- Valid resource URLs.
INSERT INTO resource_urls (resource_id, url, is_primary)
VALUES (1, 'file:///path/to/spec.yaml', TRUE),
    (
        2,
        'file:///path/to/included/catalog/spec.yaml',
        TRUE
    ),
    (3, 'file:///path/to/a/schema.yaml', TRUE),
    (4, 'https://host/path/schema?query=val', TRUE),
    (5, 'file:///path/to/other/schema.yaml', TRUE),
    (6, 'file:///path/to/some/bootstrap.sql', TRUE),
    (7, 'file:///path/to/some/transform.sql', TRUE),
    -- Alternate resource URLs.
    -- Each resource may have multiple alternate URLs.
    (3, 'https://canonical/schema/uri', NULL),
    (4, 'https://redirect-1/schema', NULL),
    (4, 'https://redirect-2/schema', NULL),
    (4, 'https://redirect-3/schema', NULL);

-- Invalid URL (not a base URL).
INSERT INTO resource_urls (resource_id, url)
VALUES (1, 'relative/url');

-- Invalid URL (cannot have a #fragment).
INSERT INTO resource_urls (resource_id, url)
VALUES (1, 'https://host/path/with#fragment');

-- Invalid URL (duplicated URL).
INSERT INTO resource_urls (resource_id, url)
VALUES (2, 'file:///path/to/spec.yaml');

-- Invalid URL (resource already has a primary URL).
INSERT INTO resource_urls (resource_id, url, is_primary)
VALUES (1, 'file:///path/to/dup/primary/spec.yaml', TRUE);

-- Expect we can natural join resources to URLs.
SELECT *
FROM resources
    NATURAL JOIN resource_urls;

-- View over all transitive JSON-Schemas.
SELECT *
FROM resource_schemas;

-- Valid lambdas.
INSERT INTO lambdas (runtime, inline, resource_id)
VALUES (
        'nodeJS',
        '(state) => { console.log(''hello''); }',
        NULL
    ),
    (
        'nodeJS',
        '(doc, state) => {...doc, key: 1}',
        NULL
    ),
    ('sqlite', 'SELECT 1;', NULL),
    ('sqliteFile', NULL, 6),
    -- Bootstrap.
    ('sqliteFile', NULL, 7),
    -- Transform.
    ('remote', 'https://remote/endpoint', NULL),
    (
        'nodeJS',
        '(doc, state) => {...doc, foo: true}',
        NULL
    );

-- Invalid lambda (unknown runtime).
INSERT INTO lambdas (runtime, inline)
VALUES ('unknown', 'foobar');

-- Invalid lambda (nodeJS without inline expression).
INSERT INTO lambdas (runtime, resource_id)
VALUES ('nodeJS', 4);

-- Invalid lambda (sqlite without inline expression).
INSERT INTO lambdas (runtime, resource_id)
VALUES ('sqlite', 6);

-- Invalid lambda (sqliteFile with inline expression).
INSERT INTO lambdas (runtime, inline)
VALUES ('sqliteFile', 'SELECT 1;');

-- Invalid lambda (sqliteFile with non-existent resource).
INSERT INTO lambdas (runtime, resource_id)
VALUES ('sqliteFile', 42);

-- Invalid lambda (remote without a valid URL).
INSERT INTO lambdas (runtime, inline)
VALUES ('remote', 'not-a-URL');

-- Expect we can natural join lambdas to resources.
SELECT *
FROM lambdas NATURAL
    LEFT JOIN resources;

-- Valid collections.
INSERT INTO collections (
        collection_name,
        schema_uri,
        key_json,
        resource_id
    )
VALUES (
        'col/src',
        'file:///path/to/a/schema.yaml#anchor',
        '["/key/0","/key/1"]',
        3
    ),
    (
        'col/derived',
        'https://canonical/schema/uri#/$defs/path',
        '["/foo"]',
        2
    ),
    (
        'col/der.iv-e_d',
        'https://canonical/schema/uri#/$defs/path',
        '["/foo"]',
        1
    ),
    (
        'col/srcs/other',
        'file:///path/to/a/schema.yaml',
        '["/key"]',
        6
    );

-- Invalid collection (schema is not a URI).
INSERT INTO collections (
        collection_name,
        schema_uri,
        key_json,
        resource_id
    )
VALUES ('col/bad', 'not-a-uri', '["/key"]', 1);

-- Invalid collection (collection name doesn't match pattern).
INSERT INTO collections (
        collection_name,
        schema_uri,
        key_json,
        resource_id
    )
VALUES (
        'spaces not allowed',
        'file:///path/to/a/schema.yaml',
        '["/key"]',
        1
    );

INSERT INTO collections (
        collection_name,
        schema_uri,
        key_json,
        resource_id
    )
VALUES (
        'bad!',
        'file:///path/to/a/schema.yaml',
        '["/key"]',
        1
    );

-- Invalid collections (a collection name cannot prefix another collection name).
INSERT INTO collections (
        collection_name,
        schema_uri,
        key_json,
        resource_id
    )
VALUES (
        'col/Src/extra',
        'file:///path/to/a/schema.yaml',
        '["/key"]',
        1
    );

INSERT INTO collections (
        collection_name,
        schema_uri,
        key_json,
        resource_id
    )
VALUES (
        'coL/srcs',
        'file:///path/to/a/schema.yaml',
        '["/key"]',
        1
    );

INSERT INTO collections (
        collection_name,
        schema_uri,
        key_json,
        resource_id
    )
VALUES (
        'coL',
        'file:///path/to/a/schema.yaml',
        '["/key"]',
        1
    );

-- Invalid collection (cannot end in '/')
INSERT INTO collections (
        collection_name,
        schema_uri,
        key_json,
        resource_id
    )
VALUES (
        'foobar/',
        'file:///path/to/a/schema.yaml',
        '["/key"]',
        1
    );

-- Invalid collection (key is not non-empty [JSON-Pointer]).
INSERT INTO collections (
        collection_name,
        schema_uri,
        key_json,
        resource_id
    )
VALUES (
        'col/bad',
        'file:///path/to/a/schema.yaml',
        '["malformed"',
        1
    );

INSERT INTO collections (
        collection_name,
        schema_uri,
        key_json,
        resource_id
    )
VALUES (
        'col/bad',
        'file:///path/to/a/schema.yaml',
        '{"not":"array"}',
        1
    );

INSERT INTO collections (
        collection_name,
        schema_uri,
        key_json,
        resource_id
    )
VALUES (
        'col/bad',
        'file:///path/to/a/schema.yaml',
        '[]',
        1
    );

-- Invalid collection (resource doesn't exist).
INSERT INTO collections (
        collection_name,
        schema_uri,
        key_json,
        resource_id
    )
VALUES (
        'col/bad',
        'file:///path/to/a/schema.yaml',
        '["/key"]',
        42
    );

SELECT *
FROM collections;

-- Valid projections.
INSERT INTO projections (
        collection_id,
        field,
        location_ptr,
        user_provided
    )
VALUES (1, 'field_1', '/key/0', TRUE),
    (1, 'field_2', '/key/1', FALSE),
    (1, 'field/3', '/path/3', FALSE),
    -- Repeat field name with different collection.
    (2, 'field_1', '', FALSE),
    (2, 'field_a', '/a', TRUE);

-- Invalid projection (no such collection).
INSERT INTO projections (
        collection_id,
        field,
        location_ptr,
        user_provided
    )
VALUES (42, 'foo', '/bar', TRUE);

-- Invalid projection (invalid JSON-Pointer).
INSERT INTO projections (
        collection_id,
        field,
        location_ptr,
        user_provided
    )
VALUES (1, 'foo', 'bar', TRUE);

INSERT INTO projections (
        collection_id,
        field,
        location_ptr,
        user_provided
    )
VALUES (1, 'foo', '/bar/', TRUE);

-- Valid projections which are partitions.
INSERT INTO PARTITIONS (collection_id, field)
VALUES (1, 'field_2'),
    (2, 'field_a');

-- Invalid partitions (no such collection).
INSERT INTO PARTITIONS (collection_id, field)
VALUES (42, 'field_2');

-- Invalid partition (no such projection).
INSERT INTO PARTITIONS (collection_id, field)
VALUES (1, 'field_zzz');

-- Invalid partition (projection field is not suitable).
INSERT INTO PARTITIONS (collection_id, field)
VALUES (1, 'field/3');

-- Valid partition selectors.
INSERT INTO partition_selectors (selector_id, collection_id)
VALUES (312, 1),
    (231, 1);

INSERT INTO partition_selector_labels (
        selector_id,
        collection_id,
        field,
        value_json,
        is_exclude
    )
VALUES (312, 1, 'field_2', 'true', FALSE),
    (312, 1, 'field_2', 'false', FALSE),
    (231, 1, 'field_2', 'null', FALSE),
    (231, 1, 'field_2', '456', TRUE),
    (231, 1, 'field_2', '"789"', TRUE);

-- Case: invalid json.
INSERT INTO partition_selector_labels (
        selector_id,
        collection_id,
        field,
        value_json,
        is_exclude
    )
VALUES (231, 1, 'field_2', '{"invalid":', FALSE);

-- Case: not a scalar (array).
INSERT INTO partition_selector_labels (
        selector_id,
        collection_id,
        field,
        value_json,
        is_exclude
    )
VALUES (231, 1, 'field_2', '[12,34]', FALSE);

-- Case: not a scalar (real).
INSERT INTO partition_selector_labels (
        selector_id,
        collection_id,
        field,
        value_json,
        is_exclude
    )
VALUES (231, 1, 'field_2', '12.34', FALSE);

-- Case: text cannot be empty.
INSERT INTO partition_selector_labels (
        selector_id,
        collection_id,
        field,
        value_json,
        is_exclude
    )
VALUES (231, 1, 'field_2', '""', FALSE);

-- Case: projection field doesn't exist.
INSERT INTO partition_selector_labels (
        selector_id,
        collection_id,
        field,
        value_json,
        is_exclude
    )
VALUES (231, 1, 'field_zzz', 'true', FALSE);

-- Case: projection field exists, but is not a logical partition.
INSERT INTO partition_selector_labels (
        selector_id,
        collection_id,
        field,
        value_json,
        is_exclude
    )
VALUES (231, 1, 'field_1', 'true', FALSE);

-- Case: projection is a logical partition of another collection (not this source).
INSERT INTO partition_selector_labels (
        selector_id,
        collection_id,
        field,
        value_json,
        is_exclude
    )
VALUES (231, 1, 'field_a', 'true', FALSE);

-- partition_selectors_json is a view which groups-up selector labels into a
-- JSON structure which matches the shape of protocol.LabelSelector.
SELECT *
FROM partition_selectors_json;

-- Valid derivations.
INSERT INTO derivations (
        collection_id,
        register_schema_uri,
        register_initial_json
    )
VALUES (
        2,
        "file:///path/to/a/schema.yaml#register",
        "{}"
    ),
    (
        3,
        "file:///path/to/a/schema.yaml#other-register",
        "[]"
    );

-- Register schema must not be NULL.
UPDATE derivations
SET register_schema_uri = NULL;

-- Register initial JSON must not be NULL.
UPDATE derivations
SET register_initial_json = NULL;

-- Register initial JSON must be JSON.
UPDATE derivations
SET register_initial_json = "[";

-- Invalid derivation (collection must exist).
INSERT INTO derivations (
        collection_id,
        register_schema_uri,
        register_initial_json
    )
VALUES (
        42,
        "file:///path/to/a/schema.yaml#register",
        "1"
    );

-- Invalid derivation (schema is not a URI).
INSERT INTO derivations (
        collection_id,
        register_schema_uri,
        register_initial_json
    )
VALUES (1, 'not-a-uri', "1");

-- Valid bootstrap.
INSERT INTO bootstraps (derivation_id, lambda_id)
VALUES (2, 1),
    (2, 4),
    (3, 1);

-- Invalid bootstrap (derivation must exist).
INSERT INTO bootstraps (derivation_id, lambda_id)
VALUES (42, 1);

-- Invalid bootstrap (collection_id 1 is not a derivation).
INSERT INTO bootstraps (derivation_id, lambda_id)
VALUES (1, 1);

-- Invalid bootstrap (lambda must exist).
INSERT INTO bootstraps (derivation_id, lambda_id)
VALUES (2, 42);

-- Expect we can natural join from bootstraps to resources.
SELECT *
FROM bootstraps NATURAL
    LEFT JOIN lambdas NATURAL
    LEFT JOIN resources;

-- Valid transforms.
INSERT INTO transforms (
        transform_name,
        derivation_id,
        source_collection_id,
        source_selector_id,
        update_id,
        publish_id,
        read_delay_seconds,
        source_schema_uri
    )
VALUES ("one", 2, 1, NULL, 2, NULL, NULL, NULL),
    ("two", 2, 1, 231, NULL, 3, NULL, NULL),
    ("3re", 2, 1, NULL, NULL, 5, 60, NULL),
    ("4or", 2, 1, 312, 6, NULL, 120, NULL),
    (
        "one",
        3,
        1,
        NULL,
        5,
        7,
        1,
        'https://alt/source/schema#anchor'
    );

-- Name must be set.
UPDATE transforms
SET transform_name = NULL
WHERE transform_id = 1;

-- It must be unique to the derivation.
UPDATE transforms
SET transform_name = "two"
WHERE transform_id = 1;

-- And of a restricted character set.
UPDATE transforms
SET transform_name = "o+ne"
WHERE transform_id = 1;

-- Invalid source-schema
UPDATE transforms
SET source_schema_uri = 'not-a-url'
WHERE transform_id = 1;

-- Invalid source schema is the same as the collection schema
INSERT INTO transforms (
        derivation_id,
        source_collection_id,
        source_schema_uri
    )
VALUES (2, 1, 'file:///path/to/a/schema.yaml#anchor');

-- Shuffle-key must be array of JSON-pointers.
UPDATE transforms
SET shuffle_key_json = '["/malformed'
WHERE transform_id = 1;

UPDATE transforms
SET shuffle_key_json = '{"not":"array"}'
WHERE transform_id = 1;

-- OK.
UPDATE transforms
SET shuffle_key_json = '["/key"]'
WHERE transform_id = 1;

-- Invalid shuffle key is the same as the source collection.
INSERT INTO transforms (
        derivation_id,
        source_collection_id,
        shuffle_key_json
    )
VALUES (2, 1, '["/key/0","/key/1"]');

-- Derivation must exist (not a collection).
UPDATE transforms
SET derivation_id = 42
WHERE transform_id = 1;

-- Derivation must exist (a collection, but not a derivation).
UPDATE transforms
SET derivation_id = 1
WHERE transform_id = 1;

-- Source collection must exist.
UPDATE transforms
SET source_collection_id = 42;

-- Update & publish lambdas must exist.
UPDATE transforms
SET publish_id = 42;

UPDATE transforms
SET update_id = 42;

-- At least one of update and publish must be set.
UPDATE transforms
SET publish_id = NULL,
    update_id = NULL
WHERE transform_id = 1;

-- Read delay must be positive.
UPDATE transforms
SET read_delay_seconds = 0;

-- The resource of the spec defining this transform must also import the
-- spec of the referenced source collection.
INSERT INTO transforms (
        transform_name,
        derivation_id,
        source_collection_id,
        publish_id
    )
VALUES ("fails", 2, 4, 2);

-- Transforms of a single derivation reading from the same source collection
-- must all use the same source schema URI.
-- Case: existing transform with same schema (success).
INSERT INTO transforms (
        transform_name,
        derivation_id,
        source_collection_id,
        publish_id,
        source_schema_uri
    )
VALUES (
        "works",
        3,
        1,
        7,
        'https://alt/source/schema#anchor'
    );

-- Case: existing transform with explicit different schema (fails).
INSERT INTO transforms (
        transform_name,
        derivation_id,
        source_collection_id,
        publish_id,
        source_schema_uri
    )
VALUES (
        "fails",
        3,
        1,
        7,
        'https://alt/source/schema#different-anchor'
    );

-- Case: existing transform with null source-schema (fails).
INSERT INTO transforms (
        transform_name,
        derivation_id,
        source_collection_id,
        publish_id,
        source_schema_uri
    )
VALUES (
        "fails",
        2,
        1,
        2,
        'https://alt/source/schema#anchor'
    );

-- Derived transitive dependencies of collections.
SELECT *
FROM collection_transitive_dependencies;

-- Transform details is a view which joins transforms with related resources
-- and emits a flattened representation with assumed default values.
SELECT *
FROM transform_details;

-- View of collection schemas which unions collection schemas with
-- any alternate schemas used by transforms reading the collection.
SELECT *
FROM collection_schemas;

-- Valid packages.
INSERT INTO nodejs_dependencies (package, version)
VALUES ('a-package', '^1.2.3'),
    -- Different packages and versions: OK.
    ('other-package', '^4.5.6'),
    ('yet-another-package', '=1.2'),
    -- Repeat of package at same version: silently ignored.
    ('a-package', '^1.2.3');

-- Invalid indexed package at a different version.
INSERT INTO nodejs_dependencies (package, version)
VALUES ('a-package', '^4.5.6');

-- Valid inferences
INSERT INTO inferences (
        schema_uri,
        location_ptr,
        types_json,
        must_exist,
        title,
        description,
        string_content_type,
        string_content_encoding_is_base64,
        string_max_length
    )
VALUES (
        'file:///path/to/a/schema.yaml#anchor',
        '/key/0',
        '["string"]',
        TRUE,
        'the title of /key/0',
        'the description of /key/0',
        'text/plain',
        FALSE,
        96
    ),
    (
        'file:///path/to/a/schema.yaml#anchor',
        '/key/1',
        '["string", "null"]',
        TRUE,
        'the title of /key/1',
        'the description of /key/1',
        'text/plain',
        TRUE,
        97
    ),
    (
        'file:///path/to/a/schema.yaml#anchor',
        '/path/3',
        '["string", "null"]',
        TRUE,
        'the title of /path/3',
        'the description of /path/3',
        'text/plain',
        FALSE,
        98
    );

INSERT INTO inferences (schema_uri, location_ptr, types_json, must_exist)
VALUES -- Will show up in collection_keys as error due to invalid type "number"
    (
        'https://canonical/schema/uri#/$defs/path',
        '/foo',
        '["number"]',
        TRUE
    ),
    -- Will show up in collection_keys as error due to must_exist being FALSE
    (
        'file:///path/to/a/schema.yaml',
        '/key',
        '["integer"]',
        FALSE
    ),
    -- Will show up in collection_keys as error due to being an impossible type
    (
        'file:///path/to/a/schema.yaml#anchor',
        '/key',
        '[]',
        TRUE
    );

-- Invalid inference (types are not valid json)
INSERT INTO inferences (schema_uri, location_ptr, types_json, must_exist)
VALUES (
        'file:///path/to/a/schema.yaml',
        '/field_2',
        '} not json {',
        FALSE
    );

-- Invalid inference (types are null)
INSERT INTO inferences (schema_uri, location_ptr, types_json, must_exist)
VALUES (
        'file:///path/to/a/schema.yaml',
        '/field_2',
        NULL,
        FALSE
    );

-- Invalid inference (types are not an array)
INSERT INTO inferences (schema_uri, location_ptr, types_json, must_exist)
VALUES (
        'file:///path/to/a/schema.yaml',
        '/field_2',
        '{"not": "an array"}',
        TRUE
    );

SELECT *
FROM projected_fields;

SELECT *
FROM projected_fields_json;

SELECT *
FROM collections_json;

SELECT *
FROM collection_keys;

SELECT *
FROM shuffle_key_types_detail;

-- Detail view of collections joined with projections, partitions,
-- and alternate source schemas.
SELECT *
FROM collection_details;

-- Valid test cases.
INSERT INTO test_cases(test_case_id, test_case_name, resource_id)
VALUES (420, "my test", 1),
    (860, "another test", 2);

INSERT INTO test_step_ingests (
        test_case_id,
        step_index,
        collection_id,
        documents_json
    )
VALUES (420, 1, 3, '[true, false]'),
    (860, 0, 2, '["a", "b"]');

INSERT INTO test_step_verifies (
        test_case_id,
        step_index,
        collection_id,
        selector_id,
        documents_json
    )
VALUES (420, 0, 3, NULL, '[111, 222]'),
    (860, 1, 1, 231, '[333, 444]');

-- Invalid (documents not a JSON array).
INSERT INTO test_step_ingests (
        test_case_id,
        step_index,
        collection_id,
        documents_json
    )
VALUES (860, 2, 1, '"zzz"');

-- Invalid (documents not a JSON array).
INSERT INTO test_step_verifies (
        test_case_id,
        step_index,
        collection_id,
        documents_json
    )
VALUES (860, 2, 1, '"zzz"');

-- Invalid (collection doesn't match that of the selector).
INSERT INTO test_step_verifies (
        test_case_id,
        step_index,
        collection_id,
        selector_id,
        documents_json
    )
VALUES (860, 2, 2, 231, '[]');

-- test_steps_json is a view which unifies test step variant tables
-- into a unified JSON structure.
SELECT *
FROM test_steps_json;

-- test_cases_json is a view which presents test cases as a nested
-- JSON structure.
SELECT *
FROM test_cases_json;