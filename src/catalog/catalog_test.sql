.load /usr/lib/sqlite3/pcre.so
.read "catalog.sql"
.changes on
.headers on
.echo on

-- Valid resources.
INSERT INTO resources (content_type, content)
VALUES ('application/vnd.estuary.dev-catalog-spec+yaml', 'catalog spec content'),
       ('application/vnd.estuary.dev-catalog-fixtures+yaml', 'catalog fixtures content'),
       ('application/schema+yaml', 'json-schema content'),
       ('text/javascript', 'javascript content'),
       ('text/x.typescript', 'typescript content'),
       ('application/sql', 'bootstrap content'),
       ('application/sql', 'transform content');

-- Invalid resource (unknown content type).
INSERT INTO resources (content_type, content)
VALUES ('application/unknown', 'unknown content');

-- Valid imports, which form a directed acyclic graph.
INSERT INTO resource_imports (resource_id, import_id)
VALUES
    -- 1 => 2 => 3 => 4.
    (1, 2),
    (3, 4),
    (2, 3),
    -- Disconnected 5 => 6.
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
VALUES (2, 3), -- Repeat.
       (1, 3), -- Alternate 1 => 3 => 4.
       (2, 4), -- Alternate 1 => 2 => 4.
       (5, 6)  -- Repeat.
ON CONFLICT DO NOTHING;

SELECT DISTINCT *
FROM resource_transitive_imports;

-- Valid resource URLs.
INSERT INTO resource_urls (resource_id, url, is_primary)
VALUES (1, 'file:///path/to/spec.yaml', TRUE),
       (2, 'file:///path/to/some/fixtures.yaml', TRUE),
       (3, 'file:///path/to/a/schema.yaml', TRUE),
       (4, 'https://host/path/javascript?query=val', TRUE),
       (5, 'file:///path/to/some/typescript.yaml', TRUE),
       (6, 'file:///path/to/some/bootstrap.sql', TRUE),
       (7, 'file:///path/to/some/transform.sql', TRUE),
       -- Alternate resource URLs.
       -- Each resource may have multiple alternate URLs.
       (3, 'https://canonical/schema/uri', NULL),
       (4, 'https://redirect-1/javascript', NULL),
       (4, 'https://redirect-2/javascript', NULL),
       (4, 'https://redirect-3/javascript', NULL);

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

-- Valid lambdas.
INSERT INTO lambdas (runtime, inline, resource_id)
VALUES ('nodeJS', '(state) => { console.log(''hello''); }', NULL),
       ('nodeJS', '(doc, state) => {...doc, key: 1}', NULL),
       ('sqlite', 'SELECT 1;', NULL),
       ('sqliteFile', NULL, 6), -- Bootstrap.
       ('sqliteFile', NULL, 7), -- Transform.
       ('remote', 'https://remote/endpoint', NULL),
       ('nodeJS', '(doc, state) => {...doc, foo: true}', NULL);

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

-- Expect we can natural join lambdas => resources
SELECT *
FROM lambdas
         NATURAL LEFT JOIN resources;

-- Valid collections.
INSERT INTO collections (name, schema_uri, key_json, resource_id)
VALUES ('col/src', 'file:///path/to/a/schema.yaml#anchor', '["/key/0","/key/1"]', 1),
       ('col/derived', 'https://canonical/schema/uri#/$defs/path', '["/foo"]', 1),
       ('col/der.iv-e+d', 'https://canonical/schema/uri#/$defs/path', '["/foo"]', 1);

-- Invalid collection (schema is not a URI).
INSERT INTO collections (name, schema_uri, key_json, resource_id)
VALUES ('col/bad', 'not-a-uri', '["/key"]', 1);

-- Invalid collection (collection name doesn't match pattern).
INSERT INTO collections (name, schema_uri, key_json, resource_id)
VALUES ('spaces not allowed', 'file:///path/to/a/schema.yaml', '["/key"]', 1);
INSERT INTO collections (name, schema_uri, key_json, resource_id)
VALUES ('bad!', 'file:///path/to/a/schema.yaml', '["/key"]', 1);

-- Invalid collection (key is not non-empty [JSON-Pointer]).
INSERT INTO collections (name, schema_uri, key_json, resource_id)
VALUES ('col/bad', 'file:///path/to/a/schema.yaml', '["malformed"', 1);
INSERT INTO collections (name, schema_uri, key_json, resource_id)
VALUES ('col/bad', 'file:///path/to/a/schema.yaml', '{"not":"array"}', 1);
INSERT INTO collections (name, schema_uri, key_json, resource_id)
VALUES ('col/bad', 'file:///path/to/a/schema.yaml', '[]', 1);

-- Invalid collection (resource doesn't exist).
INSERT INTO collections (name, schema_uri, key_json, resource_id)
VALUES ('col/bad', 'file:///path/to/a/schema.yaml', '["/key"]', 42);

SELECT *
FROM collections;

-- Valid projections.
INSERT INTO projections (collection_id, field, location_ptr, is_logical_partition)
VALUES (1, 'field_1', '/path/1', false),
       (1, 'field_2', '/path/2', true),
       (2, 'field_1', '', false), -- Repeat field name with different collection.
       (2, 'field_a', '/a', true);

-- Invalid projection (bad field name).
INSERT INTO projections (collection_id, field, location_ptr, is_logical_partition)
VALUES (1, 'no spaces', '/path/1', false);
INSERT INTO projections (collection_id, field, location_ptr, is_logical_partition)
VALUES (1, 'or-hyphens', '/path/1', false);

-- Invalid projection (no such collection).
INSERT INTO projections (collection_id, field, location_ptr, is_logical_partition)
VALUES (42, 'foo', '/bar', false);

-- Invalid projection (invalid JSON-Pointer).
INSERT INTO projections (collection_id, field, location_ptr, is_logical_partition)
VALUES (1, 'foo', 'bar', false);
INSERT INTO projections (collection_id, field, location_ptr, is_logical_partition)
VALUES (1, 'foo', '/bar/', false);


-- Valid fixtures.
INSERT INTO fixtures (collection_id, resource_id)
VALUES (1, 2),
       (2, 2);

-- Invalid fixture (no such collection);
INSERT INTO fixtures (collection_id, resource_id)
VALUES (42, 2);

-- Invalid fixture (no such resource);
INSERT INTO fixtures (collection_id, resource_id)
VALUES (2, 42);


-- Valid derivation, with NULL parallelism.
INSERT INTO derivations (collection_id)
VALUES (2),
       (3);

-- Invalid derivation (parallelism <= 0)
UPDATE derivations
SET parallelism = 0;
UPDATE derivations
SET parallelism = 16;
-- OK.

-- Invalid derivation (collection must exist).
INSERT INTO derivations (collection_id)
VALUES (42);


-- Valid bootstrap.
INSERT INTO bootstraps (derivation_id, lambda_id)
VALUES (2, 1),
       (2, 4),
       (3, 1);

-- Invalid bootstrap (derivation must exist).
INSERT INTO bootstraps (derivation_id, lambda_id)
VALUES (42, 1);
-- Invalid bootstrap (collection_id = 1 is not a derivation).
INSERT INTO bootstraps (derivation_id, lambda_id)
VALUES (1, 1);
-- Invalid bootstrap (lambda must exist).
INSERT INTO bootstraps (derivation_id, lambda_id)
VALUES (2, 42);

-- Expect we can natural join from bootstraps => resources.
SELECT *
FROM bootstraps
         NATURAL LEFT JOIN lambdas
         NATURAL LEFT JOIN resources;

-- Valid transforms.
INSERT INTO transforms (derivation_id, source_collection_id, lambda_id)
VALUES (2, 1, 2),
       (2, 1, 3),
       (2, 1, 5),
       (2, 1, 6),
       (3, 1, 7);

-- Invalid source-schema
UPDATE transforms
SET source_schema_uri = 'not-a-url'
WHERE transform_id = 1;
-- Valid source-schema.
UPDATE transforms
SET source_schema_uri = 'https://source/schema#anchor'
WHERE transform_id = 1;

-- Can only set one of 'broadcast' or 'choose'.
UPDATE transforms
SET shuffle_broadcast = 2,
    shuffle_choose    = 3
WHERE transform_id = 1;
-- They must be positive.
UPDATE transforms
SET shuffle_broadcast = 0
WHERE transform_id = 1;
UPDATE transforms
SET shuffle_choose = 0
WHERE transform_id = 1;
-- OK.
UPDATE transforms
SET shuffle_broadcast = 3
WHERE transform_id = 1;
UPDATE transforms
SET shuffle_choose = 3
WHERE transform_id = 2;

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

-- Derivation must exist (cannot use a collection).
UPDATE transforms
SET derivation_id = 42;
UPDATE transforms
SET derivation_id = 1;
-- Source collection must exist.
UPDATE transforms
SET source_collection_id = 42;
-- Lambda must exist.
UPDATE transforms
SET lambda_id = 42;

-- Expect we can natural join from transforms => resources.
SELECT *
FROM transforms
         NATURAL LEFT JOIN lambdas
         NATURAL LEFT JOIN resources;

-- Valid packages.
INSERT INTO nodejs_dependencies (package, semver)
VALUES ('a-package', '^1.2.3'),
       -- Different packages and versions: OK.
       ('other-package', '^4.5.6'),
       ('yet-another-package', '=1.2'),
       -- Repeat of package at same version: silently ignored.
       ('a-package', '^1.2.3');

-- Invalid indexed package at a different version.
INSERT INTO nodejs_dependencies (package, semver)
VALUES ('a-package', '^4.5.6');

-- View of NodeJS bootstrap invocations, by derivation.
SELECT *
FROM nodejs_expressions;
