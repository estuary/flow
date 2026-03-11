-- The first portion of this file was copied directly from:
-- crates/agent/src/integration_tests/harness.rs TestHarness::init
-- This fixture has a strict superset of those connectors, to make transitioning
-- existing tests easier.
with source_image as (
    insert into connectors (external_url, image_name, title, short_description, logo_url, recommended)
    values ('http://test.test/', 'source/test', '{"en-US": "test"}', '{"en-US": "test"}', '{"en-US": "http://test.test/"}', false)
    returning id
),
materialize_image as (
    insert into connectors (external_url, image_name, title, short_description, logo_url, recommended)
    values ('http://test.test/', 'materialize/test', '{"en-US": "test"}', '{"en-US": "test"}', '{"en-US": "http://test.test/"}', false)
    returning id
),
source_tag as (
    insert into connector_tags (
        connector_id,
        image_tag,
        protocol,
        documentation_url,
        endpoint_spec_schema,
        resource_spec_schema,
        resource_path_pointers,
        job_status
    ) values (
        (select id from source_image),
        ':test',
        'capture',
        'http://test.test/',
        '{"type": "object"}',
        '{"type": "object", "properties": {"id": {"type": "string", "x-collection-name": true}}}',
        '{/id}',
        '{"type": "success"}'
    )
),
materialize_tag as (
    insert into connector_tags (
        connector_id,
        image_tag,
        protocol,
        documentation_url,
        endpoint_spec_schema,
        resource_spec_schema,
        resource_path_pointers,
        job_status
    ) values (
        (select id from materialize_image),
        ':test',
        'materialization',
        'http://test.test/',
        '{"type": "object"}',
        '{"type": "object", "properties": {"id": {"type": "string", "x-collection-name": true}, "schema": {"type": "string", "x-schema-name": true}, "delta": {"type": "boolean", "x-delta-updates": true}}}',
        '{/id}',
        '{"type": "success"}'
    )
),
materialize_tag_no_annotations as (
    insert into connector_tags (
        connector_id,
        image_tag,
        protocol,
        documentation_url,
        endpoint_spec_schema,
        resource_spec_schema,
        resource_path_pointers,
        job_status
    ) values (
        (select id from materialize_image),
        ':test-no-annotation',
        'materialization',
        'http://test.test/',
        '{"type": "object"}',
        '{"type": "object", "properties": {"id": {"type": "string", "x-collection-name": true}, "schema": {"type": "string"}, "delta": {"type": "boolean"}}}',
        '{/id}',
        '{"type": "success"}'
    )
),
no_tags_source as (
    insert into connectors (external_url, image_name, title, short_description, logo_url, recommended)
    values ('http://test.test/', 'source/no-tags-test', '{"en-US": "test"}', '{"en-US": "test"}', '{"en-US": "http://test.test/"}', false)
    returning id
),
no_tags_dest as (
    insert into connectors (external_url, image_name, title, short_description, logo_url, recommended)
    values ('http://test.test/', 'materialize/no-tags-test', '{"en-US": "test"}', '{"en-US": "test"}', '{"en-US": "http://test.test/"}', false)
    returning id
),
multi_tag_source_image as (
   insert into connectors (external_url, image_name, title, short_description, logo_url, recommended)
   values ('http://test.test/', 'source/multi-tag-test', '{"en-US": "test"}', '{"en-US": "test"}', '{"en-US": "http://test.test/"}', false)
   returning id
),
multi_tag_materialize_image as (
   insert into connectors (external_url, image_name, title, short_description, logo_url, recommended)
   values ('http://test.test/', 'materialize/multi-tag-test', '{"en-US": "test"}', '{"en-US": "test"}', '{"en-US": "http://test.test/"}', false)
   returning id
),
multi_tag_source_tag as (
   insert into connector_tags (
       connector_id,
       image_tag,
       protocol,
       documentation_url,
       endpoint_spec_schema,
       resource_spec_schema,
       resource_path_pointers,
       job_status
   ) values (
        (select id from multi_tag_source_image),
        ':dev',
        'capture',
        'http://test.test/',
        '{"type": "object"}',
        '{"type": "object", "properties": {"id": {"type": "string", "x-collection-name": true}}}',
        '{/id}',
        '{"type": "success"}'
    ), (
        (select id from multi_tag_source_image),
        ':v1',
        'capture',
        'http://test.test/',
        '{"type": "object"}',
        '{"type": "object", "properties": {"id": {"type": "string", "x-collection-name": true}}}',
        '{/id}',
        '{"type": "success"}'
    ), (
       (select id from multi_tag_source_image),
       ':v2',
       null,
       'http://test.test/',
       null,
       null,
       null,
       '{"type": "specFailed"}'
   )
),
multi_tag_materialize_tag as (
   insert into connector_tags (
       connector_id,
       image_tag,
       protocol,
       documentation_url,
       endpoint_spec_schema,
       resource_spec_schema,
       resource_path_pointers,
       job_status
   ) values (
        (select id from multi_tag_materialize_image),
        ':dev',
        'materialization',
        'http://test.test/',
        '{"type": "object"}',
        '{"type": "object", "properties": {"id": {"type": "string", "x-collection-name": true}, "schema": {"type": "string", "x-schema-name": true}, "delta": {"type": "boolean", "x-delta-updates": true}}}',
        '{/id}',
        '{"type": "specFailed"}'
    ), (
        (select id from multi_tag_materialize_image),
        ':v2',
        'materialization',
        'http://test.test/',
        null,
        null,
        null,
        '{"type": "specFailed"}'
    ), (
       (select id from multi_tag_materialize_image),
       ':v3',
       'materialization',
       'http://test.test/',
       '{"type": "object"}',
       '{"type": "object", "properties": {"id": {"type": "string", "x-collection-name": true}, "schema": {"type": "string", "x-schema-name": true}, "delta": {"type": "boolean", "x-delta-updates": true}}}',
       '{/id}',
       '{"type": "success"}'
   )
)
select 1 as "something";
