BEGIN;

CREATE OR REPLACE VIEW live_specs_ext AS
WITH authorized_specs AS (
    SELECT l_1.id
    FROM auth_roles('read'::grant_capability) r(role_prefix, capability),
         live_specs l_1
    WHERE l_1.catalog_name::text ^@ r.role_prefix::text
)
SELECT
    l.created_at,
    l.detail,
    l.id,
    l.updated_at,
    l.catalog_name,
    l.connector_image_name,
    l.connector_image_tag,
    l.last_build_id,
    l.last_pub_id,
    l.reads_from,
    l.spec,
    l.spec_type,
    l.writes_to,
    l.built_spec,
    l.md5,
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
    d.broker_address,            -- Added column
    d.data_plane_name,           -- Added column
    d.reactor_address            -- Added column
FROM live_specs l
LEFT JOIN publication_specs p ON l.id::macaddr8 = p.live_spec_id::macaddr8 AND l.last_pub_id::macaddr8 = p.pub_id::macaddr8
LEFT JOIN connectors c ON c.image_name = l.connector_image_name
LEFT JOIN connector_tags t ON c.id::macaddr8 = t.connector_id::macaddr8 AND l.connector_image_tag = t.image_tag
LEFT JOIN internal.user_profiles u ON u.user_id = p.user_id
LEFT JOIN data_planes d ON d.id::macaddr8 = l.data_plane_id::macaddr8
WHERE (
    EXISTS (
        SELECT 1
        FROM pg_roles
        WHERE pg_roles.rolname = CURRENT_ROLE AND pg_roles.rolbypassrls = TRUE
    )
) OR (
    l.id::macaddr8 IN (
        SELECT authorized_specs.id
        FROM authorized_specs
    )
);

COMMIT;