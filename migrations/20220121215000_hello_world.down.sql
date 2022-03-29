
DROP TABLE builds;
DROP TABLE discovers;
DROP FUNCTION view_logs;
DROP TABLE logs;
DROP FUNCTION auth_session;
DROP TABLE credentials;
DROP TABLE accounts;
DROP TABLE connector_images;
DROP TABLE connectors;
DROP TABLE _model;
ALTER DOMAIN flowid DROP DEFAULT; -- Remove dependence on id_generator.
DROP FUNCTION id_generator;
DROP SEQUENCE shard_0_id_sequence;
DROP FUNCTION auth_account_id;
DROP FUNCTION auth_id;
DROP FUNCTION auth_access;
DROP TYPE jwt_id_token;
DROP TYPE jwt_access_token;
DROP DOMAIN jsonb_obj;
DROP DOMAIN json_obj;
DROP DOMAIN catalog_prefix;
DROP DOMAIN catalog_name;
DROP DOMAIN flowid;