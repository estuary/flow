
drop table drafts;
drop table discovers;
drop table connector_tags;
drop table connectors;
drop function view_logs;
drop table internal.log_lines;
drop table internal._model_async;
drop table internal._model;
alter domain flowid drop default; -- remove dependence on id_generator.
drop function internal.id_generator;
drop sequence internal.shard_0_id_sequence;
drop schema internal;
drop domain catalog_prefix;
drop domain catalog_name;
drop domain flowid;
drop domain jsonb_obj;
drop domain json_obj;
drop function auth_uid;