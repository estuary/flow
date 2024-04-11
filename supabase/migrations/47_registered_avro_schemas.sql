
create table registered_avro_schemas (
  like internal._model including all,

  avro_schema      json not null,
  avro_schema_md5  text generated always as (md5(trim(avro_schema::text))) stored,
  catalog_name     catalog_name not null,
  registry_id      serial unique not null
);

create index idx_registered_avro_schemas_avro_schema_md5 on registered_avro_schemas (avro_schema_md5);

comment on table registered_avro_schemas is '
Avro schemas registered with a globally unique, stable registery ID.

This is used to emulate the behavior of Confluent Schema Registry when
transcoding collection documents into Avro for use with Dekaf,
which must encode each message with an Avro schema ID (registry_id).
';

alter table registered_avro_schemas enable row level security;

create policy "Users must be read-authorized to the schema catalog name"
  on registered_avro_schemas as permissive
  using (exists(
    select 1 from auth_roles('read') r where catalog_name ^@ r.role_prefix
  ));

grant select on registered_avro_schemas to authenticated;
grant insert (catalog_name, avro_schema) on registered_avro_schemas to authenticated;
grant update (updated_at) on registered_avro_schemas to authenticated;
grant usage on sequence registered_avro_schemas_registry_id_seq to authenticated;