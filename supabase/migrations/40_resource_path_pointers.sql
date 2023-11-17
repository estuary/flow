
begin;

create domain json_pointer as text check(value = '' or (value ^@ '/' and length(value) > 1));


alter table connector_tags
	add column resource_path_pointers json_pointer[]
	check(array_length(resource_path_pointers, 1) > 0);

comment on column connector_tags.resource_path_pointers is
'The resource_path that was returned from the connector spec response';

commit;

