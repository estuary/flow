
-- Adds "on delete cascade" to the foreign key constraint on publication_specs to live_specs.
-- We previously never deleted live_specs, but now we do as part of pruning unbound collections
-- from in-progress publications.
begin;

alter table publication_specs drop constraint publication_specs_live_spec_id_fkey;
alter table publication_specs add constraint publication_specs_live_spec_id_fkey
	foreign key (live_spec_id)
	references live_specs(id)
	on delete cascade;

commit;