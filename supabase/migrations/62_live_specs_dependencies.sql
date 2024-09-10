begin;

alter table live_specs add column dependency_hash text;

comment on column live_specs.dependency_hash is
'An hash of all the dependencies which were used to build this spec.
Any change to the _model_ of a dependency will change this hash.
Changes to the built spec of a dependency without an accompanying
model change will not change the hash.';

commit;
