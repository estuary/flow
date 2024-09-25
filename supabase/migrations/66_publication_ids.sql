begin;

alter table publications add column pub_id flowid;

comment on column publications.pub_id is
'The effective publication id that was used by the publications handler
to commit a successful publication. This will be null if the publication
did not commit. If non-null, then this is the publication id that would
exist in the publication_specs table, and would be used as the last_pub_id
for any drafted specs';

commit;
