begin;

--TODO: define RLS policies for publications_v2
create table publications_v2(like publications including all);

commit;
