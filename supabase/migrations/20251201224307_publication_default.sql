begin;

alter table public.publications alter column data_plane_name drop default;
alter table public.publications alter column data_plane_name drop not null;

commit;
