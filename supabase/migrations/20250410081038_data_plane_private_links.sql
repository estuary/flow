-- Add private_links as a separate column to data_planes

begin;

ALTER TABLE public.data_planes ADD private_links json[] not null default array[]::json[];

GRANT SELECT(private_links) ON public.data_planes TO authenticated;

commit;
