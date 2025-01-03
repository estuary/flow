-- Allows new column to be read by users so the UI can render
-- this in the Admin section of the dashboard

begin;

GRANT SELECT(aws_link_endpoints) ON TABLE public.data_planes TO authenticated;

commit;
