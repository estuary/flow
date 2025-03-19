-- Add azure application client ID column to data_planes

begin;

ALTER TABLE public.data_planes ADD azure_application_client_id TEXT;

GRANT SELECT(azure_application_client_id) ON public.data_planes TO authenticated;

commit;
