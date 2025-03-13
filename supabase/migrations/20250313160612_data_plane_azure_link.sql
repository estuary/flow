-- Add azure private link endpoints column to data_planes

begin;

-- Private key to be used for connecting to bastion
ALTER TABLE public.data_planes ADD azure_link_endpoints JSON[];

GRANT SELECT(azure_link_endpoints) ON public.data_planes TO authenticated;

commit;
