-- Add bastion-related info columns that should be exposed to customers

begin;

-- Address for connecting to the bastion
ALTER TABLE public.data_planes ADD bastion_address TEXT;
-- Private key to be used for connecting to bastion
ALTER TABLE public.data_planes ADD bastion_private_key TEXT;

commit;
