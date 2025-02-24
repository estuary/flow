-- Add bastion-related info columns that should be exposed to customers

begin;

-- Private key to be used for connecting to bastion
ALTER TABLE public.data_planes ADD bastion_private_key TEXT;

commit;
