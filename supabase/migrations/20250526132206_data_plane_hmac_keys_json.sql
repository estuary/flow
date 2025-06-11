-- Add encrypted_hmac_keys as a json column that holds an encrypted sops document

begin;

ALTER TABLE public.data_planes ADD encrypted_hmac_keys json not null default '{}'::json;

commit;
