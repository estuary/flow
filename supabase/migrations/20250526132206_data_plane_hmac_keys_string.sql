-- Add encrypted_hmac_keys as a text column that holds an encrypted sops document

begin;

ALTER TABLE public.data_planes ADD encrypted_hmac_keys text not null default '';

commit;
