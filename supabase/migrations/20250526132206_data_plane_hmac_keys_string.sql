-- Make data_plane_hmac_keys into a string column that holds an encrypted sops document

begin;

ALTER TABLE public.data_planes ALTER COLUMN hmac_keys SET DATA TYPE text;

commit;
