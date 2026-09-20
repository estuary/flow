-- A stand-in for what config-encryption's `/secret/encrypt` returns. Only the
-- fields the control plane reads are real; `value` is ciphertext that nothing
-- in this crate can decrypt.
--
-- `id` is pinned rather than generated, so that tests can assert which secret
-- a route disclosed.
insert into internal.secrets (catalog_name, id, document) values (
  'aliceCo/in/token',
  '1111111111111111',
  '{"name":"aliceCo/in/token","value":"ENC[AES256_GCM,data:c3Rvcms=,type:str]","sops":{"lastmodified":"2026-01-15T01:29:57Z","mac":"ENC[AES256_GCM,data:bWFj]","encrypted_regex":"^value$","version":"3.11.0"}}'
), (
  'aliceCo/out/token',
  '2222222222222222',
  '{"name":"aliceCo/out/token","value":"ENC[AES256_GCM,data:cGVsaWNhbg==,type:str]","sops":{"lastmodified":"2026-01-15T01:29:57Z","mac":"ENC[AES256_GCM,data:bWFj]","encrypted_regex":"^value$","version":"3.11.0"}}'
), (
  'aliceCo/private/token',
  '3333333333333333',
  '{"name":"aliceCo/private/token","value":"ENC[AES256_GCM,data:aGVyb24=,type:str]","sops":{"lastmodified":"2026-01-15T01:29:57Z","mac":"ENC[AES256_GCM,data:bWFj]","encrypted_regex":"^value$","version":"3.11.0"}}'
), (
  -- A vendor's secret, named for the image repository which uses it. No task
  -- is a sibling of this one: only the image rule can reach it.
  'acmeVendor/oauth/connectors/ghcr.io/acmeVendor/source-widgets/oauth-client',
  '4444444444444444',
  '{"name":"acmeVendor/oauth/connectors/ghcr.io/acmeVendor/source-widgets/oauth-client","value":"ENC[AES256_GCM,data:b3NwcmV5,type:str]","sops":{"lastmodified":"2026-01-15T01:29:57Z","mac":"ENC[AES256_GCM,data:bWFj]","encrypted_regex":"^value$","version":"3.11.0"}}'
);
