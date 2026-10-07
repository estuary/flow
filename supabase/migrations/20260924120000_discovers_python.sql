begin;

-- A discover of a user-authored (python) capture has no connector tag: its
-- image is a platform built-in, and its code is the capture's `files`.
-- Such a discover's `endpoint_config` is the user's `endpoint.python.config`,
-- and the control plane resolves the rest of the endpoint (its `files`) from
-- the capture drafted into `draft_id`, or else from the live capture.
alter table public.discovers alter column connector_tag_id drop not null;

comment on column public.discovers.connector_tag_id is
'Tagged connector which is used for discovery.
NULL for a discover of a python capture, which must be drafted in `draft_id` or be live.';

comment on column public.discovers.endpoint_config is
'Endpoint configuration of the connector. May be protected by sops.
For a python capture (NULL `connector_tag_id`), this is the plaintext `endpoint.python.config`.';

commit;
