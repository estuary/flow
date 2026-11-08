begin;

-- A discover having no connector tag takes its endpoint from the capture
-- drafted into `draft_id`, or else from the live capture. Writers of such a
-- discover leave `endpoint_config` as an empty placeholder object.
alter table public.discovers alter column connector_tag_id drop not null;

-- Only the control-plane API's `createDiscover` may queue a NULL connector tag.
-- PostgREST inserts run as `authenticated`, and keep their legacy contract.
alter table public.discovers add constraint connector_tag_id_required_from_postgrest
  check (connector_tag_id is not null or current_user != 'authenticated');

comment on column public.discovers.connector_tag_id is
'Tagged connector which is used for discovery, together with `endpoint_config`.
If NULL, discovery uses the endpoint of the capture drafted in `draft_id`,
or else of the live capture.';

comment on column public.discovers.endpoint_config is
'Endpoint configuration of the connector. May be protected by sops.
An empty, ignored object if `connector_tag_id` is NULL.';

commit;
