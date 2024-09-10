begin;

grant insert (draft_id, dry_run, detail, data_plane_name) on publications to authenticated;

grant insert (capture_name, connector_tag_id, data_plane_name, draft_id, endpoint_config, update_only)
  on discovers to authenticated;

commit;