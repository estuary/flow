begin;

create table internal.migrations (
    catalog_prefix  text not null,
    cordon_at       timestamptz not null,
    src_plane_id    public.flowid not null,
    tgt_plane_id    public.flowid not null
);

end;