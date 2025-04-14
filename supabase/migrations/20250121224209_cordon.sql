begin;

create table internal.migrations (
    task_id          public.flowid primary key not null default internal.id_generator(),

    -- populated by operator
    catalog_name_or_prefix   text not null,
    src_plane_name           public.catalog_name not null,
    tgt_plane_name           public.catalog_name not null,

    -- updated by controller
    cordon_at        timestamptz,
    status           json default '{}'::json not null
);

end;