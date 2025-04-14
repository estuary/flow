BEGIN;

CREATE TABLE public.data_plane_migrations (
    id                      public.flowid PRIMARY KEY NOT NULL DEFAULT internal.id_generator(),
    created_at              TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
    active                  BOOLEAN NOT NULL DEFAULT TRUE,
    catalog_name_or_prefix  TEXT NOT NULL,
    src_plane_id            public.flowid NOT NULL,
    tgt_plane_id            public.flowid NOT NULL,
    cordon_at               TIMESTAMPTZ DEFAULT NOW() + INTERVAL '90 minutes' NOT NULL
);

END;