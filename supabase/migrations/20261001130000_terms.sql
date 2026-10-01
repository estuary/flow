begin;

create table internal.terms (
    id public.flowid primary key not null default internal.id_generator(),
    text text not null,
    version integer not null unique check (version > 0),
    created_at timestamptz not null default now()
);

comment on table internal.terms is
    'Terms text associated with each version accepted during tenant creation.';

commit;
