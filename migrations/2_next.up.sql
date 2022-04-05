
-- This isn't part of an actual V2 schema and will be removed.
-- It's here to demonstrate a migration test.
create table connectors_copy (
    image_name text unique not null
);
insert into connectors_copy (image_name)
    select 'copy/' || image_name as image_name from connectors;