-- Migration to add an index to live_specs to support
-- looking up materializations by their `sourceCapture`.

create index idx_live_specs_materializations_by_source_capture on live_specs ((spec->>'sourceCapture'));
