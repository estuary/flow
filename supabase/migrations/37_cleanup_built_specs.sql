-- There's been a bug that resulted in the `built_spec` being populated for deleted collections.
-- The agent code has been updated to fix the issue, and this just cleans up any affected rows.
begin;

update live_specs set built_spec = null where spec is null and spec_type is null;

commit;
