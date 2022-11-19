-- This migration applies the table and trigger from 15_directives.sql that enforce a global limit to the number of tenants that may be created.

-- This index is needed for the `current_count` query in `on_applied_directives_insert`
create index idx_applied_directives_directive_id on applied_directives (directive_id);

create table internal.applied_directive_limits (
  directive_id flowid references directives(id) not null primary key,
  max_count bigint not null default 9223372036854775807
);

comment on table internal.applied_directive_limits is '
Sets global limits on the number of applied_directives that can be inserted for each directive.
Not all directives need to be present in this table. Those that are not will simply
not have any limit on the number of times it can be applied.
';
comment on column internal.applied_directive_limits.directive_id is
  'The id of the directive that this limit applies to';
comment on column internal.applied_directive_limits.max_count is
  'Maximum number of times that the directive may appear in the applied_directives table.';

create function internal.on_applied_directives_insert()
returns trigger as $$
declare
  current_count bigint := (select count(*) from applied_directives where directive_id = NEW.directive_id);
  max_count bigint := (select max_count from internal.applied_directive_limits where directive_id = NEW.directive_id);
begin
  if current_count >= max_count then
    raise exception 'System quota has been reached, please contact support@estuary.dev in order to proceed';
  end if;

  return NEW;
end
$$ language 'plpgsql';

create trigger "Verify insert of applied directives"
  before insert on applied_directives
  for each row
  execute function internal.on_applied_directives_insert();


