-- This migration applies the table and trigger from 15_directives.sql that enforce a global limit to the number of tenants that may be created.

create table internal.applied_directive_tickets (
  directive_id flowid references directives(id) not null primary key,
  remaining bigint not null default 9223372036854775807
);

comment on table internal.applied_directive_tickets is '
Sets global limits on the number of applied_directives that can be inserted
for each directive. Not all directives need to be present in this table. Those
that are not will simply not have any limit on the number of times it can be
applied. Directives that are present here will have a the remaining column
decremented each time it is applied. Once the remaining column drops to 0, no
more applied_directives may be inserted for that directive_id.
';
comment on column internal.applied_directive_tickets.directive_id is
  'The id of the directive that the tickes apply to';
comment on column internal.applied_directive_tickets.remaining is
  'How many tickets are remaining for this directive';

create function internal.on_applied_directives_insert()
returns trigger as $$
declare
  canDo bigint;
begin
  update internal.applied_directive_tickets
    set remaining = remaining - 1
    where directive_id = NEW.directive_id
    returning remaining into canDo;
  -- if canDo is null, then no rows were matched and we shouldn't enforce a limit.
  -- if canDo is 0, then it means that it was just now decremented to 0, so the insert should be allowed.
  if canDo < 0 then
      raise exception 'System quota has been reached, please contact support@estuary.dev in order to proceed';
  end if;

  return NEW;
end
$$ language 'plpgsql';

create trigger "Verify insert of applied directives"
  before insert on applied_directives
  for each row
  execute function internal.on_applied_directives_insert();

