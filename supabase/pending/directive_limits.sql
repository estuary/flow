-- Updates the directives table to have an optional limit on the number of times
-- that it can be applied.

alter table directives add column uses_remaining bigint;

comment on column directives.uses_remaining is '
The maximum number of times that this directive may be applied.
This value gets decremented each time the directive is applied.
Once it reaches 0, future attempts to apply the directive will fail.
A null here means that there is no limit.
';

update directives set uses_remaining = 1 where single_use = true;

alter table directives drop column single_use;

create or replace function exchange_directive_token(bearer_token uuid)
returns exchanged_directive as $$
declare
  directive_row directives;
  applied_row applied_directives;
begin

  -- Note that uses_remaining could be null, and in that case `uses_remaining - 1`
  -- would also evaluate to null. This means that we don't actually update
  -- uses_remaining here if the current value is null.
  -- We also intentionally leave the bearer_token in place when uses_remaining
  -- drops to 0, because it's possible that something may come along and
  -- increase uses_remaining again.
  update directives
    set uses_remaining = uses_remaining - 1
    where directives.token = bearer_token
    returning * into directive_row;

  if not found then
    raise 'Bearer token % is not valid', bearer_token
      using errcode = 'check_violation';
  end if;

  if directive_row.uses_remaining is not null and directive_row.uses_remaining < 0 then
    raise 'System quota has been reached, please contact support@estuary.dev in order to proceed.'
      using errcode = 'check_violation';
  end if;

  insert into applied_directives (directive_id, user_id)
  values (directive_row.id, auth.uid())
  returning * into applied_row;

  return (directive_row, applied_row);
end;
$$ language plpgsql security definer;

