

create table directives (
  like internal._model including all,

  catalog_prefix  catalog_prefix not null,
  single_use      boolean not null default false,
  spec            jsonb_obj not null,
  token           uuid unique default gen_random_uuid(),

  constraint "spec must have a string property `type`" check (
    jsonb_typeof(spec->'type') is not distinct from 'string')
);
alter table directives enable row level security;

create policy "Users can access and change directives which they administer"
  on directives as permissive
  using (auth_catalog(catalog_prefix, 'admin'));

grant all on directives to authenticated;

create index idx_directives_catalog_prefix on directives
  (catalog_prefix text_pattern_ops);


comment on table directives is '
Directives are scoped operations that users may elect to apply.
For example, a directive might grant access to a specific catalog namespace,
or provision the setup of a new organization.

In general these operations require administrative priviledge that the user
does not directly have. The directive mechanism thus enables a user to have a
priviledged operation be applied on their behalf in a self-service fashion.

The types of operations supported by directives are open ended,
but each generally has a well-defined (but parameterizable) scope,
and may also be subject to additional server-side verification checks.

To apply a given directive a user must know its current token, which is
a secret credential that''s typically exchanged through another channel
(such as Slack, or email). The user then creates a corresponding entry in
applied_directives with accompanying user claims.
';
comment on column directives.catalog_prefix is '
Catalog prefix which contains the directive.

Operations undertaken by a directive are scoped within the catalog prefix,
and a user must admin the named prefix in order to admin its directives.
';
comment on column directives.single_use is '
Single-use directives have their token disabled after its first use.
';
comment on column directives.spec is '
Specification of the directive.

Specification documents must have a string `type` property which defines
the directive type. This type defines the meaning of the remainder of the
specification document.
';
comment on column directives.token is '
Bearer token which is presented by a user to access and apply a directive.
';

create table applied_directives (
  like internal._model_async including all,

  directive_id  flowid references directives(id) not null,
  user_id       uuid   references auth.users(id) not null default auth.uid(),
  user_claims   json_obj
);
alter table applied_directives enable row level security;
alter publication supabase_realtime add table applied_directives;

create policy "Users can access only their applied directives"
  on applied_directives as permissive
  using (user_id = auth.uid());

grant select on applied_directives to authenticated;
grant update (user_claims) on applied_directives to authenticated;
grant delete on applied_directives to authenticated;

create policy "Users may select directives which they have applied"
  on directives as permissive for select
  using (id in (select directive_id from applied_directives));

comment on table applied_directives is '
Directives which are being or have been applied by the user.

Users begin to apply a directive by exchanging its bearer token, which creates
a new applied_directives row. Then, upon supplying user_claims which further
parameterize the operation, the directive is validated and applied with the
user''s claims.
';
comment on column applied_directives.directive_id is
  'Directive which is being applied';
comment on column applied_directives.user_id is
  'User on whose behalf the directive is being applied';
comment on column applied_directives.user_claims is '
User-supplied claims which parameterize the directive''s evaluation.

User claims are initially null when an applied directive is first created,
and must be updated by the user for evaluation of the directive to begin.
';

create function internal.on_applied_directives_update()
returns trigger as $$
begin
  if OLD.job_status->>'type' = 'success' then
    raise 'Cannot modify an applied directive which has completed'
      using errcode = 'check_violation';
  end if;

  -- Clear a prior failed application, allowing the user to retry.
  if OLD.user_claims::text is distinct from NEW.user_claims::text then
    NEW.job_status = '{"type":"queued"}';
  end if;

  return NEW;
end
$$ language 'plpgsql';

create trigger "Verify update of applied directives"
  before update on applied_directives
  for each row
  execute function internal.on_applied_directives_update();


create function internal.on_applied_directives_delete()
returns trigger as $$
begin
  if OLD.job_status->>'type' = 'success' then
    raise 'Cannot delete an applied directive which has completed'
      using errcode = 'check_violation';
  end if;

  return OLD;
end
$$ language 'plpgsql';

create trigger "Verify delete of applied directives"
  before delete on applied_directives
  for each row
  execute function internal.on_applied_directives_delete();


-- Users must present the current bearer token of a directive in order to
-- apply it, and cannot directly create rows in `applied_directives` even if
-- they know the directive_id, as we consider the flowid to be insecure.
create type exchanged_directive as (
  directive directives,
  applied_directive applied_directives
);

create function exchange_directive_token(bearer_token uuid)
returns exchanged_directive as $$
declare
  directive_row directives;
  applied_row applied_directives;
begin

  select * into directive_row
  from directives d where d.token = bearer_token
  for update of d;

  if not found then
    raise 'Bearer token % is not valid', bearer_token
      using errcode = 'check_violation';
  end if;

  insert into applied_directives (directive_id, user_id)
  values (directive_row.id, auth.uid())
  returning * into applied_row;

  if directive_row.single_use then
    update directives set token = null where id = directive_row.id
    returning * into directive_row;
  end if;

  return (directive_row, applied_row);
end;
$$ language plpgsql security definer;

comment on function exchange_directive_token is '
exchange_directive_token allows a user to turn in a directive bearer token
and, in exchange, create an application of that directive.

If the supplied token is valid then a new row is created in `applied_directives`.
The user must next update it with their supplied claims.

Having applied a directive through its token, the user is now able to view
the directive. As a convience, this function also returns the directive
along with the newly-created applied_directive row.
';