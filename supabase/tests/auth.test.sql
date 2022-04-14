create function tests.startup_auth_as_alice()
returns setof text as $$
begin
  set request.jwt.claim.sub to '11111111-1111-1111-1111-111111111111';
end;
$$ language plpgsql;

create function tests.test_auth_uid()
returns setof text as $$
  select is(auth_uid(), '11111111-1111-1111-1111-111111111111', 'we''re authorized as alice');
$$ language sql;
