
create function tests.test_flowid_generation_and_model_tables()
returns setof text as $$
declare
  test_id flowid;
  model_row internal._model_async%rowtype;
  start_time timestamptz = now();
begin

  test_id = internal.id_generator();
  return query select ok(internal.id_generator() > test_id, 'ids are ascending');

  insert into internal._model_async (detail) values (default);
  select * into model_row from internal._model_async;

  return query select ok(model_row.id > test_id, 'model defaults to generated id');
  return query select ok(model_row.created_at >= start_time, 'model created_at');
  return query select ok(model_row.updated_at >= start_time, 'model updated_at');

  return query select ok(
    model_row.logs_token != '00000000-00000000-00000000-00000000',
    'model has random logs_token');

end;
$$ language plpgsql;