
create function tests.test_json_objects()
returns setof text as $$
begin

  return query select lives_ok('select ''{"1":"2"}''::json_obj');
  return query select throws_like(
    'select ''"not an object"''::json_obj',
    '% violates check constraint "json_obj_check"'
  );
  return query select lives_ok('select ''{"1":"2"}''::jsonb_obj');
  return query select throws_like(
    'select ''"not an object"''::jsonb_obj',
    '% violates check constraint "jsonb_obj_check"'
  );

end;
$$ language plpgsql;


create function tests.test_json_merge_patch_and_diff()
returns setof text as $$
declare
  test_case record;
  fwd_patch jsonb;
  rev_patch jsonb;
begin

  for test_case in
    select orig::jsonb, patch::jsonb, result::jsonb from (values
      -- Test cases from RFC 7386, lightly edited to remove supurious
      -- removals not present in the original documents, so that each
      -- test case properly round-trips.
      -- https://datatracker.ietf.org/doc/html/rfc7386#appendix-A
      ('{"a":"b"}', '{"a":"c"}', '{"a":"c"}'),
      ('{"a":"b"}', '{"b":"c"}', '{"a":"b","b":"c"}'),
      ('{"a":"b"}', '{"a":null}','{}'),
      ('{"a":"b","b":"c"}', '{"a":null}', '{"b":"c"}'),
      ('{"a":["b"]}', '{"a":"c"}', '{"a":"c"}'),
      ('{"a":"c"}', '{"a":["b"]}', '{"a":["b"]}'),
      ('{"a":{"b":"c","c":true}}', '{"a":{"b":"d","c":null}}', '{"a":{"b":"d"}}'),
      ('{"a":[{"b":"c"}]}', '{"a":[1]}', '{"a":[1]}'),
      ('["a","b"]', '["c","d"]', '["c","d"]'),
      ('{"a":"b"}', '["c"]', '["c"]'),
      ('{"a":"foo"}', 'null', null),
      ('{"a":"foo"}', '"bar"', '"bar"'),
      ('{"e":null}', '{"a":1}', '{"e":null,"a":1}'),
      ('[1,2]', '{"a":"b"}', '{"a":"b"}'),
      ('{}', '{"a":{"bb":{}}}', '{"a":{"bb":{}}}'),
      -- Test cases added by us:
      ('{"a":{"b":{"c":{},"d":42}}}', null, '{"a":{"b":{"c":{},"d":42}}}'),
      ('{"a":{"b":{"c":{}}}}', 'null', null),
      ('{"a":{"b":{"c":32},"d":true}}', '{"a":{"b":{"c":42}}}', '{"a":{"b":{"c":42},"d":true}}'),
      ('true', 'null', null),
      ('true', 'false', 'false')
    ) as t(orig, patch, result)
  loop

    return query select is(
      internal.jsonb_merge_patch(test_case.orig, test_case.patch), test_case.result,
      format('merge orig <- patch is result: %s', row_to_json(test_case)));

    fwd_patch = internal.jsonb_merge_diff(test_case.result, test_case.orig);
    rev_patch = internal.jsonb_merge_diff(test_case.orig, test_case.result);

    return query select is(fwd_patch, test_case.patch,
        format('diff orig <- result is patch: %s', row_to_json(test_case)));

    return query select is(
      internal.jsonb_merge_patch(test_case.orig, fwd_patch), test_case.result,
        format('round-trip of fwd_patch: %s', row_to_json(test_case)));
    return query select is(
      internal.jsonb_merge_patch(test_case.result, rev_patch), test_case.orig,
        format('round-trip of rev_patch: %s', row_to_json(test_case)));

  end loop;

  -- Originals of edited test cases from RFC 7386 above,
  -- repeated here with supurious "null" locations which don't round-trip.
  for test_case in
    select orig::jsonb, patch::jsonb, result::jsonb from (values
      ('{"a":{"b":"c"}}', '{"a":{"b":"d","c":null}}', '{"a":{"b":"d"}}'),
      ('[1,2]', '{"a":"b","c":null}', '{"a":"b"}'),
      ('{}', '{"a":{"bb":{"ccc":null}}}', '{"a":{"bb":{}}}')
    ) as t(orig, patch, result)
  loop
    return query select is(
      internal.jsonb_merge_patch(test_case.orig, test_case.patch), test_case.result,
      format('merge orig <- patch is result: %s', row_to_json(test_case)));
  end loop;

end;
$$ language plpgsql;


create function tests.test_strip_json_null()
returns setof text as $$
begin

  return query select is(jsonb_strip_nulls('null'), 'null');
  return query select is(jsonb_strip_nulls('42'), '42');
  return query select is(jsonb_strip_nulls('[42, null, true]'), '[42, null, true]');

  return query select is(jsonb_strip_nulls('{"a":1,"b":null,"c":false}'),
    '{"a":1,"c":false}');
  return query select is(jsonb_strip_nulls('{"a":1,"b":{"c":{"d":null}}}'),
    '{"a":1,"b":{"c":{}}}');

end;
$$ language plpgsql;