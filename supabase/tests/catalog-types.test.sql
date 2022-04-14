
create or replace function tests.test_catalog_name_constraints()
returns setof text as $$
declare
  test_case record;
begin

  for test_case in
    select "name", "valid" from (values
      ('foo/bar', true),
      ('foo/Ḃaz', true),
      ('a/ß/42_Five-Six.7', true),
      ('missing-any-slash', false),
      ('double//slash', false),
      ('has/a space', false),
      ('/leading/slash', false),
      ('ending/slash/', false)
    ) as t("name", "valid")
  loop
    case
      when test_case.valid then
        return query select lives_ok(
          format('select ''%s''::catalog_name', test_case."name"),
          format('valid catalog_name: %s', test_case."name")
        );
      else
        return query select throws_like(
          format('select ''%s''::catalog_name', test_case."name"),
          '% violates check constraint "Must be a valid catalog name"',
          format('invalid catalog_name: %s', test_case."name")
        );
    end case;
  end loop;

  for test_case in
    select "prefix", "valid" from (values
      ('foo/bar/', true),
      ('foo/Ḃaz/', true),
      ('a/ß/42_Five-Six.7/', true),
      ('top-level/', true),
      ('', false),
      ('double//slash/', false),
      ('has/a space/', false),
      ('/leading/slash/', false),
      ('ending/double/slash//', false)
    ) as t("prefix", "valid")
  loop
    case
      when test_case.valid then
        return query select lives_ok(
          format('select ''%s''::catalog_prefix', test_case."prefix"),
          format('valid catalog_prefix: %s', test_case."prefix")
        );
      else
        return query select throws_like(
          format('select ''%s''::catalog_prefix', test_case."prefix"),
          '% violates check constraint "Must be a valid catalog prefix"',
          format('invalid catalog_prefix: %s', test_case."prefix")
        );
    end case;
  end loop;
end;
$$ language plpgsql;