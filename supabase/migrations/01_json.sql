-- We write SQL according to https://www.sqlstyle.guide/
-- It's an arbitrary style guide, but it's important to have one for consistency.
-- We also lower-case SQL keywords, as is common within Supabase documentation.

create domain json_obj as json check (json_typeof(value) = 'object');
comment on domain json_obj is
  'json_obj is JSON which is restricted to the "object" type';

create domain jsonb_obj as jsonb check (jsonb_typeof(value) = 'object');
comment on domain jsonb_obj is
  'jsonb_obj is JSONB which is restricted to the "object" type';

create schema internal;
comment on schema internal is
  'Internal schema used for types, tables, and procedures we don''t expose in our API';

-- jsonb_merge_patch "target" with "patch" as a RFC 7396 JSON Merge Patch.
create function internal.jsonb_merge_patch("target" jsonb, "patch" jsonb)
returns jsonb as $$
begin
  case
    when "patch" is null then
      return "target";
    when "patch" = 'null' then
      return null; -- Remove location.
    when jsonb_typeof("target") is distinct from 'object' or
         jsonb_typeof("patch")  is distinct from 'object' then
      -- If either side is not an object, take the patch.
      return jsonb_strip_nulls("patch");
    when "target" = jsonb_strip_nulls("patch") then
      -- Both are objects, and the patch doesn't change the target.
      -- This case *could* be handled by the recursive case,
      -- but equality and stripping nulls is dirt cheap compared to
      -- the cost of recursive jsonb_object_agg, which must repeatedly
      -- copy nested sub-structure.
      return "target";
    else
      return (
        with props as (
          select
            coalesce("tkey", "pkey") as "key",
            case
                when "pval" isnull then "tval"
                else internal.jsonb_merge_patch("tval", "pval")
            end as "val"
          from            jsonb_each("target") e1("tkey", "tval")
          full outer join jsonb_each("patch")  e2("pkey", "pval") on "tkey" = "pkey"
          where "pval" is distinct from 'null'
        )
        select coalesce(jsonb_object_agg("key", "val"), '{}') from props
      );
  end case;
end;
$$ language plpgsql immutable;


-- jsonb_merge_diff "target" with "source" to derive a RFC 7396 JSON Merge Patch
-- which will patch source into target. JSON 'null' locations in both source and
-- target are permitted, but it's not possible to patch a source location into a
-- 'null' value, as this isn't a supported operation by JSON merge patch.
-- In this case, this function will instead explicitly remove the location.
--
-- Be careful when returning a jsonb_merge_diff result as JSON, because a
-- returned NULL means "there is no difference", while JSON 'null' means
-- "remove the entire document". JSON serialization will collapse both cases
-- to JSON 'null'. To fix this, first coalesce the result of this function into
-- the expected top-level type, such as "coalesce(my_patch, '{}')".
create function internal.jsonb_merge_diff("target" jsonb, "source" jsonb)
returns jsonb as $$
begin
  case
    when "target" isnull then
      return 'null'; -- Marker to remove location.
    when jsonb_typeof("target") is distinct from 'object' or
         jsonb_typeof("source") is distinct from 'object' then
      return (case
        when "target" = "source" then null
        else jsonb_strip_nulls("target")
      end);
    else
      return (
        with props as (
          select
            coalesce("tkey", "skey")                  as "key",
            internal.jsonb_merge_diff("tval", "sval") as "val"
          from            jsonb_each("target") e1("tkey", "tval")
          full outer join jsonb_each("source") e2("skey", "sval") on "tkey" = "skey"
        )
        -- If no props are different, the result is NULL (not 'null').
        select jsonb_object_agg("key", "val")
        from props
        where "val" is not null
      );
  end case;
end;
$$ language plpgsql immutable;
