
create function billing_report(billed_prefix catalog_prefix, billed_month timestamptz)
returns jsonb as $$
declare
    included_tasks  bigint = 2;
    task_rate       bigint = 2000;
    included_gb     numeric = 10.0;
    gb_rate         bigint = 75;

    actual_gb       numeric;
    actual_tasks    integer;
    max_tasks_hour  timestamptz;

    line_items      jsonb = '[]';
    line_item_count numeric;
    line_item_rate  bigint;
    subtotal        bigint = 0;
begin

  -- Verify that the user has an admin grant for the requested `billed_prefix`.
  perform 1 from auth_roles('admin') as r where billed_prefix ^@ r.role_prefix;
  if not found then
    -- errcode 28000 causes potgrest to return an HTTP 403
    -- see: https://www.postgresql.org/docs/current/errcodes-appendix.html
    -- and: https://postgrest.org/en/stable/errors.html#status-codes
    raise 'You are not authorized for the billed prefix %', billed_prefix using errcode = 28000;
  end if;

  -- Ensure `billed_month` is the truncated start of the billed month.
  billed_month = date_trunc('month', billed_month);

  -- Determine the total amount of data transfer done by tasks
  -- under `billed_prefix` in the given `billed_month`.
  select into actual_gb sum(bytes_written_by_me + bytes_read_by_me) / (1024.0 * 1024 * 1024)
    from catalog_stats
    where catalog_name ^@ billed_prefix
    and grain = 'monthly'
    and ts = billed_month
  ;

  -- Determine the hour of the month that had the largest number of distinct running tasks.
  -- Select out that hour, and the number of tasks.
  select into max_tasks_hour, actual_tasks
    ts, count(*)
    from catalog_stats
    where catalog_name ^@ billed_prefix
    and grain = 'hourly'
    and ts >= billed_month and ts < (billed_month + interval '1 month')
    -- TODO(johnny): This should be based on logs != 0 instead.
    and (bytes_written_by_me != 0 or bytes_read_by_me != 0)
    group by ts
    order by count(*) desc, ts desc
    limit 1
  ;

  select into line_items jsonb_agg(jsonb_build_object(
    'description', "description",
    'count', round(count, 4),
    'rate', rate,
    'subtotal', round(count * rate)
  )) from (values
    (
      format('Included task shards (up to %s)', included_tasks),
      least(included_tasks, actual_tasks),
      0
    ),
    (
      format('Additional task shards minimum (assessed at %s)', max_tasks_hour),
      greatest(0, actual_tasks - included_tasks),
      task_rate
    ),
    (
      format('Included data processing (in GB, up to %sGB)', included_gb),
      least(included_gb, actual_gb),
      0
    ),
    (
      'Additional data processing (in GB)',
      greatest(0, actual_gb - included_gb),
      gb_rate
    )
  ) as t("description", count, rate);

  line_items = line_items || jsonb_build_object(
      'description', 'Subtotal is greater of task shards minimum, or data processing volume'
  );
  subtotal = greatest((line_items->1->'subtotal')::bigint, (line_items->3->'subtotal')::bigint);

  return jsonb_build_object(
    'billed_prefix', billed_prefix,
    'billed_month', billed_month,
    'line_items', line_items,
    'subtotal', subtotal
  );

end
$$ language plpgsql volatile security definer;
