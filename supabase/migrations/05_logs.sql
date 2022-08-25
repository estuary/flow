
-- Log lines are newline-delimited outputs from server-side jobs.
create table internal.log_lines (
  log_line  text not null,
  logged_at timestamptz not null default now(),
  stream    text not null,
  token     uuid not null
);

comment on table internal.log_lines is
  'Logs produced by server-side operations';
comment on column internal.log_lines.log_line is
  'Logged line';
comment on column internal.log_lines.token is
  'Bearer token which demarks and provides accesss to a set of logs';
comment on column internal.log_lines.stream is
  'Identifier of the log stream within the job';
comment on column internal.log_lines.logged_at is
  'Time at which the log was collected';

create index idx_logs_token_logged_at on internal.log_lines
  using brin(token, logged_at) with (autosummarize = on);


-- We cannot provide direct SELECT access to logs, but we *can* provide
-- a view on logs so long as the user always provides a bearer token.
create function view_logs(bearer_token uuid)
returns setof internal.log_lines as $$
begin
  return query select * from internal.log_lines where internal.log_lines.token = bearer_token;
end;
$$ language plpgsql security definer;

comment on function view_logs is
  'view_logs accepts a log bearer_token and returns its matching log lines';
