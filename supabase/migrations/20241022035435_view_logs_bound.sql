BEGIN;

CREATE FUNCTION public.view_logs(bearer_token UUID, last_logged_at TIMESTAMPTZ) RETURNS SETOF internal.log_lines
LANGUAGE SQL STABLE SECURITY DEFINER
AS $$
  SELECT * FROM internal.log_lines
  WHERE internal.log_lines.token = bearer_token
    AND internal.log_lines.logged_at > COALESCE(last_logged_at, '0001-01-01T00:00:00Z');
$$;

END;