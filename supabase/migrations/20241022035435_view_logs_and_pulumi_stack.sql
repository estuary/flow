BEGIN;

CREATE FUNCTION public.view_logs(bearer_token UUID, last_logged_at TIMESTAMPTZ) RETURNS SETOF internal.log_lines
LANGUAGE SQL
STABLE
SECURITY DEFINER
SET search_path = ''
AS $$
  SELECT *
  FROM internal.log_lines
  WHERE internal.log_lines.token = bearer_token
    AND internal.log_lines.logged_at > COALESCE(last_logged_at, '0001-01-01T00:00:00Z')
  ORDER BY logged_at ASC;
$$;

-- We don't need to store this (it exists only in pulumi state)
ALTER TABLE public.data_planes DROP ssh_private_key;
-- Currently JSON, switching to TEXT.
ALTER TABLE public.data_planes DROP status;

-- Output of AWS private link attachments.
ALTER TABLE public.data_planes ADD aws_link_endpoints JSON[];
-- Controller task bound to this data plane.
ALTER TABLE public.data_planes ADD controller_task_id public.flowid DEFAULT internal.id_generator();
-- Branch used for deployments of this data-plane.
ALTER TABLE public.data_planes ADD deploy_branch TEXT;
-- Encrypted pulumi key which protects stack state secrets.
ALTER TABLE public.data_planes ADD pulumi_key TEXT;
-- Name of the pulumi stack for this data-plane.
ALTER TABLE public.data_planes ADD pulumi_stack TEXT;
-- Controller status, which is replicated into its data_planes row.
ALTER TABLE public.data_planes ADD status TEXT;
-- Pulumi stack names must be unique.
ALTER TABLE public.data_planes ADD CONSTRAINT unique_pulumi_stack UNIQUE (pulumi_stack);


CREATE OR REPLACE FUNCTION internal.check_changes_when_status_not_idle() RETURNS trigger
SET search_path = ''
AS $$
BEGIN
  -- Allow updates only when Idle.
  IF OLD.status <> 'Idle' THEN
      IF OLD.config::text IS DISTINCT FROM NEW.config::text THEN
          RAISE EXCEPTION 'Cannot change column "config" when status is not "Idle"';
      END IF;

      IF OLD.deploy_branch IS DISTINCT FROM NEW.deploy_branch THEN
          RAISE EXCEPTION 'Cannot change column "deploy_branch" when status is not "Idle"';
      END IF;
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;


CREATE TRIGGER check_changes_when_status_not_idle_trigger
BEFORE UPDATE ON public.data_planes
FOR EACH ROW
EXECUTE FUNCTION internal.check_changes_when_status_not_idle();

END;