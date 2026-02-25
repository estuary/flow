ALTER TYPE public.alert_type ADD VALUE IF NOT EXISTS 'task_chronically_failing';
ALTER TYPE public.alert_type ADD VALUE IF NOT EXISTS 'task_auto_disabled_failing';
ALTER TYPE public.alert_type ADD VALUE IF NOT EXISTS 'task_idle';
ALTER TYPE public.alert_type ADD VALUE IF NOT EXISTS 'task_auto_disabled_idle';
