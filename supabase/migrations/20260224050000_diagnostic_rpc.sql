-- Diagnostic RPC functions for cron investigation
-- These are temporary functions to diagnose the cron job issues

-- 1. Check custom DB settings
CREATE OR REPLACE FUNCTION public.check_cron_settings()
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
  RETURN jsonb_build_object(
    'edge_function_url', current_setting('custom.edge_function_url', true),
    'service_role_key_set', CASE 
      WHEN current_setting('custom.service_role_key', true) IS NOT NULL 
       AND current_setting('custom.service_role_key', true) != ''
       AND current_setting('custom.service_role_key', true) != 'YOUR_SERVICE_ROLE_KEY'
      THEN true ELSE false END,
    'current_time_utc', now()
  );
END;
$$;

-- 2. Check cron.job status
CREATE OR REPLACE FUNCTION public.check_cron_jobs()
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
  result jsonb;
BEGIN
  SELECT jsonb_agg(
    jsonb_build_object(
      'jobid', jobid,
      'jobname', jobname,
      'schedule', schedule,
      'command', command,
      'active', active,
      'username', username
    )
  )
  INTO result
  FROM cron.job;
  
  RETURN COALESCE(result, '[]'::jsonb);
END;
$$;

-- 3. Check cron.job_run_details (recent 20 executions)
CREATE OR REPLACE FUNCTION public.check_cron_history()
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
  result jsonb;
BEGIN
  SELECT jsonb_agg(
    jsonb_build_object(
      'runid', runid,
      'jobid', jobid,
      'job_pid', job_pid,
      'database', database,
      'username', username,
      'command', command,
      'status', status,
      'return_message', return_message,
      'start_time', start_time,
      'end_time', end_time
    )
    ORDER BY start_time DESC
  )
  INTO result
  FROM (
    SELECT * FROM cron.job_run_details 
    ORDER BY start_time DESC 
    LIMIT 20
  ) sub;
  
  RETURN COALESCE(result, '[]'::jsonb);
END;
$$;
