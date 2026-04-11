-- Add a force-run variant for manual execution from admin API.
-- Existing no-argument invoke_summary_cron() remains for pg_cron scheduled runs.

CREATE OR REPLACE FUNCTION public.invoke_summary_cron(force_run boolean)
RETURNS void AS $$
DECLARE
  edge_function_url text;
  cron_auth_token text;
  request_id bigint;
BEGIN
  edge_function_url := nullif(current_setting('custom.edge_function_url', true), '');
  cron_auth_token := nullif(current_setting('custom.cron_auth_token', true), '');

  IF edge_function_url IS NULL THEN
    edge_function_url := 'https://ppuzcvstdknliqbendaz.supabase.co/functions/v1/summary-cron';
  END IF;

  IF force_run THEN
    IF strpos(edge_function_url, '?') > 0 THEN
      edge_function_url := edge_function_url || '&force=true';
    ELSE
      edge_function_url := edge_function_url || '?force=true';
    END IF;
  END IF;

  IF cron_auth_token IS NULL THEN
    RAISE WARNING 'invoke_summary_cron(force_run=%) skipped: custom.cron_auth_token is not set', force_run;
    RETURN;
  END IF;

  SELECT net.http_post(
      url := edge_function_url,
      headers := jsonb_build_object(
        'Content-Type', 'application/json',
        'Authorization', 'Bearer ' || cron_auth_token
      )
  ) INTO request_id;
  
  RAISE LOG 'invoke_summary_cron(force_run=%): Triggered Edge Function at %, request_id=%', force_run, edge_function_url, request_id;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
