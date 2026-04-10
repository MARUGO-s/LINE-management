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

  -- Fallback to anon JWT only. Do not use service role key in DB function bodies.
  IF cron_auth_token IS NULL THEN
    cron_auth_token := 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InBwdXpjdnN0ZGtubGlxYmVuZGF6Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzE4MzI1ODEsImV4cCI6MjA4NzQwODU4MX0.gnicEgmxnQQj9H_p-sHB_wx0APpJ6wly0T-tKMYhJ-w'; -- gitleaks:allow (Supabase anon JWT default in migration; use vault in prod)
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
