-- Harden cron invocation: remove service-role-key fallback from DB function.
-- Use a non-privileged JWT token for invoking the protected Edge Function.

CREATE OR REPLACE FUNCTION public.invoke_summary_cron()
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

  -- Fallback to anon JWT only. Do not use service role key in DB function bodies.
  IF cron_auth_token IS NULL THEN
    cron_auth_token := 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InBwdXpjdnN0ZGtubGlxYmVuZGF6Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzE4MzI1ODEsImV4cCI6MjA4NzQwODU4MX0.gnicEgmxnQQj9H_p-sHB_wx0APpJ6wly0T-tKMYhJ-w';
  END IF;

  SELECT net.http_post(
      url := edge_function_url,
      headers := jsonb_build_object(
        'Content-Type', 'application/json',
        'Authorization', 'Bearer ' || cron_auth_token
      )
  ) INTO request_id;
  
  RAISE LOG 'invoke_summary_cron: Triggered Edge Function at %, request_id=%', edge_function_url, request_id;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Try to clean up legacy setting, but do not fail migration if permission is missing.
DO $$
BEGIN
  BEGIN
    EXECUTE 'ALTER DATABASE postgres RESET custom.service_role_key';
  EXCEPTION
    WHEN insufficient_privilege THEN
      RAISE NOTICE 'Skipping RESET custom.service_role_key (insufficient privilege).';
  END;
END;
$$;

-- Update diagnostic helper for the new token source.
CREATE OR REPLACE FUNCTION public.check_cron_settings()
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
  RETURN jsonb_build_object(
    'edge_function_url', current_setting('custom.edge_function_url', true),
    'cron_auth_token_set', CASE 
      WHEN current_setting('custom.cron_auth_token', true) IS NOT NULL 
       AND current_setting('custom.cron_auth_token', true) != ''
      THEN true ELSE false END,
    'legacy_service_role_key_set', CASE 
      WHEN current_setting('custom.service_role_key', true) IS NOT NULL 
       AND current_setting('custom.service_role_key', true) != ''
       AND current_setting('custom.service_role_key', true) != 'YOUR_SERVICE_ROLE_KEY'
      THEN true ELSE false END,
    'current_time_utc', now()
  );
END;
$$;
