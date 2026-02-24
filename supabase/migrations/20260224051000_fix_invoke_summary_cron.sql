-- Legacy fix (sanitized): removed previously embedded sensitive key.
-- Current secure behavior is defined in later migrations.

CREATE OR REPLACE FUNCTION public.invoke_summary_cron()
RETURNS void AS $$
DECLARE
  edge_function_url text;
  service_role_key text;
  request_id bigint;
BEGIN
  -- Try to get from custom settings first
  edge_function_url := current_setting('custom.edge_function_url', true);
  service_role_key := current_setting('custom.service_role_key', true);
  
  -- Fallback: Use hardcoded production URL if custom setting is null or empty
  IF edge_function_url IS NULL OR edge_function_url = '' THEN
    edge_function_url := 'https://ppuzcvstdknliqbendaz.supabase.co/functions/v1/summary-cron';
  END IF;

  -- Fallback: Use hardcoded service role key if not configured
  -- WARNING: In production, use ALTER DATABASE to set this securely
  IF service_role_key IS NULL OR service_role_key = '' OR service_role_key = 'YOUR_SERVICE_ROLE_KEY' THEN
    service_role_key := 'YOUR_SERVICE_ROLE_KEY';
  END IF;

  SELECT net.http_post(
      url := edge_function_url,
      headers := jsonb_build_object(
        'Content-Type', 'application/json',
        'Authorization', 'Bearer ' || service_role_key
      )
  ) INTO request_id;
  
  RAISE LOG 'invoke_summary_cron: Triggered Edge Function at %, request_id=%', edge_function_url, request_id;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
