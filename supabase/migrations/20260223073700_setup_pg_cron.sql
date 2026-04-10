-- Enable pg_net for HTTP requests
create extension if not exists "pg_net";
-- Enable pg_cron for scheduled jobs
create extension if not exists "pg_cron" schema pg_catalog;


-- Create a function to trigger the summary-cron Edge Function
create or replace function public.invoke_summary_cron()
returns void as $$
declare
  edge_function_url text;
  service_role_key text;
  request_id bigint;
begin
  -- Set custom configuration for the environment
  -- In production, make sure to set these via:
  -- ALTER DATABASE postgres SET custom.edge_function_url TO 'https://[ref].supabase.co/functions/v1/summary-cron';
  -- ALTER DATABASE postgres SET custom.service_role_key TO '...';
  
  edge_function_url := current_setting('custom.edge_function_url', true);
  service_role_key := current_setting('custom.service_role_key', true);
  
  -- Fallback for local development environment
  if edge_function_url is null then
    edge_function_url := 'http://host.docker.internal:54321/functions/v1/summary-cron';
  end if;

  if service_role_key is null then
    service_role_key := 'YOUR_SERVICE_ROLE_KEY'; -- Only for placeholder
  end if;

  select net.http_post(
      url := edge_function_url,
      headers := jsonb_build_object(
        'Content-Type', 'application/json',
        'Authorization', 'Bearer ' || service_role_key
      )
  ) into request_id;
end;
$$ language plpgsql security definer;

-- Schedule the cron job to run at 12:00 and 18:00 every day
-- Note: the cron extension must be enabled. Supabase enables it by default on the database.
select cron.schedule(
  'summary-cron-job',
  '0 12,18 * * *', -- Runs at 12:00 PM and 6:00 PM UTC by default (adjust for JST if needed, e.g. 3,9 for JST 12:00, 18:00)
  $$ select public.invoke_summary_cron(); $$
);
