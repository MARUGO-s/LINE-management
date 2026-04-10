-- Add a dedicated Gmail-alert cron runner for near real-time LINE notifications.
-- This avoids waiting for the summary cron schedule and runs every minute.

create or replace function public.invoke_gmail_alert_cron()
returns void
language plpgsql
security definer
as $$
declare
  edge_function_url text;
  cron_auth_token text;
  request_id bigint;
begin
  edge_function_url := nullif(current_setting('custom.gmail_alert_edge_function_url', true), '');
  cron_auth_token := nullif(current_setting('custom.cron_auth_token', true), '');

  if edge_function_url is null then
    edge_function_url := 'https://jhpmzqxqvapdkyvvhyra.supabase.co/functions/v1/gmail-alert-cron';
  end if;

  if cron_auth_token is null then
    cron_auth_token := 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImpocG16cXhxdmFwZGt5dnZoeXJhIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzM1MTg2MDMsImV4cCI6MjA4OTA5NDYwM30.OKZtSANaGsqOVLK1bqjYEeVvSMvRp8uFsIsjiscgpI0';
  end if;

  select net.http_post(
    url := edge_function_url,
    headers := jsonb_build_object(
      'Content-Type', 'application/json',
      'Authorization', 'Bearer ' || cron_auth_token
    )
  ) into request_id;

  raise log 'invoke_gmail_alert_cron: Triggered Edge Function at %, request_id=%', edge_function_url, request_id;
end;
$$;

do $$
begin
  begin
    perform cron.unschedule('gmail-alert-cron-job');
  exception
    when others then
      null;
  end;
end
$$;

select cron.schedule(
  'gmail-alert-cron-job',
  '* * * * *',
  $$ select public.invoke_gmail_alert_cron(); $$
);
