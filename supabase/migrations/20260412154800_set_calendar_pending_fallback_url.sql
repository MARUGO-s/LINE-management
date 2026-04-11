-- Keep calendar pending cron invoker functional even when ALTER DATABASE is not permitted.
-- Use project URL fallback when custom.calendar_pending_edge_function_url is not configured.

create or replace function public.invoke_calendar_pending_cron()
returns void
language plpgsql
security definer
as $$
declare
  edge_function_url text;
  cron_auth_token text;
  request_id bigint;
begin
  edge_function_url := nullif(current_setting('custom.calendar_pending_edge_function_url', true), '');
  if edge_function_url is null then
    edge_function_url := 'https://jhpmzqxqvapdkyvvhyra.supabase.co/functions/v1/calendar-pending-cron';
  end if;

  cron_auth_token := public.resolve_edge_cron_auth_token();
  if cron_auth_token is null then
    raise warning 'invoke_calendar_pending_cron skipped: cron auth token is not configured';
    return;
  end if;

  select net.http_post(
    url := edge_function_url,
    headers := jsonb_build_object(
      'Content-Type', 'application/json',
      'Authorization', 'Bearer ' || cron_auth_token
    )
  ) into request_id;

  raise log 'invoke_calendar_pending_cron: Triggered Edge Function at %, request_id=%', edge_function_url, request_id;
end;
$$;
