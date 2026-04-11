-- Harden calendar pending cron invoker:
-- - remove hardcoded project URL fallback
-- - remove hardcoded JWT fallback
-- The invoker now requires custom settings to be explicitly set.

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
  cron_auth_token := nullif(current_setting('custom.cron_auth_token', true), '');

  if edge_function_url is null then
    raise warning 'invoke_calendar_pending_cron skipped: custom.calendar_pending_edge_function_url is not set';
    return;
  end if;

  if cron_auth_token is null then
    raise warning 'invoke_calendar_pending_cron skipped: custom.cron_auth_token is not set';
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
