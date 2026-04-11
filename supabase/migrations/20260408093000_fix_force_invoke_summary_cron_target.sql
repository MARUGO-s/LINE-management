-- Fix manual-run RPC target and fallback token to current project.
-- invoke_summary_cron(force_run) was still pointing to legacy project (ppuz...),
-- causing admin manual execution to enqueue work on another project and no local log to appear.

create or replace function public.invoke_summary_cron(force_run boolean)
returns void
language plpgsql
security definer
as $$
declare
  edge_function_url text;
  cron_auth_token text;
  request_id bigint;
begin
  edge_function_url := nullif(current_setting('custom.edge_function_url', true), '');
  cron_auth_token := nullif(current_setting('custom.cron_auth_token', true), '');

  if edge_function_url is null then
    edge_function_url := 'https://jhpmzqxqvapdkyvvhyra.supabase.co/functions/v1/summary-cron';
  end if;

  if force_run then
    if strpos(edge_function_url, '?') > 0 then
      edge_function_url := edge_function_url || '&force=true';
    else
      edge_function_url := edge_function_url || '?force=true';
    end if;
  end if;

  if cron_auth_token is null then
    raise warning 'invoke_summary_cron(force_run=%) skipped: custom.cron_auth_token is not set', force_run;
    return;
  end if;

  select net.http_post(
    url := edge_function_url,
    headers := jsonb_build_object(
      'Content-Type', 'application/json',
      'Authorization', 'Bearer ' || cron_auth_token
    )
  ) into request_id;

  raise log 'invoke_summary_cron(force_run=%): Triggered Edge Function at %, request_id=%', force_run, edge_function_url, request_id;
end;
$$;
