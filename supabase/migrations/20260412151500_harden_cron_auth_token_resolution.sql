-- Remove plaintext cron token fallbacks and resolve auth token from secure sources only.
-- Priority: custom.cron_auth_token -> vault CRON_AUTH_TOKEN -> vault SUPABASE_ANON_KEY.

create or replace function public.resolve_edge_cron_auth_token()
returns text
language plpgsql
security definer
as $$
declare
  token text;
begin
  token := nullif(current_setting('custom.cron_auth_token', true), '');
  if token is not null then
    return token;
  end if;

  begin
    execute 'select decrypted_secret from vault.decrypted_secrets where name = $1 limit 1'
      into token
      using 'CRON_AUTH_TOKEN';
    token := nullif(token, '');
  exception when others then
    token := null;
  end;
  if token is not null then
    return token;
  end if;

  begin
    execute 'select decrypted_secret from vault.decrypted_secrets where name = $1 limit 1'
      into token
      using 'SUPABASE_ANON_KEY';
    token := nullif(token, '');
  exception when others then
    token := null;
  end;
  return token;
end;
$$;

create or replace function public.invoke_summary_cron()
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
  if edge_function_url is null then
    edge_function_url := 'https://jhpmzqxqvapdkyvvhyra.supabase.co/functions/v1/summary-cron';
  end if;

  cron_auth_token := public.resolve_edge_cron_auth_token();
  if cron_auth_token is null then
    raise warning 'invoke_summary_cron skipped: cron auth token is not configured';
    return;
  end if;

  select net.http_post(
    url := edge_function_url,
    headers := jsonb_build_object(
      'Content-Type', 'application/json',
      'Authorization', 'Bearer ' || cron_auth_token
    )
  ) into request_id;

  raise log 'invoke_summary_cron: Triggered Edge Function at %, request_id=%', edge_function_url, request_id;
end;
$$;

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

  cron_auth_token := public.resolve_edge_cron_auth_token();
  if cron_auth_token is null then
    raise warning 'invoke_summary_cron(force_run=%) skipped: cron auth token is not configured', force_run;
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
  if edge_function_url is null then
    edge_function_url := 'https://jhpmzqxqvapdkyvvhyra.supabase.co/functions/v1/gmail-alert-cron';
  end if;

  cron_auth_token := public.resolve_edge_cron_auth_token();
  if cron_auth_token is null then
    raise warning 'invoke_gmail_alert_cron skipped: cron auth token is not configured';
    return;
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
    raise warning 'invoke_calendar_pending_cron skipped: custom.calendar_pending_edge_function_url is not set';
    return;
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
