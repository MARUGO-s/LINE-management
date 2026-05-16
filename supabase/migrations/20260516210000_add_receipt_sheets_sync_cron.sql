-- Pilot: Google Sheets <-> Supabase for one store (receipt-sheets-sync-cron Edge Function)

create or replace function public.invoke_receipt_sheets_sync_cron()
returns void
language plpgsql
security definer
as $$
declare
  edge_function_url text;
  cron_auth_token text;
  request_id bigint;
begin
  edge_function_url := nullif(current_setting('custom.receipt_sheets_sync_edge_function_url', true), '');
  if edge_function_url is null then
    edge_function_url := 'https://jhpmzqxqvapdkyvvhyra.supabase.co/functions/v1/receipt-sheets-sync-cron';
  end if;

  cron_auth_token := public.resolve_edge_cron_auth_token();
  if cron_auth_token is null then
    raise warning 'invoke_receipt_sheets_sync_cron skipped: cron auth token is not configured';
    return;
  end if;

  select net.http_post(
    url := edge_function_url,
    headers := jsonb_build_object(
      'Content-Type', 'application/json',
      'Authorization', 'Bearer ' || cron_auth_token
    ),
    body := jsonb_build_object('direction', 'both')
  ) into request_id;

  raise log 'invoke_receipt_sheets_sync_cron: Triggered Edge Function at %, request_id=%', edge_function_url, request_id;
end;
$$;

do $$
begin
  begin
    perform cron.unschedule('receipt-sheets-sync-cron-job');
  exception
    when others then
      null;
  end;
end
$$;

-- Every hour at :15 (UTC). Adjust if needed; manual sync via Apps Script is also supported.
select cron.schedule(
  'receipt-sheets-sync-cron-job',
  '15 * * * *',
  $$ select public.invoke_receipt_sheets_sync_cron(); $$
);
