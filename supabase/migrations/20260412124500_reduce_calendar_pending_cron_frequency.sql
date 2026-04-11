-- Reduce calendar pending cron frequency to every 5 minutes.
-- This lowers background polling load while still processing expired
-- pending confirmations regularly.

do $$
begin
  begin
    perform cron.unschedule('calendar-pending-cron-job');
  exception
    when others then
      null;
  end;
end
$$;

select cron.schedule(
  'calendar-pending-cron-job',
  '*/5 * * * *',
  $$ select public.invoke_calendar_pending_cron(); $$
);
