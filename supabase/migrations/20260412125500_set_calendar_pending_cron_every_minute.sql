-- Change calendar pending cron job back to every minute.

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
  '* * * * *',
  $$ select public.invoke_calendar_pending_cron(); $$
);
