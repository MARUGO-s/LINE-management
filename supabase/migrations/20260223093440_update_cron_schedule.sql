-- Remove the existing scheduled job
select cron.unschedule('summary-cron-job');

-- Schedule the cron job to run at 12:00, 17:00, and 23:00 JST
-- JST is UTC+9, so:
-- 12:00 JST = 03:00 UTC
-- 17:00 JST = 08:00 UTC
-- 23:00 JST = 14:00 UTC
select cron.schedule(
  'summary-cron-job',
  '0 3,8,14 * * *',
  $$ select public.invoke_summary_cron(); $$
);
