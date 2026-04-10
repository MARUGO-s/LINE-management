-- Remove the existing scheduled job
select cron.unschedule('summary-cron-job');

-- Schedule the cron job to run at 12:00, 17:00, and 23:00 JST
-- Based on the behavior where setting 14 resulted in 14:00 (14:13) execution,
-- the DB/pg_cron time zone appears to be JST.
select cron.schedule(
  'summary-cron-job',
  '0 12,17,23 * * *',
  $$ select public.invoke_summary_cron(); $$
);
