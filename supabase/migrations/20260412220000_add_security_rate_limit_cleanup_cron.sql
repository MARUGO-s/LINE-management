-- Daily cleanup job for shared rate limit table.

create or replace function public.cleanup_security_rate_limits(
  retention interval default interval '2 days'
)
returns integer
language plpgsql
security definer
as $$
declare
  deleted_count integer := 0;
begin
  delete from public.security_rate_limits
  where updated_at < now() - retention;

  get diagnostics deleted_count = row_count;
  return deleted_count;
end;
$$;

do $$
begin
  begin
    perform cron.unschedule('security-rate-limit-cleanup-job');
  exception
    when others then
      null;
  end;
end
$$;

select cron.schedule(
  'security-rate-limit-cleanup-job',
  '17 3 * * *',
  $$ select public.cleanup_security_rate_limits(interval '2 days'); $$
);
