-- Shared DB-backed rate limiter for Edge Functions.
-- In-memory counters are not reliable across distributed instances.

create table if not exists public.security_rate_limits (
  bucket text not null,
  window_start timestamptz not null,
  hit_count integer not null default 0,
  updated_at timestamptz not null default now(),
  primary key (bucket, window_start)
);

create index if not exists security_rate_limits_updated_at_idx
  on public.security_rate_limits (updated_at);

create or replace function public.consume_security_rate_limit(
  rate_bucket text,
  window_seconds integer,
  max_hits integer
)
returns table (
  allowed boolean,
  hit_count integer,
  retry_after_seconds integer
)
language plpgsql
security definer
as $$
declare
  now_ts timestamptz;
  window_ts timestamptz;
  new_count integer;
  elapsed_seconds integer;
begin
  if rate_bucket is null or btrim(rate_bucket) = '' then
    raise exception 'rate_bucket must not be empty';
  end if;
  if window_seconds is null or window_seconds < 1 then
    raise exception 'window_seconds must be >= 1';
  end if;
  if max_hits is null or max_hits < 1 then
    raise exception 'max_hits must be >= 1';
  end if;

  now_ts := now();
  window_ts := to_timestamp(floor(extract(epoch from now_ts) / window_seconds) * window_seconds);

  insert into public.security_rate_limits (bucket, window_start, hit_count, updated_at)
  values (rate_bucket, window_ts, 1, now_ts)
  on conflict (bucket, window_start)
  do update
    set hit_count = public.security_rate_limits.hit_count + 1,
        updated_at = excluded.updated_at
  returning public.security_rate_limits.hit_count into new_count;

  elapsed_seconds := greatest(0, floor(extract(epoch from now_ts - window_ts))::integer);

  -- Periodic lightweight cleanup.
  if random() < 0.01 then
    delete from public.security_rate_limits
    where updated_at < (now() - interval '2 days');
  end if;

  allowed := new_count <= max_hits;
  hit_count := new_count;
  retry_after_seconds := greatest(1, window_seconds - elapsed_seconds);
  return next;
end;
$$;
