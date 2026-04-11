create table if not exists public.line_room_names (
  room_id text primary key,
  room_name text not null,
  updated_at timestamptz not null default now()
);

alter table public.line_room_names enable row level security;

do $$
begin
  if not exists (
    select 1
    from pg_policies
    where schemaname = 'public'
      and tablename = 'line_room_names'
      and policyname = 'Service role can do everything on line_room_names'
  ) then
    create policy "Service role can do everything on line_room_names"
      on public.line_room_names
      for all
      to public
      using ((auth.jwt() ->> 'role') = 'service_role')
      with check ((auth.jwt() ->> 'role') = 'service_role');
  end if;
end
$$;

create index if not exists idx_line_room_names_updated_at
  on public.line_room_names (updated_at desc);

create or replace function public.get_room_overview()
returns table (
  room_id text,
  room_name text,
  total_messages bigint,
  pending_messages bigint,
  last_message_at timestamptz,
  last_pending_at timestamptz,
  settings_enabled boolean,
  settings_delivery_hours integer[],
  settings_updated_at timestamptz
)
language sql
security definer
set search_path = public
as $$
with message_stats as (
    select
        lm.room_id,
        count(*)::bigint as total_messages,
        count(*) filter (where lm.processed = false)::bigint as pending_messages,
        max(lm.created_at) as last_message_at,
        max(lm.created_at) filter (where lm.processed = false) as last_pending_at
    from public.line_messages lm
    group by lm.room_id
),
all_room_ids as (
    select room_id from message_stats
    union
    select room_id from public.room_summary_settings
    union
    select room_id from public.line_room_names
)
select
    r.room_id,
    coalesce(rs.room_name, rn.room_name, r.room_id) as room_name,
    coalesce(ms.total_messages, 0)::bigint as total_messages,
    coalesce(ms.pending_messages, 0)::bigint as pending_messages,
    ms.last_message_at,
    ms.last_pending_at,
    coalesce(rs.is_enabled, true) as settings_enabled,
    rs.delivery_hours as settings_delivery_hours,
    rs.updated_at as settings_updated_at
from all_room_ids r
left join message_stats ms on ms.room_id = r.room_id
left join public.room_summary_settings rs on rs.room_id = r.room_id
left join public.line_room_names rn on rn.room_id = r.room_id
order by coalesce(ms.last_message_at, rs.updated_at, rn.updated_at) desc nulls last, r.room_id asc;
$$;
