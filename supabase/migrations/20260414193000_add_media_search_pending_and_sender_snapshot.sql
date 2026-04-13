alter table public.line_message_media
  add column if not exists sender_display_name text;

comment on column public.line_message_media.sender_display_name is
  'Snapshot of LINE sender display name at save time (for media search filters).';

create table if not exists public.media_search_pending_confirmations (
  id uuid primary key default gen_random_uuid(),
  conversation_key text not null unique,
  room_id text not null,
  user_id text,
  stage text not null default 'select_period' check (stage in ('select_period', 'select_item')),
  period_months int not null default 3 check (period_months in (0, 3, 6, 12)),
  category_key text not null default 'all',
  sender_query text not null default '',
  item_cursor int not null default 0,
  items_json jsonb not null default '[]'::jsonb,
  expires_at timestamptz not null,
  updated_at timestamptz not null default now(),
  created_at timestamptz not null default now()
);

create index if not exists media_search_pending_expires_idx
  on public.media_search_pending_confirmations (expires_at desc);

alter table public.media_search_pending_confirmations enable row level security;

do $$
begin
  if not exists (
    select 1
    from pg_policies
    where schemaname = 'public'
      and tablename = 'media_search_pending_confirmations'
      and policyname = 'Service role can do everything on media_search_pending_confirmations'
  ) then
    create policy "Service role can do everything on media_search_pending_confirmations"
      on public.media_search_pending_confirmations
      for all
      using (auth.jwt() ->> 'role' = 'service_role');
  end if;
end;
$$;
