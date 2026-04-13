create table if not exists public.haccp_bulk_pending_confirmations (
  id uuid primary key default gen_random_uuid(),
  conversation_key text not null unique,
  room_id text not null,
  user_id text,
  line_message_id text not null,
  sender_display_name text,
  original_file_name text,
  items_json jsonb not null default '[]'::jsonb,
  expires_at timestamptz not null,
  updated_at timestamptz not null default now(),
  created_at timestamptz not null default now()
);

create index if not exists haccp_bulk_pending_expires_idx
  on public.haccp_bulk_pending_confirmations (expires_at desc);

alter table public.haccp_bulk_pending_confirmations enable row level security;

do $$
begin
  if not exists (
    select 1
    from pg_policies
    where schemaname = 'public'
      and tablename = 'haccp_bulk_pending_confirmations'
      and policyname = 'Service role can do everything on haccp_bulk_pending_confirmations'
  ) then
    create policy "Service role can do everything on haccp_bulk_pending_confirmations"
      on public.haccp_bulk_pending_confirmations
      for all
      using (auth.jwt() ->> 'role' = 'service_role');
  end if;
end;
$$;
