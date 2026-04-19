create table if not exists public.receipt_correction_pending_confirmations (
  id uuid primary key default gen_random_uuid(),
  conversation_key text not null unique,
  room_id text not null,
  user_id text,
  receipt_entry_id bigint not null references public.line_receipt_entries(id) on delete cascade,
  stage text not null default 'select_field' check (stage in ('select_field', 'input_value')),
  current_field_key text check (
    current_field_key in ('storeName', 'date', 'netSales', 'taxAmount', 'grossSales', 'partyCount', 'guestCount', 'unitPrice')
  ),
  draft_json jsonb not null default '{}'::jsonb,
  expires_at timestamptz not null,
  updated_at timestamptz not null default now(),
  created_at timestamptz not null default now()
);

create index if not exists receipt_correction_pending_expires_idx
  on public.receipt_correction_pending_confirmations (expires_at desc);

alter table public.receipt_correction_pending_confirmations enable row level security;

do $$
begin
  if not exists (
    select 1
    from pg_policies
    where schemaname = 'public'
      and tablename = 'receipt_correction_pending_confirmations'
      and policyname = 'Service role can do everything on receipt_correction_pending_confirmations'
  ) then
    create policy "Service role can do everything on receipt_correction_pending_confirmations"
      on public.receipt_correction_pending_confirmations
      for all
      using (auth.jwt() ->> 'role' = 'service_role');
  end if;
end;
$$;
