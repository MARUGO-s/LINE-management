alter table public.calendar_pending_confirmations
add column if not exists location text;
