-- Replace free-text memo with assigned store label (dropdown in admin UI).

alter table public.line_user_permissions
  drop column if exists note;

alter table public.line_user_permissions
  add column if not exists assigned_store text null;
