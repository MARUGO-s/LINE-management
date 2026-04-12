-- User job title (dropdown in admin UI).

alter table public.line_user_permissions
  add column if not exists assigned_job_title text null;
