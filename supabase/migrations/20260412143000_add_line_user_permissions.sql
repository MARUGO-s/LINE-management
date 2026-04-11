create table if not exists public.line_user_permissions (
  id bigserial primary key,
  line_user_id text not null unique,
  display_name text null,
  is_active boolean not null default true,
  can_message_search boolean not null default true,
  can_library_search boolean not null default true,
  can_calendar_create boolean not null default true,
  can_calendar_update boolean not null default true,
  can_media_access boolean not null default true,
  note text null,
  updated_at timestamptz not null default now()
);

alter table public.line_user_permissions enable row level security;

do $$
begin
  if not exists (
    select 1
    from pg_policies
    where schemaname = 'public'
      and tablename = 'line_user_permissions'
      and policyname = 'Service role can do everything on line_user_permissions'
  ) then
    create policy "Service role can do everything on line_user_permissions"
      on public.line_user_permissions
      for all
      to public
      using ((auth.jwt() ->> 'role') = 'service_role')
      with check ((auth.jwt() ->> 'role') = 'service_role');
  end if;
end
$$;

create index if not exists idx_line_user_permissions_updated_at
  on public.line_user_permissions (updated_at desc);
