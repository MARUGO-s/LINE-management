alter table public.line_user_permissions
  add column if not exists can_calendar_view boolean not null default true;

comment on column public.line_user_permissions.can_calendar_view is 'カレンダー予定の一覧・確認・閲覧（検索）を許可';
