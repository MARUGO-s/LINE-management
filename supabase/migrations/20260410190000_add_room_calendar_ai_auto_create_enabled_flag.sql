alter table public.room_summary_settings
add column if not exists calendar_ai_auto_create_enabled boolean not null default true;

update public.room_summary_settings
set calendar_ai_auto_create_enabled = true
where calendar_ai_auto_create_enabled is null;
