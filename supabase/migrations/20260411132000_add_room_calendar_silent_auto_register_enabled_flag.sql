alter table public.room_summary_settings
add column if not exists calendar_silent_auto_register_enabled boolean not null default false;

update public.room_summary_settings
set calendar_silent_auto_register_enabled = false
where calendar_silent_auto_register_enabled is null;
