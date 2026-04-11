alter table public.room_summary_settings
add column if not exists media_file_access_enabled boolean not null default true;

update public.room_summary_settings
set media_file_access_enabled = true
where media_file_access_enabled is null;
