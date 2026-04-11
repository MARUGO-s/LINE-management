alter table public.room_summary_settings
add column if not exists message_search_library_enabled boolean not null default true;

update public.room_summary_settings
set message_search_library_enabled = true
where message_search_library_enabled is null;
