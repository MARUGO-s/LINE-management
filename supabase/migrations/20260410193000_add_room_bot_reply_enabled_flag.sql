alter table public.room_summary_settings
add column if not exists bot_reply_enabled boolean not null default true;

update public.room_summary_settings
set bot_reply_enabled = true
where bot_reply_enabled is null;
