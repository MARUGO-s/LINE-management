alter table if exists public.room_summary_settings
add column if not exists bot_reply_hard_mute_enabled boolean not null default false;
