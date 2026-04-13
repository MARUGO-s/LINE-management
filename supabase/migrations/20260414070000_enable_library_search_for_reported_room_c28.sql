-- Targeted fix for reported room:
-- Keep conversation search disabled, but ensure library search is enabled.
update public.room_summary_settings
set
  message_search_enabled = false,
  message_search_library_enabled = true,
  updated_at = now()
where room_id = 'C28ab07fbad8ef16cf1b2584add2e1f38';
