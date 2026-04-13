-- Force-fix: reported room had message_search_enabled=false despite UI expectation.
-- Keep this targeted to the reported room_id to avoid broad behavior changes.
update public.room_summary_settings
set
  message_search_enabled = true,
  message_search_library_enabled = true,
  updated_at = now()
where room_id = 'Ca141366d8e3934fec158c96237669b5f';
