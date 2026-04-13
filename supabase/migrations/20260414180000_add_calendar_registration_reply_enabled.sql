-- 無返信自動登録（高確度）で Google カレンダーに書き込んだあと、登録内容を LINE で返信するか
alter table public.room_summary_settings
  add column if not exists calendar_registration_reply_enabled boolean not null default false;

comment on column public.room_summary_settings.calendar_registration_reply_enabled is
  'When silent high-confidence auto-register runs, also send a LINE reply summarizing the created event (requires bot_reply_enabled). Independent of calendar_low_confidence_confirm_reply_enabled.';
