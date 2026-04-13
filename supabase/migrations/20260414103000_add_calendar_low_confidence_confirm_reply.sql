-- 無返信自動登録ONのルームで、低確度時に LINE で確認返信（はい／いいえ）を送るか
alter table public.room_summary_settings
  add column if not exists calendar_low_confidence_confirm_reply_enabled boolean not null default false;

comment on column public.room_summary_settings.calendar_low_confidence_confirm_reply_enabled is
  'When silent auto-register is on: true = low-confidence uses LINE confirmation (pending); false = provisional title only.';
