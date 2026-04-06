ALTER TABLE public.room_summary_settings
ADD COLUMN IF NOT EXISTS calendar_tomorrow_reminder_enabled boolean NOT NULL DEFAULT false;
