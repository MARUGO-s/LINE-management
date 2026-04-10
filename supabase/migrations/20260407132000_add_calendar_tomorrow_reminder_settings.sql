ALTER TABLE public.summary_settings
ADD COLUMN IF NOT EXISTS calendar_tomorrow_reminder_enabled boolean NOT NULL DEFAULT true,
ADD COLUMN IF NOT EXISTS calendar_tomorrow_reminder_hours integer[] NOT NULL DEFAULT '{19}',
ADD COLUMN IF NOT EXISTS calendar_tomorrow_reminder_only_if_events boolean NOT NULL DEFAULT false,
ADD COLUMN IF NOT EXISTS calendar_tomorrow_reminder_max_items integer NOT NULL DEFAULT 20;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'summary_settings_calendar_tomorrow_reminder_max_items_check'
      AND conrelid = 'public.summary_settings'::regclass
  ) THEN
    ALTER TABLE public.summary_settings
      ADD CONSTRAINT summary_settings_calendar_tomorrow_reminder_max_items_check
      CHECK (calendar_tomorrow_reminder_max_items >= 1 AND calendar_tomorrow_reminder_max_items <= 50);
  END IF;
END $$;

UPDATE public.summary_settings
SET
  calendar_tomorrow_reminder_enabled = COALESCE(calendar_tomorrow_reminder_enabled, true),
  calendar_tomorrow_reminder_hours = CASE
    WHEN calendar_tomorrow_reminder_hours IS NULL OR array_length(calendar_tomorrow_reminder_hours, 1) IS NULL THEN '{19}'::integer[]
    ELSE calendar_tomorrow_reminder_hours
  END,
  calendar_tomorrow_reminder_only_if_events = COALESCE(calendar_tomorrow_reminder_only_if_events, false),
  calendar_tomorrow_reminder_max_items = COALESCE(calendar_tomorrow_reminder_max_items, 20)
WHERE id = 1;
