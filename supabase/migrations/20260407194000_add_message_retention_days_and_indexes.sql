ALTER TABLE public.summary_settings
ADD COLUMN IF NOT EXISTS message_retention_days integer;

UPDATE public.summary_settings
SET message_retention_days = 60
WHERE message_retention_days IS NULL
   OR message_retention_days NOT IN (60, 120, 180);

ALTER TABLE public.summary_settings
ALTER COLUMN message_retention_days SET DEFAULT 60;

ALTER TABLE public.summary_settings
ALTER COLUMN message_retention_days SET NOT NULL;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'summary_settings_message_retention_days_check'
      AND conrelid = 'public.summary_settings'::regclass
  ) THEN
    ALTER TABLE public.summary_settings
      ADD CONSTRAINT summary_settings_message_retention_days_check
      CHECK (message_retention_days IN (60, 120, 180));
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS line_messages_created_at_idx
  ON public.line_messages (created_at);

CREATE INDEX IF NOT EXISTS line_messages_room_created_at_idx
  ON public.line_messages (room_id, created_at DESC);
