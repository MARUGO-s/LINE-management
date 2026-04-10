ALTER TABLE public.room_summary_settings
ADD COLUMN IF NOT EXISTS message_cleanup_timing text DEFAULT NULL,
ADD COLUMN IF NOT EXISTS last_delivery_summary_mode text DEFAULT NULL;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'room_summary_settings_message_cleanup_timing_check'
      AND conrelid = 'public.room_summary_settings'::regclass
  ) THEN
    ALTER TABLE public.room_summary_settings
      ADD CONSTRAINT room_summary_settings_message_cleanup_timing_check
      CHECK (
        message_cleanup_timing IS NULL
        OR message_cleanup_timing IN ('after_each_delivery', 'end_of_day')
      );
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'room_summary_settings_last_delivery_summary_mode_check'
      AND conrelid = 'public.room_summary_settings'::regclass
  ) THEN
    ALTER TABLE public.room_summary_settings
      ADD CONSTRAINT room_summary_settings_last_delivery_summary_mode_check
      CHECK (
        last_delivery_summary_mode IS NULL
        OR last_delivery_summary_mode IN ('independent', 'daily_rollup')
      );
  END IF;
END $$;

