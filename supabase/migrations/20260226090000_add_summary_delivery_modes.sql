ALTER TABLE public.summary_settings
ADD COLUMN IF NOT EXISTS message_cleanup_timing text NOT NULL DEFAULT 'after_each_delivery',
ADD COLUMN IF NOT EXISTS last_delivery_summary_mode text NOT NULL DEFAULT 'independent';

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'summary_settings_message_cleanup_timing_check'
      AND conrelid = 'public.summary_settings'::regclass
  ) THEN
    ALTER TABLE public.summary_settings
      ADD CONSTRAINT summary_settings_message_cleanup_timing_check
      CHECK (message_cleanup_timing IN ('after_each_delivery', 'end_of_day'));
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'summary_settings_last_delivery_summary_mode_check'
      AND conrelid = 'public.summary_settings'::regclass
  ) THEN
    ALTER TABLE public.summary_settings
      ADD CONSTRAINT summary_settings_last_delivery_summary_mode_check
      CHECK (last_delivery_summary_mode IN ('independent', 'daily_rollup'));
  END IF;
END $$;

