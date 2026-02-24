ALTER TABLE public.room_summary_settings
ADD COLUMN IF NOT EXISTS send_room_summary boolean NOT NULL DEFAULT false;
