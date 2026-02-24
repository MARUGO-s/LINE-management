-- Create summary_settings table
CREATE TABLE IF NOT EXISTS public.summary_settings (
    id bigint PRIMARY KEY DEFAULT 1,
    delivery_hours integer[] NOT NULL DEFAULT '{12, 17, 23}',
    is_enabled boolean NOT NULL DEFAULT true,
    updated_at timestamp with time zone DEFAULT now(),
    CONSTRAINT one_row_only CHECK (id = 1)
);

-- Create room_summary_settings table
CREATE TABLE IF NOT EXISTS public.room_summary_settings (
    room_id text PRIMARY KEY,
    room_name text,
    delivery_hours integer[] DEFAULT NULL, -- NULL means follow global settings
    is_enabled boolean NOT NULL DEFAULT true,
    updated_at timestamp with time zone DEFAULT now()
);

-- Enable RLS
ALTER TABLE public.summary_settings ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.room_summary_settings ENABLE ROW LEVEL SECURITY;

-- Admin policies
-- Simplification: For now, we allow the service_role to do everything.
-- Admin access from the frontend will be handled via RPC or bypassing RLS if needed,
-- as the profiles table is not in this database.
CREATE POLICY "Service role can do everything on summary_settings"
    ON public.summary_settings
    FOR ALL
    USING (auth.jwt() ->> 'role' = 'service_role');

CREATE POLICY "Service role can do everything on room_summary_settings"
    ON public.room_summary_settings
    FOR ALL
    USING (auth.jwt() ->> 'role' = 'service_role');

-- Initial data
INSERT INTO public.summary_settings (id, delivery_hours, is_enabled)
VALUES (1, '{12, 17, 23}', true)
ON CONFLICT (id) DO NOTHING;

-- Change cron schedule to run every hour
-- This will cause the summary-cron Edge Function to be invoked every hour.
-- The function itself will check these settings to decide whether to send a summary.
SELECT cron.unschedule('summary-cron-job');
SELECT cron.schedule(
  'summary-cron-job',
  '0 * * * *',
  $$ select public.invoke_summary_cron(); $$
);
