ALTER TABLE public.summary_settings
ADD COLUMN IF NOT EXISTS admin_dashboard_token_hash text,
ADD COLUMN IF NOT EXISTS admin_dashboard_token_updated_at timestamptz;
