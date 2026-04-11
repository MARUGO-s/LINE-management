ALTER TABLE public.calendar_update_pending_targets
ADD COLUMN IF NOT EXISTS source_line_message_ids_json jsonb NOT NULL DEFAULT '[]'::jsonb;

