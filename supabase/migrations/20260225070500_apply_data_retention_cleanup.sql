-- Immediate cleanup for retention policy:
-- 1) remove already processed LINE messages
-- 2) keep only latest 50 delivery logs

DELETE FROM public.line_messages
WHERE processed = true;

WITH cutoff AS (
  SELECT id
  FROM public.summary_delivery_logs
  ORDER BY id DESC
  OFFSET 49
  LIMIT 1
)
DELETE FROM public.summary_delivery_logs
WHERE id < (SELECT id FROM cutoff);
