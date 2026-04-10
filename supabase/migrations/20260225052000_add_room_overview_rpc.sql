CREATE OR REPLACE FUNCTION public.get_room_overview()
RETURNS TABLE (
    room_id text,
    room_name text,
    total_messages bigint,
    pending_messages bigint,
    last_message_at timestamp with time zone,
    last_pending_at timestamp with time zone,
    settings_enabled boolean,
    settings_delivery_hours integer[],
    settings_updated_at timestamp with time zone
)
LANGUAGE sql
SECURITY DEFINER
SET search_path = public
AS $$
WITH message_stats AS (
    SELECT
        lm.room_id,
        count(*)::bigint AS total_messages,
        count(*) FILTER (WHERE lm.processed = false)::bigint AS pending_messages,
        max(lm.created_at) AS last_message_at,
        max(lm.created_at) FILTER (WHERE lm.processed = false) AS last_pending_at
    FROM public.line_messages lm
    GROUP BY lm.room_id
),
all_room_ids AS (
    SELECT room_id FROM message_stats
    UNION
    SELECT room_id FROM public.room_summary_settings
)
SELECT
    r.room_id,
    coalesce(rs.room_name, r.room_id) AS room_name,
    coalesce(ms.total_messages, 0)::bigint AS total_messages,
    coalesce(ms.pending_messages, 0)::bigint AS pending_messages,
    ms.last_message_at,
    ms.last_pending_at,
    coalesce(rs.is_enabled, true) AS settings_enabled,
    rs.delivery_hours AS settings_delivery_hours,
    rs.updated_at AS settings_updated_at
FROM all_room_ids r
LEFT JOIN message_stats ms ON ms.room_id = r.room_id
LEFT JOIN public.room_summary_settings rs ON rs.room_id = r.room_id
ORDER BY coalesce(ms.last_message_at, rs.updated_at) DESC NULLS LAST, r.room_id ASC;
$$;
