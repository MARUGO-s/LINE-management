-- 会話検索: DB 側でキーワード（トークン AND・各トークンは類義語 OR）を事前絞り込みし、Edge への転送量を抑える。
CREATE INDEX IF NOT EXISTS line_messages_created_at_desc_idx
  ON public.line_messages (created_at DESC);

CREATE OR REPLACE FUNCTION public.escape_like_pattern_fragment(p_text text)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT replace(replace(replace(COALESCE(p_text, ''), E'\\', E'\\\\'), '%', E'\\%'), '_', E'\\_');
$$;

CREATE OR REPLACE FUNCTION public.search_line_messages_keyword_window(
  p_since timestamptz,
  p_before timestamptz,
  p_room_id text,
  p_all_rooms boolean,
  p_exclude_room_ids text[],
  p_token_or_groups jsonb,
  p_max_rows int
)
RETURNS TABLE (
  room_id text,
  content text,
  created_at timestamptz,
  user_id text
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public
AS $$
  SELECT m.room_id, m.content, m.created_at, m.user_id
  FROM public.line_messages m
  WHERE
    (p_since IS NULL OR m.created_at >= p_since)
    AND (p_before IS NULL OR m.created_at < p_before)
    AND (
      (p_all_rooms IS TRUE
        AND (p_exclude_room_ids IS NULL OR array_length(p_exclude_room_ids, 1) IS NULL
          OR NOT (m.room_id = ANY (p_exclude_room_ids))))
      OR (COALESCE(p_all_rooms, FALSE) IS FALSE AND m.room_id = p_room_id)
    )
    AND p_token_or_groups IS NOT NULL
    AND jsonb_typeof(p_token_or_groups) = 'array'
    AND jsonb_array_length(p_token_or_groups) > 0
    AND NOT EXISTS (
      SELECT 1
      FROM jsonb_array_elements(p_token_or_groups) AS g(elem)
      WHERE NOT EXISTS (
        SELECT 1
        FROM jsonb_array_elements_text(g.elem) AS pat(pat)
        WHERE m.content ILIKE (
          '%' || public.escape_like_pattern_fragment(pat) || '%'
        ) ESCAPE '\'
      )
    )
  ORDER BY m.created_at DESC
  LIMIT LEAST(GREATEST(COALESCE(p_max_rows, 1), 1), 200000);
$$;

REVOKE ALL ON FUNCTION public.search_line_messages_keyword_window(
  timestamptz, timestamptz, text, boolean, text[], jsonb, int
) FROM PUBLIC;

GRANT EXECUTE ON FUNCTION public.search_line_messages_keyword_window(
  timestamptz, timestamptz, text, boolean, text[], jsonb, int
) TO service_role;
