-- LINE送信通数（概算）をJST月次で確認するための集計SQLです。
-- 注意:
-- - ここでの件数は「送信ログ行数ベース」の概算です。
-- - LINEの正式な課金通数はLINE側の quota/consumption API を参照してください。

-- 1) 今月（JST）の push 送信成功件数（機能別）
WITH jst_bounds AS (
  SELECT
    date_trunc('month', timezone('Asia/Tokyo', now())) AS month_start_jst,
    date_trunc('month', timezone('Asia/Tokyo', now())) + interval '1 month' AS month_end_jst
),
webhook_push AS (
  SELECT
    'line-webhook'::text AS source,
    COALESCE(NULLIF(context, ''), 'unknown') AS context,
    created_at AS event_at_utc
  FROM public.line_webhook_delivery_logs, jst_bounds
  WHERE method = 'push'
    AND line_send_success = true
    AND timezone('Asia/Tokyo', created_at) >= month_start_jst
    AND timezone('Asia/Tokyo', created_at) < month_end_jst
),
cron_push AS (
  SELECT
    COALESCE(NULLIF(details->>'source', ''), 'summary_delivery_logs') AS source,
    COALESCE(NULLIF(details->>'context', ''), COALESCE(NULLIF(reason, ''), 'unknown')) AS context,
    run_at AS event_at_utc
  FROM public.summary_delivery_logs, jst_bounds
  WHERE line_send_attempted = true
    AND line_send_success = true
    AND timezone('Asia/Tokyo', run_at) >= month_start_jst
    AND timezone('Asia/Tokyo', run_at) < month_end_jst
),
all_push AS (
  SELECT * FROM webhook_push
  UNION ALL
  SELECT * FROM cron_push
)
SELECT
  source,
  context,
  count(*)::bigint AS sent_rows
FROM all_push
GROUP BY source, context
ORDER BY sent_rows DESC, source, context;

-- 2) 今月（JST）の push 日次推移
WITH jst_bounds AS (
  SELECT
    date_trunc('month', timezone('Asia/Tokyo', now())) AS month_start_jst,
    date_trunc('month', timezone('Asia/Tokyo', now())) + interval '1 month' AS month_end_jst
),
all_push AS (
  SELECT created_at AS event_at_utc
  FROM public.line_webhook_delivery_logs, jst_bounds
  WHERE method = 'push'
    AND line_send_success = true
    AND timezone('Asia/Tokyo', created_at) >= month_start_jst
    AND timezone('Asia/Tokyo', created_at) < month_end_jst
  UNION ALL
  SELECT run_at AS event_at_utc
  FROM public.summary_delivery_logs, jst_bounds
  WHERE line_send_attempted = true
    AND line_send_success = true
    AND timezone('Asia/Tokyo', run_at) >= month_start_jst
    AND timezone('Asia/Tokyo', run_at) < month_end_jst
)
SELECT
  to_char(timezone('Asia/Tokyo', event_at_utc)::date, 'YYYY-MM-DD') AS jst_date,
  count(*)::bigint AS sent_rows
FROM all_push
GROUP BY timezone('Asia/Tokyo', event_at_utc)::date
ORDER BY jst_date;
