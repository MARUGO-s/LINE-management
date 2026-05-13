-- プレビューのみ: 誤 OCR「CAVA CAVA BISTRO」（正は BISTRO CAVA CAVA / store_partition_key bistrocavacava）
-- 正しい店の行には一致しません（cavacavabistro ≠ bistrocavacava）。

select
  id,
  line_message_id,
  room_id,
  store_name,
  store_partition_key,
  receipt_date,
  gross_sales_yen,
  created_at
from public.line_receipt_entries
where store_partition_key = 'cavacavabistro'
  and trim(coalesce(store_name, '')) = 'CAVA CAVA BISTRO'
  and receipt_date = date '2026-05-13'
order by created_at desc;
