-- 誤って 2026-05-14 に入った BISTRO CAVA CAVA / 税込 12,800 円 の1件を削除する（13日分と重複した行の整理用）
-- 実行先: Supabase Dashboard → SQL Editor
--
-- 手順は delete_receipt_bistrocavacava_20260513_12800yen.sql と同じ（プレビュー → Storage → DO ブロック）

-- ━━━ 1) プレビュー ━━━
select
  e.id,
  e.line_message_id,
  e.message_id,
  e.room_id,
  e.store_name,
  e.store_partition_key,
  e.receipt_date,
  e.gross_sales_yen,
  e.created_at,
  m.storage_bucket,
  m.storage_path
from public.line_receipt_entries e
left join public.line_message_media m on m.line_message_id = e.line_message_id
where e.store_partition_key = 'bistrocavacava'
  and e.receipt_date = date '2026-05-14'
  and e.gross_sales_yen = 12800
order by e.created_at desc;

-- ━━━ 2) Storage で上記 storage_path を削除 ━━━

-- ━━━ 3) DB 削除（ちょうど 1 件のときのみ） ━━━
do $$
declare
  n int;
  mid uuid;
begin
  select count(*) into n
  from public.line_receipt_entries
  where store_partition_key = 'bistrocavacava'
    and receipt_date = date '2026-05-14'
    and gross_sales_yen = 12800;

  if n = 0 then
    raise exception '該当する line_receipt_entries がありません';
  end if;

  if n > 1 then
    raise exception '該当が % 件あります。プレビューで line_message_id を確認し、cleanup スクリプトで削除してください。', n;
  end if;

  select message_id into mid
  from public.line_receipt_entries
  where store_partition_key = 'bistrocavacava'
    and receipt_date = date '2026-05-14'
    and gross_sales_yen = 12800
  order by created_at desc
  limit 1;

  delete from public.line_messages where id = mid;
  raise notice 'Deleted line_messages id=%', mid;
end $$;
