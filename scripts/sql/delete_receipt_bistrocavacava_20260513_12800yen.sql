-- BISTRO CAVA CAVA / 2026-05-13 / 税込売上 12,800 円 のレシート1件を削除する（再入力用）
-- 実行先: 本番プロジェクトの Supabase Dashboard → SQL Editor
--
-- line_receipt_entries / line_message_media は line_messages に ON DELETE CASCADE があるため、
-- 親の line_messages を消せば子は消える。Storage の画像は CASCADE されないので手順 2 が必須。
--
-- ━━━ 1) プレビュー（必ず先に実行） ━━━
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
  and e.receipt_date = date '2026-05-13'
  and e.gross_sales_yen = 12800
order by e.created_at desc;

-- ━━━ 2) Storage 削除 ━━━
-- Dashboard → Storage → 上で得た storage_bucket（多くは line-media）→ storage_path のファイルを削除

-- ━━━ 3) DB 削除（プレビューがちょうど 1 行のときだけ。複数行なら例外で止まる） ━━━
do $$
declare
  n int;
  mid uuid;
begin
  select count(*) into n
  from public.line_receipt_entries
  where store_partition_key = 'bistrocavacava'
    and receipt_date = date '2026-05-13'
    and gross_sales_yen = 12800;

  if n = 0 then
    raise exception '該当する line_receipt_entries がありません（店キー・日付・税込売上を確認してください）';
  end if;

  if n > 1 then
    raise exception '該当が % 件あります。line_message_id で絞るか、不要行を確認してからスクリプトを修正してください。', n;
  end if;

  select message_id into mid
  from public.line_receipt_entries
  where store_partition_key = 'bistrocavacava'
    and receipt_date = date '2026-05-13'
    and gross_sales_yen = 12800
  order by created_at desc
  limit 1;

  delete from public.line_messages where id = mid;
  raise notice 'Deleted line_messages id=%', mid;
end $$;
