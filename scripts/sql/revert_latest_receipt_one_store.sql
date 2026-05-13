-- 直近に登録されたレシート1件分を消し、月次集計を「その1件前」に戻す手順
-- （line_receipt_entries / line_message_media は line_messages の CASCADE で消える）
-- Storage 上の画像は CASCADE されないため、手順 2 を必ず行うこと。
--
-- 対象店: BISTRO CAVA CAVA → store_partition_key は多くの環境で bistrocavacava
-- 別店のテストなら下の WHERE の店舗キーを変えるか、プレビュー用のコメント内「全店」を使う。

-- ━━━ 1) プレビュー（必ず先に実行して内容を確認） ━━━
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
order by e.created_at desc
limit 5;

-- 別店・不明な場合は店舗条件なしで直近を見る:
-- select e.id, e.line_message_id, e.message_id, e.store_partition_key, e.gross_sales_yen, e.created_at
-- from public.line_receipt_entries e
-- order by e.created_at desc
-- limit 10;

-- ━━━ 2) Storage 削除 ━━━
-- Dashboard → Storage → 上で得た storage_bucket（多くは line-media）→ storage_path のファイルを削除
-- または docs/ops/CLEANUP_TEST_RECEIPT.md の Deno スクリプトで line_message_id を指定して削除

-- ━━━ 3) DB から親メッセージを削除（確認後に1回だけ実行） ━━━
-- begin;
-- delete from public.line_messages
-- where id = (
--   select message_id
--   from public.line_receipt_entries
--   where store_partition_key = 'bistrocavacava'
--   order by created_at desc
--   limit 1
-- );
-- commit;

-- 店舗を絞らず「全体で最新の1件」だけ消す場合（他ルームの投稿が混ざると危険）:
-- begin;
-- delete from public.line_messages
-- where id = (
--   select message_id from public.line_receipt_entries order by created_at desc limit 1
-- );
-- commit;
