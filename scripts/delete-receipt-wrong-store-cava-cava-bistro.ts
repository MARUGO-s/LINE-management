#!/usr/bin/env -S deno run --allow-net --allow-env
/**
 * OCR 揺れで誤保存された「CAVA CAVA BISTRO」行のみを削除する（正店は BISTRO CAVA CAVA / bistrocavacava）。
 *
 * 削除条件（すべて一致する行のみ。取り違え防止のため固定文字列）:
 * - store_name が **完全一致** `CAVA CAVA BISTRO`
 * - store_partition_key が **完全一致** `cavacavabistro`（normalizeStoreToken 由来）
 * - 既定: receipt_date が `2026-05-13`（スクリーンショットの営業日。別日の誤登録を除外）
 *
 * 処理内容は cleanup-receipt-by-line-message-id.ts と同型:
 * Storage オブジェクト削除 → line_messages 削除（CASCADE で line_receipt_entries / line_message_media 等）
 *
 * 使い方:
 *   export SUPABASE_URL="https://xxxx.supabase.co"
 *   export SUPABASE_SERVICE_ROLE_KEY="eyJ..."
 *   # まず対象一覧のみ
 *   deno run --allow-net --allow-env scripts/delete-receipt-wrong-store-cava-cava-bistro.ts
 *   # 問題なければ実行
 *   deno run --allow-net --allow-env scripts/delete-receipt-wrong-store-cava-cava-bistro.ts --execute
 *
 * 営業日フィルタを外す場合（他日付の同一誤表記も消す）:
 *   deno run --allow-net --allow-env scripts/delete-receipt-wrong-store-cava-cava-bistro.ts --execute --any-receipt-date
 */
import { createClient } from 'https://esm.sh/@supabase/supabase-js@2.49.1'

const BAD_STORE_NAME = 'CAVA CAVA BISTRO'
const BAD_STORE_PARTITION_KEY = 'cavacavabistro'
/** ユーザー提示レシートの営業日。--any-receipt-date で無効化 */
const DEFAULT_RECEIPT_DATE = '2026-05-13'

const execute = Deno.args.includes('--execute')
const anyReceiptDate = Deno.args.includes('--any-receipt-date')

const url = Deno.env.get('SUPABASE_URL')?.trim()
const key = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')?.trim()
if (!url || !key) {
  console.error('Set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY')
  Deno.exit(1)
}

const supabase = createClient(url, key)

async function deleteOneReceiptByLineMessageId(lineMessageId: string): Promise<void> {
  const { data: media, error: mediaErr } = await supabase
    .from('line_message_media')
    .select('message_id, storage_bucket, storage_path')
    .eq('line_message_id', lineMessageId)
    .maybeSingle()

  if (mediaErr) throw new Error(`line_message_media: ${mediaErr.message}`)

  let messageId: string | null = media?.message_id ? String(media.message_id) : null

  if (!messageId) {
    const { data: rec, error: recErr } = await supabase
      .from('line_receipt_entries')
      .select('message_id')
      .eq('line_message_id', lineMessageId)
      .maybeSingle()
    if (recErr) throw new Error(`line_receipt_entries: ${recErr.message}`)
    if (rec?.message_id) messageId = String(rec.message_id)
  }

  if (!messageId) {
    throw new Error(`No message_id for line_message_id=${lineMessageId}`)
  }

  if (media?.storage_bucket && media?.storage_path) {
    const bucket = String(media.storage_bucket)
    const path = String(media.storage_path)
    const { error: stErr } = await supabase.storage.from(bucket).remove([path])
    if (stErr) throw new Error(`Storage remove: ${stErr.message}`)
    console.log(`  storage removed ${bucket}/${path}`)
  } else {
    console.warn(`  no line_message_media row for ${lineMessageId}; skip storage`)
  }

  const { error: delErr } = await supabase.from('line_messages').delete().eq('id', messageId)
  if (delErr) throw new Error(`line_messages delete: ${delErr.message}`)
  console.log(`  deleted line_messages id=${messageId}`)
}

let q = supabase
  .from('line_receipt_entries')
  .select('id, line_message_id, room_id, store_name, store_partition_key, receipt_date, gross_sales_yen, created_at')
  .eq('store_partition_key', BAD_STORE_PARTITION_KEY)
  .eq('store_name', BAD_STORE_NAME)

if (!anyReceiptDate) {
  q = q.eq('receipt_date', DEFAULT_RECEIPT_DATE)
}

const { data: rows, error } = await q.order('created_at', { ascending: false })

if (error) {
  console.error('select failed:', error.message)
  Deno.exit(1)
}

const list = Array.isArray(rows) ? rows : []

if (list.length === 0) {
  console.log(
    '該当行はありません（条件: store_name=CAVA CAVA BISTRO AND store_partition_key=cavacavabistro' +
      (anyReceiptDate ? '' : ` AND receipt_date=${DEFAULT_RECEIPT_DATE}`) +
      '）。',
  )
  Deno.exit(0)
}

console.log(`ヒット ${list.length} 件（dry-run=${!execute}）:`)
for (const r of list) {
  console.log(
    `  id=${(r as any).id} line_message_id=${(r as any).line_message_id} room=${(r as any).room_id} receipt_date=${(r as any).receipt_date} gross=${(r as any).gross_sales_yen} created_at=${(r as any).created_at}`,
  )
}

if (!execute) {
  console.log('\n削除するには同じコマンドに --execute を付けて再実行してください。')
  Deno.exit(0)
}

for (const r of list) {
  const lmid = String((r as any).line_message_id ?? '').trim()
  if (!lmid) {
    console.error('skip row missing line_message_id', r)
    continue
  }
  console.log(`\ndelete line_message_id=${lmid} ...`)
  try {
    await deleteOneReceiptByLineMessageId(lmid)
  } catch (e) {
    console.error(`FAILED line_message_id=${lmid}:`, (e as Error).message)
    Deno.exit(1)
  }
}

console.log('\nDone.（BISTRO CAVA CAVA / bistrocavacava の行には触れていません）')
