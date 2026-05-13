#!/usr/bin/env -S deno run --allow-net --allow-env
/**
 * テスト配信などで追加したレシート1件分を DB と Storage から削除する。
 *
 * 前提:
 * - line_receipt_entries / line_message_media は line_messages(id) に FK（ON DELETE CASCADE）
 * - Storage の実ファイルは CASCADE されないため、先に削除する
 *
 * 使い方:
 *   export SUPABASE_URL="https://xxxx.supabase.co"
 *   export SUPABASE_SERVICE_ROLE_KEY="eyJ..."  # service_role（Dashboard → Settings → API）
 *   deno run --allow-net --allow-env scripts/cleanup-receipt-by-line-message-id.ts <LINE_MESSAGE_ID>
 *
 * LINE_MESSAGE_ID は LINE から届く Webhook の message.id（画像メッセージの ID）です。
 * 不明な場合は docs/ops/CLEANUP_TEST_RECEIPT.md の SQL で直近行を確認してください。
 */
import { createClient } from 'https://esm.sh/@supabase/supabase-js@2.49.1'

const lineMessageId = Deno.args[0]?.trim()
if (!lineMessageId) {
  console.error('Usage: cleanup-receipt-by-line-message-id.ts <LINE_MESSAGE_ID>')
  Deno.exit(1)
}

const url = Deno.env.get('SUPABASE_URL')?.trim()
const key = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')?.trim()
if (!url || !key) {
  console.error('Set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY')
  Deno.exit(1)
}

const supabase = createClient(url, key)

const { data: media, error: mediaErr } = await supabase
  .from('line_message_media')
  .select('message_id, storage_bucket, storage_path')
  .eq('line_message_id', lineMessageId)
  .maybeSingle()

if (mediaErr) {
  console.error('line_message_media select failed:', mediaErr.message)
  Deno.exit(1)
}

let messageId: string | null = media?.message_id ? String(media.message_id) : null

if (!messageId) {
  const { data: rec, error: recErr } = await supabase
    .from('line_receipt_entries')
    .select('message_id')
    .eq('line_message_id', lineMessageId)
    .maybeSingle()
  if (recErr) {
    console.error('line_receipt_entries select failed:', recErr.message)
    Deno.exit(1)
  }
  if (rec?.message_id) messageId = String(rec.message_id)
}

if (!messageId) {
  console.error(`No line_message_media or line_receipt_entries for line_message_id=${lineMessageId}`)
  Deno.exit(1)
}

if (media?.storage_bucket && media?.storage_path) {
  const bucket = String(media.storage_bucket)
  const path = String(media.storage_path)
  const { error: stErr } = await supabase.storage.from(bucket).remove([path])
  if (stErr) {
    console.error('Storage remove failed:', stErr.message)
    Deno.exit(1)
  }
  console.log(`Removed storage: ${bucket}/${path}`)
} else {
  console.warn('No media row with storage_path; skipping storage delete')
}

const { error: delErr } = await supabase.from('line_messages').delete().eq('id', messageId)
if (delErr) {
  console.error('line_messages delete failed:', delErr.message)
  Deno.exit(1)
}

console.log(`Deleted line_messages id=${messageId} (cascades line_message_media, line_receipt_entries, receipt_correction_pending where applicable)`)
console.log('Done.')
