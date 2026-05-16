# テスト配信レシートの削除（本番データを元に戻す）

レシート画像の解析で追加される主なデータは次のとおりです。

| 保存先 | 内容 |
|--------|------|
| `storage`（通常 `line-media` バケット） | 画像ファイル |
| `line_message_media` | メタデータ・`storage_path` |
| `line_messages` | メディア保存用の行（グループ／1:1 でメディア保存時） |
| `line_receipt_entries` | 解析された売上・店舗・日付など |

`line_receipt_entries` と `line_message_media` は `line_messages.id` に **`ON DELETE CASCADE`** があるため、**親の `line_messages` を削除**すれば子レコードは自動削除されます。  
**Storage のファイルだけは CASCADE されない**ため、必ず先にオブジェクト削除が必要です（下記スクリプトが両方行います）。

---

## 1. 削除対象の `line_message_id` を特定する

LINE の Messaging API 上の **メッセージ ID**（Webhook の `events[].message.id`）です。

### SQL（Supabase Dashboard → SQL Editor）

直近に登録されたレシート数件:

```sql
select line_message_id, room_id, store_name, receipt_date, gross_sales_yen, created_at
from public.line_receipt_entries
order by created_at desc
limit 10;
```

対応するメディア行:

```sql
select line_message_id, room_id, storage_bucket, storage_path, created_at
from public.line_message_media
order by created_at desc
limit 10;
```

テストで分かっている **`line_message_id` 1 件** をメモします。

---

## 2. 誤店名「CAVA CAVA BISTRO」だけを消す（取り違え防止）

正店 **BISTRO CAVA CAVA**（`store_partition_key = bistrocavacava`）には触れません。対象は次の **すべて一致** です。

- `store_name` = `CAVA CAVA BISTRO`（完全一致）
- `store_partition_key` = `cavacavabistro`
- 既定: `receipt_date` = `2026-05-13`（他日付も消す場合はスクリプトの `--any-receipt-date`）

プレビュー SQL: [`scripts/sql/preview_receipt_wrong_store_cava_cava_bistro.sql`](../../scripts/sql/preview_receipt_wrong_store_cava_cava_bistro.sql)

削除スクリプト（まず dry-run、問題なければ `--execute`）:

```bash
export SUPABASE_URL="https://<project-ref>.supabase.co"
export SUPABASE_SERVICE_ROLE_KEY="<service_role>"

deno run --allow-net --allow-env scripts/delete-receipt-wrong-store-cava-cava-bistro.ts
deno run --allow-net --allow-env scripts/delete-receipt-wrong-store-cava-cava-bistro.ts --execute
```

## 3. スクリプトで削除（推奨・`line_message_id` が分かる場合）

`@supabase/supabase-js` を使う **Deno ワンショット**（Storage と DB の両方を確実に処理します）。

リポジトリルートで:

```bash
export SUPABASE_URL="https://<project-ref>.supabase.co"
export SUPABASE_SERVICE_ROLE_KEY="<service_role シークレット>"

# Deno 未導入の場合: https://docs.deno.com/runtime/getting_started/installation/
deno run --allow-net --allow-env scripts/cleanup-receipt-by-line-message-id.ts "<LINE_MESSAGE_ID>"
```

- Storage の該当ファイルを削除したうえで、`line_messages` を削除します。
- `line_receipt_entries`・`line_message_media`・`receipt_correction_pending_confirmations`（該当レシートに紐づくもの）は CASCADE で削除されます。

Deno を入れたくない場合は、下記 **4. 手動** の手順だけでも同じ結果にできます。

---

## 4. 手動で戻す場合（Dashboard）

1. **SQL** で `line_message_media` から `storage_bucket` / `storage_path` を取得。
2. **Storage** → バケット（多くは `line-media`）→ 該当パスのファイルを削除。
3. **SQL** で `line_messages` を削除:

```sql
-- message_id は line_message_media.message_id または line_receipt_entries.message_id
delete from public.line_messages
where id = '<uuid>';
```

（`line_message_id` だけ分かる場合）

```sql
delete from public.line_messages
where id = (
  select message_id from public.line_message_media where line_message_id = '<LINE_MESSAGE_ID>' limit 1
);
```

---

## 5. 予算・手入力売上について

テスト中に **analytics の月間予算・店舗休日・手入力前年** を変更した場合、それらは **`line_sales_month_budgets` / `line_sales_month_store_closed_days` / `line_sales_manual_month_gross`** に残ります。レシート削除だけでは戻りません。**必要なら管理画面から元の値に戻す**か、該当行を SQL で修正してください。

**日次予算差の見え方**（早朝に当日差が 0 など）は **進行日・按分待ち（JST 5:00）**のためです。詳細は **`docs/RECEIPT_ANALYSIS_POLICY.md` 8.0** を参照。

---

## 6. 月中・月間レポートの送信ログについて

本番の中間・月間 LINE レポート送信時に `line_receipt_mid_reports` に行が追加されることがあります（スケジュールは **`docs/RECEIPT_LINE_SALES_REPORT.md`** 参照: 中間＝16日10:00、月間＝翌月1日10:00）。

テストで増えた行だけ消す場合は `room_id` と `report_month` と `report_kind` で特定して削除してください。管理画面の「テスト送信」はこのテーブルに**書き込みません**。
