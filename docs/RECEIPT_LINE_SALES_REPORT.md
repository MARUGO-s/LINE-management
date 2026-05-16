# 売上中間・月間 LINE レポート

レシート解析データ（`line_receipt_entries`）をもとに、ルームごとに **中間報告** と **月間報告** を LINE Flex で配信する機能の仕様です。

実装の中心:

- Edge Function: `receipt-midreport-cron`（本番送信）
- 共有: `supabase/functions/_shared/receipt_report_aggregate.ts`（集計）
- 共有: `supabase/functions/_shared/receipt_report_flex.ts`（Flex 組み立て）
- 共有: `supabase/functions/_shared/receipt_budget_comparison.ts`（【予算】行）
- 管理画面: ルーム設定の「集計する店舗」「売上中間／月末レポート」ON/OFF
- テスト送信: `admin-api` `POST /actions/test-receipt-report`

---

## 1. 送信スケジュール（JST）

pg_cron は **毎分** `receipt-midreport-cron` を呼び出します。関数内で **送信時刻と日付** を判定し、該当時のみ Push します。

| レポート | 送信日時 | 集計期間（`receipt_date`） |
|----------|----------|---------------------------|
| **中間報告** | **毎月 16 日 10:00** | 当月 **1 日〜15 日** |
| **月間報告** | **翌月 1 日 10:00** | **前月** 1 日〜末日 |

例（2026年5月分）:

- 中間 → **5/16 10:00**（5/1〜5/15 を集計）
- 月間 → **6/1 10:00**（5/1〜5/31 を集計）

### 営業日 5 時と送信 10 時の関係

- **予算・日次差・analytics の「進行日」** … **JST 5:00** で営業日が切り替わる（深夜営業店向け）。詳細は `docs/RECEIPT_ANALYSIS_POLICY.md` §8 および `RECEIPT_BUDGET_BUSINESS_DAY_START_HOUR_JST`（`sales_budget_allocation.ts`）。
- **LINE レポートの送信** … **10:00** 固定（早朝の通知を避ける）。

15 日深夜〜16 日 5 時未満の売上は「15 日の営業」として集計に含まれたうえで、**16 日 10 時**にまとめて通知されます。

### 16 日 10 時より早い送信（補助）

**16 日 10:00 以降**に、そのルームでレシート画像の保存／重複確認確定などが走り、かつ当月中間を未送信の場合、`line-webhook` から先に 1 通送ることがあります（`trigger_type: day15_post`）。**10 時前には送りません。**

---

## 2. ルーム設定

`room_summary_settings` の関連列:

| 列 | 説明 |
|----|------|
| `receipt_midreport_enabled` | 中間報告（既定 `true`） |
| `receipt_monthend_report_enabled` | 月間報告（既定 `true`） |
| `receipt_report_store_partition_key` | 集計店舗（`line_receipt_entries.store_partition_key`）。**NULL** のときはルーム名・当ルームのレシート履歴から推定 |

管理画面（`index.html` / `admin-ui`）のルーム設定モーダル:

- **集計する店舗** … analytics と同じ店舗キー（例: `bistrocavacava` → BISTRO CAVA CAVA）
- **中間／月末レポートをテスト送信** … 本番と同じ Flex。`line_receipt_mid_reports` には**書かない**

マイグレーション: `supabase/migrations/20260516150000_add_receipt_report_store_to_room_settings.sql`

---

## 3. 集計ロジック

### 3.1 店舗の決定順

`resolveStorePartitionKeyForRoom`（`receipt_report_aggregate.ts`）:

1. `receipt_report_store_partition_key`（DB 保存値）を最優先
2. ルーム名から既知店舗名を推定（`receipt_store_name_resolve.ts`）
3. 当ルーム直近レシートの最多 `store_partition_key`

テスト送信時は、画面で選んだ店舗を API 経由で `store_partition_key` クエリに渡せる（**保存前でも可**）。

### 3.2 期間と売上分析の整合

集計は **店舗 × レシート日付（`receipt_date`）** を基本とし、売上分析 `GET /receipts/sales` と揃えるため、次の二段構えを使います。

1. **主**: 対象月の `created_at` 窓で行を取得し、クライアント側で `receipt_date` が報告期間内の行だけを合算（analytics 日次と同系統）
2. **補**: 上記が 0 件のとき `receipt_date` の範囲クエリで再取得

中間・月末とも **ルーム内の全レシート件数ではなく、指定店舗の期間合計**です。

### 3.3 送信しない場合

| 理由 | テスト送信時の表示例 |
|------|---------------------|
| 店舗が解決できない | `store_not_resolved_for_room` |
| 期間内レシート 0 件 | `no_receipt_entries_in_period_for_store` |
| ルームでレポート OFF | cron 対象外 |
| 当月同種別を送信済み | cron でスキップ（`line_receipt_mid_reports`） |

---

## 4. Flex メッセージの内容

### 4.1 売上サマリー

| 行 | 内容 |
|----|------|
| 総売上 | 期間合計（税込） |
| 組数合計 | `25 組（2.5 組/日）` のように **合計と日平均を同一行** |
| 客数合計 | `59 名（5.9 名/日）` 同様 |
| 客単価 | 客数 > 0 のとき |
| 1日平均売上 | 営業日数ベース |
| レシート | 件数 |

※ 単独行の「組数平均」は **表示しない**（2026-05 以降）。

### 4.2 【予算】

`line_sales_month_budgets` 等が設定されている店舗のみ表示。レシート返信の【予算】と共通ロジック（`receipt_budget_comparison.ts`）。

**中間・月間レポートに含める行:**

| 行 | 説明 |
|----|------|
| 月次目標 | 当月予算 |
| 月次実績 | 期間合計と達成率。金額は黒、**（xx.x%）は 100% 未満で赤** |
| 日次予算累計 | 報告期末日まで（analytics KPI「日次予算差（累計）」と同系統） |

**中間・月間では出さない行**（レシート返信では出す）:

- 当日目標
- 日次予算差（当日）

### 4.3 フッター

「📈 売上推移を見る」→ `analytics.html`（`ADMIN_DASHBOARD_TOKEN` 付き URL 可）

---

## 5. テスト送信

### 5.1 前提

Edge secret **`RECEIPT_MIDREPORT_CRON_TEST_KEY`** を **`receipt-midreport-cron`** と **`admin-api`** の両方に **同じ値**で設定。

`supabase/config.toml` で `receipt-midreport-cron` の `verify_jwt = false`（admin-api からの内部呼び出し用）。

### 5.2 管理画面

ルーム設定 →「中間レポートをテスト送信」「月末レポートをテスト送信」

- 集計期間: 中間＝**当月 1〜15 日**、月末＝**当月 1 日〜末日**（テストは実行日の月）
- `line_receipt_mid_reports` 非記録

### 5.3 API

`POST /actions/test-receipt-report`（`x-admin-token` 必須）

```json
{
  "room_id": "Cxxxxxxxx",
  "report_kind": "mid_month",
  "store_partition_key": "bistrocavacava",
  "year": 2026,
  "month": 5
}
```

`store_partition_key` は任意（未指定時はルーム設定／推定）。`year` / `month` も任意（省略時は JST 当月）。

---

## 6. 重複防止

本番送信時のみ `line_receipt_mid_reports` に記録。

- ユニーク: `(report_month, report_kind, room_id)` 相当
- `trigger_type`: `day15_fallback`（cron 中間）, `day15_post`（webhook 補助）, `month_end_fallback`（cron 月末） 等

---

## 7. 関連ファイル一覧

| パス | 役割 |
|------|------|
| `supabase/functions/receipt-midreport-cron/index.ts` | スケジュール判定・Push |
| `supabase/functions/_shared/receipt_report_aggregate.ts` | 店舗解決・期間集計 |
| `supabase/functions/_shared/receipt_report_flex.ts` | Flex 本文 |
| `supabase/functions/_shared/receipt_budget_comparison.ts` | 【予算】行 |
| `supabase/functions/line-webhook/index.ts` | `maybeCreateMidMonthReceiptReportOnPost`（16日10時以降） |
| `supabase/functions/admin-api/index.ts` | テスト送信・`receipt_store_options` |
| `supabase/migrations/20260516150000_add_receipt_report_store_to_room_settings.sql` | 店舗列 |

---

## 8. 変更履歴（要点）

| 日付（目安） | 内容 |
|--------------|------|
| 2026-05 | ルーム別 `receipt_report_store_partition_key`、analytics 整合集計、テスト送信で店舗指定 |
| 2026-05 | Flex に【予算】追加（中間・月末は当日目標／日次予算差なし） |
| 2026-05 | 組数・客数を「合計（日平均）」1行表記、月次実績の％のみ赤字 |
| 2026-05 | 送信: 15日23:59 → **16日5:00締め** → **16日・翌月1日の10:00送信**（予算5時切替と分離） |
