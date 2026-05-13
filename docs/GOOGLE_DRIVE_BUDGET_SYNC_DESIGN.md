# Google Drive予算連携 設計図（コード変更なし）

最終更新: 2026-05-11 (JST)

**非エンジニア向けの日々の手順**は **[`GOOGLE_DRIVE_BUDGET_SYNC_RUNBOOK.md`](./GOOGLE_DRIVE_BUDGET_SYNC_RUNBOOK.md)** を参照してください。

## 1. 目的

- Google Drive（主に Google Sheets）に置いた予算ファイルを自動で読み取り、既存の予算機能へ反映する。
- 既存の表示・返信（analytics / LINE webhook）が参照する Supabase 予算データを更新対象とする。

## 2. 推奨方式（結論）

方式: **Push（Apps Script -> Supabase API）**

- シート更新をトリガーに Apps Script が実行
- データ整形後に API へ送信
- 反映結果をログ通知

理由:

- 定期ポーリング不要で反映が速い
- Google 側の権限管理がシンプル
- 予算担当者がシート中心に運用できる

## 3. 全体アーキテクチャ

1. Google Sheets（予算マスタ）
2. Apps Script（変換・送信）
3. Supabase Admin API（受信）
4. DB 保存
   - `line_sales_month_budgets`
   - `line_sales_month_store_closed_days`（必要時）
5. 利用側
   - `analytics.html`
   - `line-webhook`

### 3.1 日次予算の「進行日」（JST 5:00）

- **Sheets から同期するデータ**は、月の予算額・按分重み・休業日など **DB に載るマスタ**です。シートを更新した時刻や、同期ジョブの実行時刻は **進行日の切り替えとは無関係**です。
- **日次予算差・日次予算累計**で「いつからその暦日を当日として数えるか」は、暦の 0 時ではなく **JST 5:00** です。**JST 0:00〜4:59** はまだ **前の暦日が進行日**（当日行は差 0 扱いになり得る）。**5:00 以降** で進行日がその暦日に切り替わります。意図は、早朝に売上がまだ無い状態で **当日分の予算だけがマイナス差に乗る**見え方を避けることです。
- **利用側**の計算は `supabase/functions/_shared/sales_budget_allocation.ts` の `getJstBusinessDateForReceiptBudget` と同一定義（画面は `analytics.html`、LINE は `line-webhook`）。按分・休日マージのロジックとは別レイヤーのルールです。
- **運用説明の正本**: **`docs/RECEIPT_ANALYSIS_POLICY.md` 8.0**。列仕様・按分との対応: **`docs/ANALYTICS_RECEIPT_SALES_AND_BUDGET.md`**。

## 4. シート仕様（案）

### 4.1 シート名

- `monthly_budgets`

### 4.2 必須列

- `month`（`YYYY-MM`）
- `store_name`（運用入力）
- `store_partition_key`（推奨: 直接入力）
- `budget_yen`（整数）
- `weekday_weight`
- `pre_holiday_weight`
- `holiday_weight`
- `closed_dates`（`YYYY-MM-DD,YYYY-MM-DD...`）
- `enabled`（TRUE/FALSE）
- `updated_at`（任意）

### 4.3 補助シート（推奨）

- `store_mapping`
  - `store_name_alias`
  - `store_partition_key`

## 5. 同期ルール

- `enabled=TRUE` の行のみ同期対象
- `month + store_partition_key` を一意キーとして upsert
- `closed_dates` は配列へ正規化
- 欠損・型不正は当該行のみスキップしエラー記録
- 送信単位は「1行ずつ」または「月単位バッチ」

## 6. 反映トリガー設計

### 6.1 即時反映（推奨）

- `onEdit` で対象列変更時に同期キュー化
- 実処理は時間主導トリガー（数分おき）で実行して連打を吸収

### 6.2 定時反映

- 例: 毎日 6:00 / 12:00 / 18:00
- 月初に全件同期を追加実行

## 7. API 設計（既存活用）

既存の予算保存 API（admin-api）へ以下を送信する。

- `month`
- `store_partition_key`
- `budget_yen`
- `weekday_weight`
- `pre_holiday_weight`
- `holiday_weight`
- `store_closed_dates`

認証:

- Bearer token（Apps Script の Script Properties に保持）
- 可能なら専用トークン（最小権限）

## 8. バリデーション設計

- `month`: `^\d{4}-\d{2}$`
- `budget_yen`: 0 以上の整数
- 各 `weight`: 正数
- `closed_dates`: 対象月内の日付のみ許可
- `store_partition_key`: 許可リスト照合

不正時:

- 反映しない
- `sync_log` シートへ行番号と理由を追記

## 9. 監視・運用

- 毎回、成功件数 / 失敗件数を記録
- 失敗時は Slack 通知（Webhook）
- 月初に全店舗同期レポートを自動通知
- 手動再実行メニュー（Apps Script カスタムメニュー）を用意

## 10. 失敗時リカバリ

- API 失敗: 指数バックオフで最大 3 回リトライ
- 部分失敗: 失敗行のみ再送できる設計
- 誤更新: シート履歴 + DB 監査ログで復旧

## 11. セキュリティ

- トークンは Script Properties または Secret Manager で管理
- シート編集権限は予算担当のみに限定
- 送信先 API を固定（プロジェクト URL allowlist）
- 変更者情報（メール）を同期ログへ記録

## 12. 導入ステップ（実装前）

1. シート列仕様を確定
2. 店舗キー運用ルール（alias 許容有無）を確定
3. 既存 API との入力互換を確認
4. テスト環境で 1 店舗 / 1 か月を検証
5. 通知・ログ運用を確認
6. 本番切替（初回全件同期）

## 13. 改訂履歴

| 日付 | 内容 |
|------|------|
| 2026-05-11 | §3.1 を追加し、日次予算の進行日（JST 5:00）と同期データの違いを明記 |

---

運用手順書: [`GOOGLE_DRIVE_BUDGET_SYNC_RUNBOOK.md`](./GOOGLE_DRIVE_BUDGET_SYNC_RUNBOOK.md)
