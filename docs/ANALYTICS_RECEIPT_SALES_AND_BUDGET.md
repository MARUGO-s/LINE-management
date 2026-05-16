# 売上分析（analytics）：日次売上・月間予算・按分・店舗休日

最終更新: 2026-05-14 (JST) — 按分待ち（5:00 前の差と予算表示）、日付列表示、時刻分解（Intl）を追記

レシート集計（`line_receipt_entries`）をもとに、**店舗・月**単位で売上を表示し、**月間予算**を**平日／休日前日／休日**の比率で**日別に自動按分**する機能の説明です。フロントは主に **`analytics.html`**、API は **`admin-api`** Edge Function、按分ロジックは **`supabase/functions/_shared/sales_budget_allocation.ts`** で共有されています。LINE 上のレシート報告文面でも同じ按分結果を参照する場合は **`line-webhook`** が同モジュールを利用します。

### 運用・問い合わせで必ず押さえること（日次予算の進行日と按分待ち）

**日次表の「差額」列と KPI「日次予算差（累計）」は、暦の 0 時ではなく JST 5:00 を境界に含めます。**  
深夜〜早朝に「まだ売上がないのに当日分の予算がマイナス差に乗る」のを避けるためです（0〜4 時は前日が進行日）。

加えて、**暦の当日でまだ JST 5:00 前**のときは **按分待ち**（`shouldDeferDailyBudgetUntilJstOpen`）となり、**差額・累計差の按分負担だけ**をまだ立てません（実績 − 0）。**予算列の按分額と LINE「当日目標」は営業日として表示**します（詳細は **`docs/RECEIPT_ANALYSIS_POLICY.md` 8.0**）。

- 定数: `RECEIPT_BUDGET_BUSINESS_DAY_START_HOUR_JST`（既定 `5`）
- 進行日: `getJstBusinessDateForReceiptBudget`（TS）／`getJstBusinessDateStringForReceiptBudgetJs`（`analytics.html`）
- 按分待ち: `shouldDeferDailyBudgetUntilJstOpen`（TS）／`receiptDailyDeferDailyBudgetJs`（`analytics.html`）

時刻の分解は **`Intl.DateTimeFormat('sv-SE', { timeZone: 'Asia/Tokyo', hour12: false, hourCycle: 'h23' })`** に統一し、ランタイム差で hour が欠ける・12 時間表記になる問題を避けています。

LINE の【予算】ブロックも同じ進行日・按分待ちで計算します。方針の正本は **`docs/RECEIPT_ANALYSIS_POLICY.md` 8.0**。

---

## 1. 概要

| 領域 | 内容 |
|------|------|
| 日次売上 | 対象月の各日について、総売上・組数・客数・**日別予算**・**差額（実績−予算）**を表形式で表示 |
| 月間予算 | 店舗×月ごとに金額を保存（DB）。未設定時は予算列・差額は `-` など |
| 按分 | 月額を「平日・休日前日・休日」の**重み**に応じて各日へ分配（**端数は最大剰余法**で1円単位） |
| 店舗休日 | 指定した日は按分から**除外**（重み0）。その分は**他の営業日**へ再配分。日別予算は **0円** |
| 進行日 | **JST の暦日 0〜4 時は前日扱い**、**5 時以降がその日の開始**（`getJstBusinessDateForReceiptBudget` と同一定義）。**進行日より後**の暦日は差額を **0円** とし、累計差額にも含めない |
| 按分待ち | **暦の当日**かつ店休でなく **JST 5:00 前** … **予算列は按分を表示**、**差額は実績 − 0**（未計上なら ¥0）。累計差も **`g − 0`** で寄与。5:00 以降は通常の **実績 − 按分** |
| 前年比 | **比較西暦**と**同じ月番号**の売上と比較。**手入力**があればレシート集計より手入力を優先 |

---

## 2. 画面の場所と前提

- **画面**: プロジェクト内の `analytics.html`（管理トークンで `admin-api` に接続）。
- **認証**: トークン保存後、`GET /receipts/sales` などでデータ取得。
- **店舗**: セレクトで **`store_key`（store_partition_key）** を指定して保存・表示の一貫性を保つ。未指定だと API 側で既定店舗が選ばれ、予算キーと取り違える原因になるため、**保存前に店舗選択が推奨**される（モーダル内のヒントあり）。

---

## 3. 日次売上（数値）表

### 3.1 列

1. **日付** — `M/D` と **曜日（JST 基準）**。祝日・日曜は強調色、土曜は別色（クライアント側 `JAPANESE_HOLIDAYS` と曜日判定）。**曜日まで省略しない**（テーブルは `text-overflow: ellipsis` を使わず、日付列に十分な `min-width` を確保して横スクロールで閲覧）。
2. **総売上** — その日のレシート合計（円）。
3. **組数** — `party_count` 合計。
4. **客数** — `guest_count` 合計。
5. **予算** — その日の**按分後の日別予算**（店舗休日は **¥0**）。
6. **差額** — 原則 **実績 − 予算**（符号付き円表示）。

### 3.2 レイアウト（ブロック数）

ビューポート幅に応じて **1〜4 ブロック**（各ブロック6列）に分割し、横スクロールしやすいようにしている。

### 3.3 差額のルール（表・フッタ・KPI で共通）

- **店舗休日**  
  - 予算は **0円**。  
  - 売上が **0** のときは差額 `-`（実績ゼロで比較しない表示）。  
  - 売上が **0 以外** のときは **実績 − 0 = 実績** を差額表示（休日に売上が載ったケース用）。
- **店舗休日でない日** で日別予算がある場合  
  - **進行日より後**の暦日: 差額は **¥0**（将来日は進捗に含めない）。  
  - **按分待ち**（暦の当日・5:00 前・店休でない）: 差額は **¥0**（実績 − 0。按分は予算列のみ表示）。  
  - **進行日以前**かつ按分待ちでない日: **実績 − 予算**（実績が未計上でも 0 として計算し、例えば **−予算** になり得る）。進行日は **JST 5:00** で切り替え（0〜4 時はまだ前日）。

### 3.4 月間合計行の差額

フッタの **差額合計** は、上記ルールで**日ごとの差額を足し込んだ値**と一致する。按分待ちの日は **`g − 0`** が寄与する。KPI **「日次予算差（累計）」** もこの合計と同じ。

### 3.5 予算列と按分待ち

**按分待ち**中でも **予算列には按分後の日別予算を表示**する（営業日の目標として可視化）。**差額列だけ**按分を差し引かない（3.3 参照）。フッタの **予算合計** は按分額をそのまま足す（按分待ちで予算を 0 にしない）。

---

## 4. KPI（ダッシュボード上部）

実装の中心となるもの:

- **月間予算** — DB の当月予算額。サブテキストで対象月・按分保存の旨を表示。
- **達成率・予算差** — 月間実績と月間予算から達成率・予算差（月単位）。
- **日次予算差（累計）** — 日次表フッタと同じ「進行ベースの累計差額」。色分け（プラス／マイナス／ゼロ）。
- **前年同月対比** — 比較年の**同じ月**の売上（手入力優先）に対する **%** のみ表示。

---

## 5. 月間予算・日別配分モーダル

「月間予算・日別配分」ダイアログで、次を設定する。

### 5.1 入力項目

- **月間予算（円）** — 月の総額。
- **平日の重み** — デフォルト例: `1`
- **休日前日の重み** — デフォルト例: `1.5`
- **休日の重み（日曜・祝）** — デフォルト例: `2`

### 5.2 プレビュー表

月間予算が正のとき、**対象月の全日**について:

- **日付** — `M/D` と **曜（JST）**。
- **区分** — 按分上の区分（**平日** / **休日前日** / **休日**）。店舗休日にチェックが入っている日は表示上 **店舗休日**。
- **店舗休日** — チェックでその日を休店扱いに。
- **日別予算** — 按分結果（円）。

### 5.3 曜日一括（店舗休日）

プレビュー直上の **月・火・水・木・金・土・日** のチェックで、その**曜日に該当する全日**をまとめて店舗休日 ON/OFF。対象月にその曜日が無い場合は無効。

### 5.4 保存・クリア

- **保存** — `PUT /receipts/sales-budget` で DB 更新後、一覧再読込。
- **予算クリア** — 当月・当店舗の予算行と店舗休日を削除。

---

## 6. 按分ロジック（アルゴリズム）

実装: `allocateDailyBudgetsForMonth`（`sales_budget_allocation.ts`）。フロントのプレビュー（`allocateDailyBudgetsClient`）も同じ考え方。

### 6.1 日タイプ（暦・祝日ベース）

- **休日** — **日曜**、または **日本の祝日**（共有モジュール `japanese_holidays.ts` の日付集合）。
- **休日前日** — **翌日が休日**となる日（当日は休日でない）。
- **平日** — 上記以外。

※ここでの「休日」は**国民の祝日＋日曜**であり、**店舗休日**とは別概念。

### 6.2 重みと店舗休日

各日に、区分に応じた **raw 重み**（平日／休日前日／休日）を割り当てる。

- **店舗休日**に指定された日は **effective 重み = 0**（按分対象外）。
- 月間予算 **全体**は、**effective 重みが正の日**だけで按分し直す（休店分は他日へ再配分）。

### 6.3 端数

- 各日の按分額は **floor** し、**不足分（remainder）** を **小数部が大きい順**に **1円ずつ** 配分（**最大剰余法**）。
- 端数の追加は **effective 重み > 0 の日**にのみ行う（店舗休日には載せない）。

### 6.4 全て休店など重み総和が 0

その月の全日 **0円** のマップを返す。

---

## 7. データの保存と API

### 7.1 `GET /receipts/sales`

クエリ例: `month=YYYY-MM`、`store_key`、`compare_year`（任意）。

返却のうち本機能関連:

| フィールド | 意味 |
|------------|------|
| `series` | 日次の実績配列（全日ぶん、レシートが無い日は 0 埋め） |
| `month_budget_yen` | 月間予算 |
| `budget_weekday_weight` など | 按分重み |
| `store_closed_dates` | 店舗休日 `YYYY-MM-DD` の配列（**専用テーブルと jsonb をマージ**した結果） |
| `daily_budget_yen_by_date` | 日付キー → 按分後の日別予算（円） |
| `manual_comparison_gross_yen` | 比較月の手入力売上（取得できた場合） |
| `comparison_year` / `comparison_sales_month` | 比較に使った年・月 |

按分計算は **サーバー側**でも行い、`daily_budget_yen_by_date` として返すため、表・KPI は API と整合しやすい。

### 7.2 `PUT /receipts/sales-budget`

ボディ例（保存時）:

- `store_key`, `month`（正規化された `YYYY-MM`）
- `budget_yen`（クリア時は null 等で削除動作）
- `weekday_weight`, `pre_holiday_weight`, `holiday_weight`
- `store_closed_dates` — 文字列配列

処理の要点:

- `line_sales_month_budgets` に **upsert**（重み・予算・`store_closed_dates` jsonb）。
- **`line_sales_month_store_closed_days`** に **店舗休日を行単位で再保存**（信頼できるソースとして利用）。

読取時は **専用テーブル**と **jsonb** を **`mergeStoreClosedDateLists`** でマージし、過去データや環境差でも空にならないようにしている。

### 7.3 手入力前年売上

- `GET /receipts/sales-manual-months?year=&store_key=`
- `PUT /receipts/sales-manual-months` — 西暦×月の **総売上・会計組数・客数** を店舗ごとに登録（組数・客数は省略可）。

`GET /receipts/sales` では、比較月の手入力があれば前年比計算に利用（組数・客数は手入力があれば優先、なければレシート集計）。

`GET /analytics/monthly` でも、手入力がある月は月次シリーズへマージされる（KPI の前月比・前年比バッジに反映）。

---

## 8. データベース（主要テーブル）

| テーブル | 役割 |
|----------|------|
| `line_sales_month_budgets` | 店舗×月の `budget_yen`、按分重み、`store_closed_dates`（jsonb） |
| `line_sales_month_store_closed_days` | 店舗休日の **正規化行**（`store_partition_key`, `target_month`, `closed_on`） |
| `line_sales_manual_month_gross` | 任意西暦の月次総売上・会計組数・客数（前年比用・手入力） |
| `line_receipt_entries` | レシート明細（集計ソース） |

マイグレーションは `supabase/migrations` 内の `line_sales_month_budgets`、按分重み追加、`store_closed_dates`、専用テーブル作成などのファイルを参照。

RLS は **service_role** による運用を想定したポリシーが付与されている。

---

## 9. LINE 側（line-webhook）との関係

レシート関連メッセージで日別予算や休日を表示する処理では、`line_sales_month_budgets` と `line_sales_month_store_closed_days` を読み、`mergeStoreClosedDateLists` と **`allocateDailyBudgetsForMonth`** で **admin-api と同じ按分**を再現する。

### 9.1 解析 Flex の「日次予算差」と「日次予算累計」

- **日次予算差** … レシート日の総売上と、その日の日別目標との差（当日スナップショット）。**按分待ち**中は **¥0**（実績 − 0）。**当日目標**は按分額を表示。  
- **日次予算累計** … 対象月の各日について、**analytics.html** の `receiptDailyFooterBudgets`（日次表フッタ・KPI「日次予算差（累計）」）と**同じループ条件**で合算した値。店舗休日・**進行日（JST 5:00 切り替え）**より後の日・**按分待ち**（`g − 0`）もフッタと揃える。  
- LINE 上では **ラベルを分けた2行**で表示する（1行に併記すると Flex 上で省略されやすいため）。

実装では、月内の `line_receipt_entries` を日付で集計したマップ（`loadStoreGrossSumsByMonthDates`）と按分マップから `computeReceiptDailyDiffTotalLikeAnalyticsFooter` で累計を算出し、`buildReceiptBudgetComparisonRows` が Flex 行を組み立てる。

**月間 KPI の 1日平均（LINE 解析カード）**: ダッシュボードの **営業日数**（`receipt_count > 0` の日数）を分母に、総売上・組数・客数の平均を **analytics と同じ式**で LINE 側にも表示する。レイアウト・型の説明は **`docs/RECEIPT_ANALYSIS_POLICY.md` 8.2** を参照。

---

## 10. タイムゾーンと祝日の注意

- **営業日・進行日判定**（差額の将来日除外など）は **JST（Asia/Tokyo）** を基準にし、**日次予算の締めは 5:00**（`RECEIPT_BUDGET_BUSINESS_DAY_START_HOUR_JST` / `getJstBusinessDateForReceiptBudget`）。**按分待ち**（暦当日・5:00 前）は `shouldDeferDailyBudgetUntilJstOpen`（予算表示は維持、差のみ遅延）。曜日表示など他ロジックは従来どおり暦日ベースの箇所もある。
- **按分上の休日**は **共有の祝日カレンダー＋日曜**。**振替休日**などは実装の祝日セットに依存するため、境界日はコード／データ更新時に確認するとよい。

---

## 11. 関連ファイル一覧（参照用）

| 種別 | パス |
|------|------|
| UI | `analytics.html` |
| 按分・休日マージ・**進行日（5:00）**・**按分待ち** | `supabase/functions/_shared/sales_budget_allocation.ts`（`getJstBusinessDateForReceiptBudget`、`shouldDeferDailyBudgetUntilJstOpen`） |
| 祝日データ | `supabase/functions/_shared/japanese_holidays.ts` |
| 売上・予算 API | `supabase/functions/admin-api/index.ts`（`fetchReceiptSalesState`, `upsertReceiptSalesBudget` 等） |
| LINE | `supabase/functions/line-webhook/index.ts`（按分・**進行日** `receiptDateIsAfterTodayJst`、**按分待ち**・累計 `computeReceiptDailyDiffTotalLikeAnalyticsFooter`、`parseReceiptDateToIso` / `resolveReceiptDateIsoForPersist`） |
| DB | `supabase/migrations/*line_sales*`, `*sales_budget*`, `*store_closed*` など |

---

## 12. よくある運用フロー

1. **analytics** で店舗・対象月を選ぶ。  
2. **月間予算・日別配分**を開き、月額と三種の重みを入力。  
3. 定休や臨時休業日を **店舗休日**（または曜日一括）で指定。  
4. プレビューで日別予算を確認し **保存**。  
5. 日次表で実績・差額・累計差額（KPI）を確認。  
6. 前年比較が必要なら **比較西暦**と **手入力（任意年の月売上）** を設定。

以上が、日次売上表示から月間予算の比率按分、店舗休日、進行日の差額扱い、API／DB までを含む機能の全体像です。
