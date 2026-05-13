# レシート分析方針

最終更新: 2026-05-14 (JST) — 5〜7節（店名・信頼度・削除）、4.1節（日付正規化）、8.0節（按分待ちと進行日の整理）を追記

## 1. 目的

LINEに投稿されたレシート画像から、売上管理に必要な情報を抽出・保存し、日次返信と月次集計に利用する。

## 2. 基本フロー

1. 画像を受信したら、レシート/領収書かどうかを判定する。  
2. レシートと判定された場合、構造化データを抽出する（モデル任意の `receipt_confidence` とサーバ側ヒューリスティックを併用）。  
3. **解析信頼度**が閾値未満のときは **DB に保存せず**（画像アップロード・`line_messages` も取り消し）、撮り直しを促す。  
4. 抽出値を正規化してDBへ永続保存する。  
5. ルーム設定に応じてLINEへ解析結果を返信する。  
6. 保存済みデータを使って中間・月末レポートを自動配信する。  

## 3. 抽出対象項目

- 店名
- 日付
- 純売上
- 消費税
- 総売上
- 会計組数
- 客数
- 客単価
- 明細（最大5件）

## 4. 正規化ルール

- 金額は `¥`、カンマ、全角/半角の揺れを吸収して数値化する。  
- 件数（組数・客数）は整数に正規化する。  
- 欠損項目は、整合性が取れる範囲で補完する。  
  - 例: `総売上` と `客数` があり `客単価` がない場合は算出して補完。  
- 税額が総売上を上回るなど不自然な値は、誤読として補正または未設定扱いにする。  

### 4.1 レシート日付の正規化と永続化（画像解析・`line-webhook`）

レシート画像から読み取った **日付文字列**は、DB の `receipt_date`（`YYYY-MM-DD`）に載せる前に **`parseReceiptDateToIso`** で正規化する。

- **Unicode エスケープ**（`\uXXXX` 等）を人が読める文字に戻してからパースする。  
- **和文で「月」が欠落した OCR**（例: `2026年513日`）を想定し、年の直後の **3〜4 桁**から月日を復元する。4 桁は `MMDD`、3 桁は `MM`+`D` または `M`+`DD` など候補を **`toIsoDateStringSafe`** で検証し、最初に成立する暦日を採用する（「513日」のような誤表記の吸収）。  
- 一般的な **`YYYY` … `MM` … `DD`**（区切りは数文字まで許容）も正規表現で抽出する。  
- 上記いずれでも日付が確定しない場合は **`resolveReceiptDateIsoForPersist`** が **投稿時点の進行営業日**（後述 **8.0** の `getJstBusinessDateForReceiptBudget` と同義）を `receipt_date` に使う。JST 深夜帯に **暦日だけが翌日に進んだレシート**でも、営業日の切り替え（5:00）に揃えて前日に寄せられる。

Flex 等に印字する和暦表記は **`formatJapaneseReceiptDateFromIso`** で ISO から `YYYY年M月D日` に整形する（保存キーは常に ISO）。

## 5. 店舗名の扱い（店舗ごと区分）

- 店舗名は可能な限り正規化・別名吸収して保存する。  
- `line_receipt_entries.store_partition_key` に正規化キーを保存し、店舗単位で確実に区分する。  
- これにより、集計・グラフ表示は店舗ごとに安全に絞り込める。  
- **英字レシート**で OCR が語順を入れ替えた場合（例: `CAVA CAVA BISTRO`）、既知ブランドの **ラテン文字列**と照合し、**既存の正規店名**（例: `BISTRO CAVA CAVA`）へ寄せる。実装は **`supabase/functions/_shared/receipt_store_name_resolve.ts`**（アナグラム一致＋レーベンシュタイン類似）。  
- **グループ店舗一覧（`MARUGO_GROUP_STORE_OPTIONS`）に一致しない店名文字列は採用しない**（`store_name` は null、`store_partition_key` は `unknown_store`）。画面上は「店舗一覧に一致せず未登録」とし、**この結果を修正**で一覧に合う店名を入力する。  

## 6. 解析信頼度（保存可否）

- Groq 応答の **`receipt_confidence`（0〜1）** と、日付・金額整合・件数などから計算した **ヒューリスティック信頼度**を合成する。  
- 合成値が **閾値未満**のときは **`line_receipt_entries` / `line_message_media` を作成せず**、Storage の一時ファイルと当該 `line_messages` 行を削除し、**撮り直し**を促す（`line-webhook` の `RECEIPT_ANALYSIS_CONFIDENCE_MIN`）。  

## 7. 保存ポリシー（永続化）

- レシート解析結果は `line_receipt_entries` に保存する。  
- 生データ相当の解析ペイロードは `raw_payload` に保存する。  
- 同一LINEメッセージは `line_message_id` で重複保存しない。  
- 画像ファイル本体は `line_message_media` と Storage に保存する。  

## 8. LINE返信ポリシー

- 返信可否はルーム権限（画像解析返信設定）で制御する。  
- 返信時は **LINE Flex**（リッチメッセージ）で解析結果を返す。  
- 解析カードのフッターに **この結果を修正** に加え **この解析結果を削除** を出す。削除はメッセージ **`レシート解析削除 ID:（LINEメッセージID）`** で処理し、**`line_receipt_entries`・当該画像の Storage・`line_message_media`・紐づく `line_messages` 行**を削除する。  
- 日次返信には当月累計売上（当月1日から投稿時点まで）を含める。  

### 8.0 進行日（5:00）と「按分待ち」（同日の二層ルール）

営業日の境界は **暦の 0 時ではなく JST 5:00**（定数 `RECEIPT_BUDGET_BUSINESS_DAY_START_HOUR_JST`、既定 `5`）。ここから **二つの概念**を分けて理解する。

#### 進行日（`getJstBusinessDateForReceiptBudget`）

**「いまがどの営業日にいるか」**の基準日（`YYYY-MM-DD`）。JST 0:00〜4:59 は **まだ前暦日が進行日**。

- **進行日より後の暦日**（表の行・集計上）は、**差額を ¥0** とし、**累計差**にもその日を **含めない**（まだ来ていない日扱い）。  
- **日付がパースできず** `resolveReceiptDateIsoForPersist` に落ちた場合も、この進行日を `receipt_date` に保存する（深夜投稿の暦ズレ防止）。

ランタイム間で **時刻パーツが欠ける・12 時間表記になる**差を避けるため、`getJstBusinessDateForReceiptBudget` / `getJstCalendarDateIsoTokyo` / `getJstHourInTokyo` は **`Intl.DateTimeFormat('sv-SE', { timeZone: 'Asia/Tokyo', hour12: false, hourCycle: 'h23' })`** で年月日時を分解する（Deno・ブラウザ共通の取り回し）。

#### 按分待ち（`shouldDeferDailyBudgetUntilJstOpen`）

**暦の「今日」かつ店休でなく、まだ JST 5:00 前**のときだけ真になるフラグ。進行日は前日でも、**レシート行や表の行の `receipt_date` が暦の今日**なら、**その暦日は按分待ちになり得る**（条件は「東京暦日 === 行の日付」かつ 5:00 前）。

- **日次の「差額」** … 按分待ち中は **実績 − 按分目標ではなく実績 − 0**（差は実績側のみ。未計上なら **¥0**）。**深夜にまだ「当日営業の按分」を差し引かない**ため。  
- **日次予算累計（フッタ・KPI・LINE 累計行）** … 按分待ちの日も、累計に足す差分は **`g − 0`**（按分 `b` は引かない）。  
- **予算の「見せ方」** … 按分待ちでも **按分後の日別予算そのものは営業日の目標として表示**する。`analytics.html` の予算列・LINE の **「当日目標」**は按分額を出し、**差だけ**を上記のとおり遅延する。

**JST 5:00 以降**（同一暦日内）に按分待ちは解除され、**実績 − 当日按分**で日次差・累計に按分が乗る。売上ゼロなら **−当日按分** もあり得る。

実装の単一ソース: **`supabase/functions/_shared/sales_budget_allocation.ts`**（`getJstBusinessDateForReceiptBudget`、`shouldDeferDailyBudgetUntilJstOpen`、`getJstCalendarDateIsoTokyo`）。  
画面は **`analytics.html`** の `getJstBusinessDateStringForReceiptBudgetJs` / `receiptDailyDeferDailyBudgetJs`、LINE は **`line-webhook`** の `receiptDateIsAfterTodayJst`、`computeReceiptDailyDiffTotalLikeAnalyticsFooter`、`buildReceiptBudgetComparisonRows` が同じ考え方を踏襲する。

詳細・表の列との対応は **`docs/ANALYTICS_RECEIPT_SALES_AND_BUDGET.md`** を参照。

### 8.1 解析カード内の予算ブロック（【予算】）

店舗×月の予算が `line_sales_month_budgets` 等に設定されている場合、解析カードに **【予算】** セクションを付与する。按分・店舗休日は **`sales_budget_allocation.ts`** 経由で **analytics / admin-api と同じルール**を用いる。

| 表示ラベル | 意味 |
|------------|------|
| 月次目標 | 当月の予算額（`budget_yen`） |
| 月次実績 | 当月の総売上累計と達成率 |
| 当日目標 | レシート日の日別按分予算。**按分待ち**（8.0）中も按分額を表示する。 |
| 日次予算差 | **そのレシート日**について、通常は「実績 − 当日目標」。**進行日より後**の暦日は差 **¥0**。**按分待ち**中も差は **¥0**（実績 − 0 扱い）。店舗休日は実装どおり。 |
| 日次予算累計 | **月初〜進行日まで**の日次差の合計（按分待ちの日は **`g − 0`** で寄与）。`getJstBusinessDateForReceiptBudget` で進行日を決める。analytics の日次表フッタ／KPI **「日次予算差（累計）」**（`receiptDailyFooterBudgets`）と同じ定義。**1行に詰めると省略されやすいため、日次予算差とは別行**で表示する。 |

実装の目安: `supabase/functions/line-webhook/index.ts` の `buildReceiptBudgetComparisonRows`、`loadStoreGrossSumsByMonthDates`、`computeReceiptDailyDiffTotalLikeAnalyticsFooter`。

### 8.2 月間総売上・組数・客数の「1日平均」

カード下部（点線の下）に、店舗×対象月の **月間合計** と **1日平均** を示す。

#### 分母（営業日数）

**analytics.html** の「営業日数」と同じ定義に揃える。

- **意味**: 対象月において、日次 series 上で **`receipt_count > 0`** となる日数。
- **データソース上の同等定義**: 当該店舗（`store_partition_key`）×当該月について、`line_receipt_entries` の **`receipt_date` のユニーク日数**（その日に1件でもレシート行があればカウント）。
- **平均が付かない場合**: 上記営業日数が 0（その月にまだレシートがない等）のときは、合計行のみ表示し **1日平均行は出さない**。

#### 計算式

| 行ラベル | 上段（合計） | 下段「1日平均」の値（右列） |
|----------|--------------|-----------------------------|
| 月間総売上 | 月内総売上（円） | `round(合計 ÷ 営業日数)` を `¥` 表記 |
| 月間会計組数 | 月内会計組数の合計 | `合計 ÷ 営業日数` を小数第1位まで（analytics の「1日平均 n 組」に合わせ、表示は **`n 組`**） |
| 月間客数 | 月内客数の合計 | 同上（表示は **`n 名`**） |

#### Flex 上のレイアウト

カッコで 1 行に詰めず、**段落（行）を分ける**。

- 各指標は **縦並びの `box`** とし、**1行目**が通常の baseline（ラベル + 合計）、**2行目**が baseline で左に固定ラベル **「1日平均」**、右に上表の値。
- 2行目はフォントを一段小さく（`xs`）、色は補助的（グレー系）。
- 型・ビルダー: `ReceiptFlexBaselineKvRow.avgLineValue`（右列の文字列のみ）を `buildReceiptFlexBaselineRows` が解釈し、縦 `box` を組み立てる。

#### 実装の目安（`line-webhook`）

| 処理 | 関数・フィールド |
|------|------------------|
| 営業日数の算出 | `countReceiptActiveDaysInStoreMonth` |
| 返信組み立て時の分母解決 | `resolveReceiptMonthDailyAvgDivisor` → `buildLineReceiptImageAnalysisReply` の `monthAvgBusinessDayDivisor` |
| Flex 行の生成 | `buildLineReceiptImageAnalysisReply` 内の `monthRows`、`buildReceiptFlexBaselineRows` |

ダッシュボード側の KPI 定義は **`analytics.html`**（`activeDays = dailySeries.filter(d => d.receipt_count > 0).length`）を参照。

### 8.3 レシート修正フロー（Flex）

解析結果カード下部の **「この結果を修正」** から、該当レシートの修正フローを開始できる。

- ボタン押下時は `レシート修正 ID:<line_message_id>` を送信し、そのレシートを直接編集対象にする。  
- **項目一覧・値入力**はいずれも **Flex（青ヘッダー + 本文）**。項目と現在値は解析カード本編と同様に **baseline のラベル flex 6 / 値 flex 10** で並べる。  
- 修正フローは「項目選択 → 値入力 → 確定」で保存され、保存後は更新後の解析結果カードを再返信する。  

**ユーザー向け操作説明（トークへ文字で送る）**は、カード本文末尾で次を明示する（実装と `normalizeReceiptCorrectionControl` に整合）。

- **1〜8** … その番号の項目の修正へ進む  
- **「確定」「保存」「反映」「完了」「OK」など** … 変更を保存して終了  
- **「キャンセル」「中止」「終了」など** … 修正をやめる（未保存は破棄）  
- 値入力画面では **「戻る」「back」「項目選択」など** … 項目一覧に戻る  

実装の目安: `buildReceiptCorrectionFieldSelectionPrompt`、`buildReceiptCorrectionValueInputPrompt`、`normalizeReceiptCorrectionControl`。  

## 9. 定期集計レポート

- 中間報告: 毎月15日 23:59 (JST)  
  - 集計期間: 当月1日〜15日  
- 月間報告: 毎月月末日 23:59 (JST)  
  - 集計期間: 当月1日〜月末  
- 月次集計は毎月1日を起点に再計算し、前月値は持ち越さない。  

## 10. 管理画面での可視化

- 売上グラフ画面 `sales.html` で可視化する。  
- 店舗選択はドロップダウンで行い、店舗ごとの時系列グラフを表示する。  
- APIは `GET /receipts/sales` を利用し、月・店舗単位の集計結果を取得する。  
