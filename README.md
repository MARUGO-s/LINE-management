# LINE Management

LINEトークを取り込み、AIで意図判定し、
- カレンダー登録/照会
- 会話検索（トーク履歴 + 資料）
- 定期要約配信
- 翌日予定通知
- Gmail予約メール通知

を実行する Supabase Edge Functions ベースの運用アプリです。

---

## 1. アーキテクチャ概要

- 受信: `line-webhook`
- 管理API: `admin-api`
- 管理UI: `index.html`（静的） / `admin-ui`（Edge Function配信）
- メディア/資料UI: `media.html`
- 定期処理: `summary-cron`（要約・翌日予定） / `gmail-alert-cron`（Gmail予約通知）
- 診断: `check-cron`
- DB: Supabase Postgres（`line_messages` ほか）
- Storage: `line-media`, `line-documents`

---

## 2. このアプリでできること（機能網羅）

### 2.1 LINE受信・保存

- LINE Webhook（署名検証あり）でメッセージ受信
- `line_messages` に全メッセージを保存（テキスト以外は説明タグで保存）
- ルーム表示名の自動同期（group/room summary API）

### 2.2 LINEメディア保存

- 保存対象: `image`, `video`, `audio`, `file`
- LINE Content API から実データ取得し Storage へ保存
- メタ情報を `line_message_media` に保存
- 制限:
  - 1ファイル上限: 管理画面設定 `media_upload_max_mb`（1〜20MB、既定10MB未満）
  - 絶対上限: 20MB（LINE側レスポンスヘッダ検査）
  - 総容量上限: 500MB（超過時は保存スキップ/ロールバック）

### 2.3 AI一次判定（テキスト）

Groq による一次分類:
- `create_calendar`
- `list_calendar`
- `search_messages`
- `none`

低信頼時は確認プロンプトを返す（設定条件あり）。

### 2.4 予定登録（Google Calendar）

- 明示コマンド: `予定登録 ...`, `予定追加 ...`
- 自然文からのAI抽出登録
- 日時・件名・場所の抽出
- 件名は文脈重視で抽出（日時語を除外、汎用語のみにならないよう補正）
- 場所は `「Xで試飲会」` 形式や `場所:` 行から推定
- 複数予定文の自動登録（最大5件）

### 2.5 予定確認（Google Calendar照会）

- 明示コマンド: `予定確認`, `予定一覧`, `予定報告`
- 自然文質問から照会スコープ推定
- スコープ対応:
  - 今日/明日/今週/来週
  - 今月/来月
  - 日付指定（YYYY-MM-DD / 4月10日 など）
  - 年月指定/年指定
  - 今後30日

### 2.6 登録前確認フロー（pending）

- 自動登録しないケースでは候補を提示し、`はい` で登録 / `いいえ` でキャンセル
- pending有効期限: 30分
- pending中に修正可能:
  - 例: `場所をmarugoに変更`
  - 例: `時間を19:00に変更`
  - 例: `件名をシェフミーティングに変更`

### 2.7 会話検索（トーク + 資料）

- 明示コマンド/自然文の両対応
- 検索対象:
  - `line_messages.content`
  - `line_search_documents`（ファイル名 + 抽出テキスト）
- 期間指定:
  - `60/120/180/365/730/1095日`
  - `全期間`（0）
- スコープ:
  - このルーム
  - 全ルーム横断
- 一致件数が適量ならAI要約を付与

### 2.8 定期要約配信（summary-cron）

- 配信時刻は全体/ルーム別で設定可能（0〜23時）
- 全体配信（`LINE_OVERALL_ROOM_ID`）
- ルーム個別配信（`send_room_summary=true` のルーム）
- 最終回集計モード:
  - `independent`（各回独立）
  - `daily_rollup`（1日最終回で日次集計）
- 処理タイミング:
  - `after_each_delivery`
  - `end_of_day`

### 2.9 翌日予定通知

- 全体設定 + ルーム別ON/OFF
- 指定時刻に翌日の予定を配信
- `予定がある日だけ通知` オプション
- 表示上限件数設定（1〜50）

### 2.10 Gmail予約メール通知（リアルタイム）

- 専用 `gmail-alert-cron` が毎分実行
- Gmail unread から予約関連メールを抽出
- ルール抽出 + 必要時AI抽出で予約情報を整形
- 対象ルームへLINE push
- 通知済みメールは `gmail_reservation_alert_logs` で重複防止

### 2.11 管理画面（index.html / admin-ui）

- 管理トークン認証（`x-admin-token`）
- グローバル設定編集
- ルーム個別設定の追加/更新/削除
- ルーム行のドラッグ＆ドロップ並び替え（`room_sort_order` 永続化）
- 配信ログ閲覧
- 今すぐ要約実行（手動）
- Gmail連携先確認
- ストレージ使用量表示

### 2.12 メディア/資料ビューア（media.html）

- メディア一覧（種別・ルーム絞り込み、ページング）
- メディア前後文脈表示
- メディア削除
- メディア容量進捗表示
- メディアアップロード上限（MB）変更
- 資料ライブラリ:
  - アップロード（TXT/PDF）
  - ルーム紐付け（または共通資料）
  - 一覧/絞り込み/削除

### 2.13 診断機能

- `check-cron` で cron/job/設定状態を確認
- `summary_settings` と cron 関数定義の確認

---

## 3. 返信・自動登録の挙動（重要）

ルーム設定の主キー:
- `is_enabled`（有効）
- `bot_reply_enabled`（自動応答）
- `calendar_ai_auto_create_enabled`（AI自動登録）
- `message_search_enabled`（会話検索応答）

挙動の要点:

- `is_enabled=false`
  - 返信/AI判定/登録処理は実施しない
  - ただしメッセージ保存とメディア保存は継続

- `is_enabled=true` かつ `bot_reply_enabled=true`
  - 通常の返信あり（確認メッセージ含む）
  - AI自動登録は `calendar_ai_auto_create_enabled` と `CALENDAR_AI_AUTO_CREATE_ENABLED` の両方で制御

- `is_enabled=true` かつ `bot_reply_enabled=false`
  - 返信はしない（サイレント）
  - 条件によりカレンダー登録だけ実行される（無言登録）

- `message_search_enabled=false`
  - 会話検索応答を返さない
  - 低信頼の確認プロンプト抑止にも影響

---

## 4. LINEコマンド仕様

### 4.1 予定登録

```text
予定登録 2026-05-07 14:30 60 定例ミーティング
```

- 形式: `予定登録 YYYY-MM-DD HH:mm [durationMin] タイトル`
- `durationMin` 省略時は60分

### 4.2 予定確認

```text
予定確認 今日
予定確認 来週
予定確認 2026-05-07
予定確認 2026年5月
予定確認 今後
```

### 4.3 会話検索

```text
会話検索 試飲会
会話検索 120日 発注
会話検索 全期間 notebook lm
会話検索 このルーム 人参
会話検索 全ルーム 価格
```

### 4.4 pending確認中の修正

```text
場所をmarugoに変更
時間を19:00に変更
件名をシェフミーティングに変更
```

---

## 5. 管理API（admin-api）

認証: すべて `x-admin-token` 必須

- `GET /state`
  - 全体設定、ルーム設定、ルーム概要、配信ログ、ストレージ使用量
- `GET /gmail/account`
  - Gmail連携状態チェック
- `GET /media`
  - メディア一覧
- `DELETE /media/:id`
  - メディア削除
- `GET /documents`
  - 資料一覧
- `POST /documents`
  - 資料アップロード（TXT/PDF）
- `DELETE /documents/:id`
  - 資料削除
- `PUT /settings/media-upload-limit`
  - メディア上限MB変更
- `PUT /auth/token`
  - 管理トークン更新（DBにはハッシュ保存）
- `PUT /settings/global`
  - 全体設定更新
- `PUT /settings/rooms`
  - ルーム設定 upsert
- `DELETE /settings/rooms/:room_id`
  - ルーム設定のみ削除
- `DELETE /rooms/:room_id`
  - ルーム本体データ削除（messages/media/documents/settings）
- `POST /actions/run-summary`
  - 要約処理を手動実行

---

## 6. データモデル（主要テーブル/バケット）

### テーブル

- `line_messages`
  - LINE受信メッセージ本体
- `summary_settings`
  - 全体設定（配信時刻、保持期間、翌日通知、管理トークンハッシュ等）
- `room_summary_settings`
  - ルーム個別設定（有効/自動応答/自動登録/会話検索/Gmail通知/並び順など）
- `summary_delivery_logs`
  - 配信/通知実行ログ
- `calendar_pending_confirmations`
  - カレンダー確認待ち候補
- `line_message_media`
  - LINEメディアメタデータ
- `line_search_documents`
  - 検索資料メタデータ（TXT/PDF）
- `gmail_reservation_alert_logs`
  - Gmail通知済み記録

### Storageバケット

- `line-media`
- `line-documents`

### 代表RPC

- `invoke_summary_cron(force_run boolean)`
- `invoke_gmail_alert_cron()`
- `get_room_overview()`
- `get_storage_usage_stats()`
- `get_line_media_usage_stats(...)`
- `get_line_document_usage_stats(...)`

---

## 7. 環境変数（Secrets）

### 7.1 必須（基本動作）

- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`
- `LINE_CHANNEL_SECRET`
- `LINE_CHANNEL_ACCESS_TOKEN`
- `LINE_OVERALL_ROOM_ID`
- `ADMIN_DASHBOARD_TOKEN`
- `GROQ_API_KEY`

### 7.2 Google Calendar連携

- `GOOGLE_CALENDAR_ID`
- `GOOGLE_SERVICE_ACCOUNT_EMAIL`
- `GOOGLE_SERVICE_ACCOUNT_PRIVATE_KEY`
- `GOOGLE_CALENDAR_TIMEZONE`（省略時 `Asia/Tokyo`）

### 7.3 Gmail通知

- `GMAIL_ALERT_ENABLED`
- `GMAIL_CLIENT_ID`
- `GMAIL_CLIENT_SECRET`
- `GMAIL_REFRESH_TOKEN`
- `GMAIL_ALERT_QUERY`（任意）
- `GMAIL_ALERT_MAX_MESSAGES`（任意）
- `LINE_GMAIL_ALERT_ROOM_ID`（任意）
- `GMAIL_ALERT_AI_ENABLED`（任意）
- `GMAIL_ALERT_AI_MAX_BODY_CHARS`（任意）

### 7.4 AI登録制御

- `CALENDAR_AI_AUTO_CREATE_ENABLED`
  - `false` で通常のAI自動登録を抑止
  - ただしサイレント運用時の登録動作は別条件で発生し得る（現行仕様）

### 7.5 診断

- `SUPABASE_DB_URL`（`check-cron` 用）

---

## 8. セットアップ

1. プロジェクトを Supabase にリンク

```bash
supabase link --project-ref <project-ref>
```

2. DBマイグレーション適用

```bash
supabase db push
```

3. Secrets設定

```bash
supabase secrets set KEY=VALUE --project-ref <project-ref>
```

4. Functionsデプロイ

```bash
supabase functions deploy line-webhook --project-ref <project-ref>
supabase functions deploy summary-cron --project-ref <project-ref>
supabase functions deploy gmail-alert-cron --project-ref <project-ref>
supabase functions deploy admin-api --project-ref <project-ref>
supabase functions deploy admin-ui --project-ref <project-ref>
supabase functions deploy check-cron --project-ref <project-ref>
```

5. LINE Developers で Webhook URL 設定

```text
https://<project-ref>.supabase.co/functions/v1/line-webhook
```

---

## 9. スケジュール運用（pg_cron）

- `summary-cron-job`: 要約配信
- `gmail-alert-cron-job`: 毎分実行のGmail通知

推奨:
- DBの `custom.edge_function_url` / `custom.gmail_alert_edge_function_url` / `custom.cron_auth_token` を適切に設定し、
  フォールバック値に依存しない運用にする

---

## 10. 管理UIの運用ポイント

- `index.html`:
  - 静的管理画面（`Project URL` は固定表示）
  - トークンはブラウザ `localStorage` 保存
- `admin-dashboard/index.html`:
  - 互換用リダイレクト（`../index.html` に転送）
- `admin-ui`:
  - Edge Function配信版UI
- ルーム並び替え:
  - ドラッグ＆ドロップで順序変更
  - 変更時に `room_sort_order` を保存
- テーブルUI:
  - 横スクロール時に主要列を固定表示

---

## 11. 制限事項・既知仕様

- LINE reply API制限に合わせ、返信は最大5メッセージに分割
- 会話検索は大量ヒット時に分割・省略案内が入る
- 資料アップロード上限は20MB未満（TXT/PDFのみ）
- 資料アップロードの本文抽出は現状 `text/plain` 系のみ
- PDFはアップロード可能だが、本文抽出/OCRは未実装（主にファイル名一致検索）
- メディア総容量キャップは500MB

## 11.1 設定値の制約

- `message_retention_days`: `0/60/120/180/365/730/1095`
- `media_upload_max_mb`: `1..20`
- `calendar_tomorrow_reminder_max_items`: `1..50`
- `delivery_hours`, `calendar_tomorrow_reminder_hours`: `0..23` の整数配列
- `daily_rollup` を使う場合:
  - 全体設定: `message_cleanup_timing=end_of_day` が必須
  - ルーム設定: `message_cleanup_timing=end_of_day` または `null(継承)` が必要

---

## 12. セキュリティ

- APIキー/秘密鍵はGitにコミットしない
- `admin-api` は `x-admin-token` 必須
- 管理トークンはDBにSHA-256ハッシュで保持（平文を保存しない）
- 比較はタイミング攻撃耐性を意識した `secureEqual` を使用
- UI通信は `referrerPolicy: no-referrer`
- 外部CDNスクリプト依存を除去
- GitHub Actions:
  - Secret Scan（push/PR/週次）
  - 月次ローテーションReminder Issue

---

## 13. トラブルシュート

- 予定登録できない
  - `GOOGLE_CALENDAR_*` の不足、サービスアカウント共有設定を確認
- 要約が配信されない
  - `summary_settings.is_enabled`, 配信時刻、`LINE_OVERALL_ROOM_ID` を確認
- 管理画面が401
  - `ADMIN_DASHBOARD_TOKEN` / DBハッシュ更新後の入力値を確認
- Gmail通知が来ない
  - `GMAIL_ALERT_ENABLED`, OAuth情報, 対象ルーム設定を確認
- cron状態を確認したい
  - `check-cron` を実行し、job定義と設定を確認

---

## 14. 変更履歴（2026-04-11）

### セキュリティ強化

- `media.html` の外部CDN `pdf.js` 動的読み込みを削除
- 管理画面/メディア画面/API経由UIに `referrerPolicy: no-referrer` を適用
- `index.html` / `media.html` / `admin-ui` の外部フォント `@import` を削除
- 資料アップロード時、クライアント送信 `extracted_text` を受け付けない実装に変更

### 機能修正・品質改善

- 会話検索の期間条件を資料検索にも適用
- 資料一覧ルーム名を `room_summary_settings` 優先で表示
- 資料アップロード時の二重送信（`file` + `extracted_text`）を廃止
