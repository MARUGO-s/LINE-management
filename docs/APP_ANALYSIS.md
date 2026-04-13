# LINE Management アプリケーション 総合解析レポート（最新版）

本ドキュメントは、現在の `main` 実装に合わせて、主要機能・運用・セキュリティ対策を要約した分析メモです。

## 1. アプリケーション概要

LINEグループ/ルームの会話を蓄積し、AI（Groq）を活用して以下を実行する運用ボットです。

- 会話要約の定期配信
- Google Calendar への予定登録/照会/変更
- 過去会話検索（第1段 **過去180日一括** → 0件なら第2段で保持いっぱいまで会話を連鎖 → 会話でも0件なら第3段で **資料ライブラリ**の確認。明示 `会話検索フル` は第1・2段省略。早期ヒット時は **古い帯の追加検索**の確認可）
- Gmail予約通知のLINE配信
- LINEメディアの保存・閲覧

管理画面は `index.html`（静的）と `admin-ui`（Edge配信HTML）で提供され、設定変更は `admin-api` 経由で即時反映されます。

---

## 2. コア機能（現行仕様）

### 2.1 LINE Webhook（`line-webhook`）
- LINE署名検証（`LINE_CHANNEL_SECRET`）を実施。
- AI意図判定と明示コマンドの併用で、予定登録/一覧/変更・会話検索を処理。
- **会話検索**: 第1段は `MESSAGE_SEARCH_NORMAL_MAX_DAYS`（180 日）相当を一括検索。0 件かつ保持がより長い場合は **同一応答内で** フル相当の段階検索に進む。明示 **`会話検索フル`**（`detectFullRetentionSearchRequest`）は第1段を省略。詳細は `README.md` §8.0 / §8.1。
- **検索と LLM の分離**: 会話・資料のキーワード検索は **DB 保存テキストへのルール照合**で **Groq 不使用**。蓄積量が増えても **検索＝全文 LLM 投入** にはならず、**Groq 従量を抑える設計**（`README.md` §7.1 / §8.5）。
- 低信頼の予定登録は pending にして確認し、期限切れは `calendar-pending-cron` が自動処理。
- LINEメディア（image/video/audio/file）を `line-media` へ保存。
  - **画像**（`jpeg` / `jpg` / `png`）は `GROQ_API_KEY` がある場合、Groq **`meta-llama/llama-4-scout-17b-16e-instruct`** で短文の内容説明を生成し `line_message_media.content_preview` に保存。`media.html` で **何が写っているか**を確認できる（詳細は `docs/MEDIA_OCR_AND_CLASSIFICATION.md` §1.3）。
  - 1ファイル絶対上限: 20MB
  - 合計上限: 2GB
- 共有レート制限（DB-backed）を適用。
  - IP単位: 120 req / 60秒
  - source単位: 90 events / 60秒

### 2.2 要約バッチ（`summary-cron`）
- 毎時実行（`summary-cron-job`）で、配信時刻・ルーム設定に応じて要約を送信。
- 翌日予定通知・保持期間に基づくクリーンアップを実施。

### 2.3 Gmail通知（`gmail-alert-cron`）
- Gmail API を監視し、予約関連通知を抽出してLINEへ配信。
- AI抽出の有効化/上限文字数は環境変数で制御。

### 2.4 カレンダー pending 処理（`calendar-pending-cron`）
- 期限切れ確認待ちを定期処理し、運用ルールに沿って確定/仮登録を進行。

### 2.5 管理API（`admin-api`）
- `x-admin-token` 認証必須（DBのハッシュ照合 + fallback token）。
- グローバル設定/ルーム設定/ユーザー権限/ログ/メディア/資料管理を提供。
- 資料アップロードはサーバ側抽出のみ。
  - 受理形式: TXT/PDF/DOCX/XLSX
  - 上限: 20MB未満
  - PDF/ZIPシグネチャ検査、Officeアーカイブ安全検査（ZIP爆弾対策）
- 共有レート制限（DB-backed）を適用。
  - 通常API: 180 req / 60秒
  - `POST /documents`: 12 req / 60秒

---

## 3. データ/インフラ構成

| コンポーネント | 役割 |
|---|---|
| Supabase Postgres | メッセージ・設定・ログ・pending・権限・レート制限カウンタ |
| Supabase Storage | `line-media` / `line-documents` の実ファイル |
| Edge Functions | `line-webhook` / `summary-cron` / `gmail-alert-cron` / `calendar-pending-cron` / `admin-api` / `admin-ui` / `check-cron` |
| LINE Messaging API | 受信Webhook / reply / push |
| Google Calendar API | 予定作成・照会・更新 |
| Gmail API | 予約通知メール取得 |
| Groq API | 意図判定・要約・抽出・画像の内容説明（Llama 4 Scout、`line-webhook`） |

---

## 4. セキュリティ実装（最新）

### 4.1 Secrets管理
- 平文シークレットはGitに残さない方針。
- cron invoker 用トークン解決は `resolve_edge_cron_auth_token()` に統一。
  1. `custom.cron_auth_token`
  2. Vault: `CRON_AUTH_TOKEN`
  3. Vault: `SUPABASE_ANON_KEY`
- 解決不可時は invoker は安全にスキップ（warningログ）。

### 4.2 ファイル攻撃対策
- MIME/拡張子だけでなく magic bytes を確認。
- Office ZIPは以下を検査。
  - エントリ数上限
  - 単一エントリ展開サイズ上限
  - 展開後合計サイズ上限
  - 圧縮率上限
  - 危険パス（`../` など）拒否
  - 必須エントリ存在確認（DOCX/XLSX）

### 4.3 レート制限（分散対応）
- `security_rate_limits` + `consume_security_rate_limit(...)` でDB共有カウンタ化。
- Edgeインスタンス分散時でも制限が一貫して動作。

### 4.4 レート制限履歴の自動保守
- `cleanup_security_rate_limits(interval '2 days')`
- `security-rate-limit-cleanup-job` が毎日実行し古い履歴を削除。

---

## 5. 主要な上限値（運用上重要）

- メディア総量: 2GB
- メディア単体絶対上限: 20MB
- 資料アップロード上限: 20MB未満
- `message_retention_days`: `0 / 60 / 120 / 180 / 365 / 730 / 1095`

---

## 6. 現時点の総評

- 機能面: 要約・検索・カレンダー・Gmail通知・メディア運用まで一貫運用可能。
- セキュリティ面: Secrets統一、ZIP爆弾対策、分散対応レート制限、日次クリーンアップまで導入済み。
- 運用面: リポジトリ直下の `README.md` と `docs/SECURITY_AUTOMATION.md` を最新仕様に更新済みで、監視SQLも整備済み。
