# Admin Dashboard

管理画面はプロジェクト直下の `index.html` です。  
このフォルダの `index.html` は互換用リダイレクトです。

## 使い方

1. ブラウザでプロジェクト直下の `index.html` を開く  
2. `Project URL` は固定（`https://jhpmzqxqvapdkyvvhyra.supabase.co`）  
3. `ADMIN_DASHBOARD_TOKEN` を入力して接続

## 対応機能

- 全体配信設定（ON/OFF、配信時刻）
- 消去タイミング設定（配信ごと / 1日の最終配信後）
- 最終配信の集計方式設定（各回独立 / 最終回のみ1日まとめ）
- ルーム別配信設定（有効/無効、時刻上書き、継承に戻す）
- 配信ログ確認
- 手動で要約実行

## トークン再発行

必要なら次で再発行できます。

```bash
TOKEN=$(node -e "console.log(require('crypto').randomBytes(24).toString('base64url'))")
supabase secrets set ADMIN_DASHBOARD_TOKEN="$TOKEN" --project-ref jhpmzqxqvapdkyvvhyra
echo "$TOKEN"
```
