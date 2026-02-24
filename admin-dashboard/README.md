# Admin Dashboard

このフォルダの `index.html` が管理画面です。

## 使い方

1. ブラウザで `index.html` を開く  
2. `Project URL` に Supabase の URL を入力  
3. `ADMIN_DASHBOARD_TOKEN` を入力して接続

## 対応機能

- 全体配信設定（ON/OFF、配信時刻）
- ルーム別配信設定（有効/無効、時刻上書き、継承に戻す）
- 配信ログ確認
- 手動で要約実行

## トークン再発行

必要なら次で再発行できます。

```bash
TOKEN=$(node -e "console.log(require('crypto').randomBytes(24).toString('base64url'))")
supabase secrets set ADMIN_DASHBOARD_TOKEN="$TOKEN" --project-ref ppuzcvstdknliqbendaz
echo "$TOKEN"
```
