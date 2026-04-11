import "jsr:@supabase/functions-js/edge-runtime.d.ts"

const html = String.raw`<!doctype html>
<html lang="ja">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <meta name="referrer" content="no-referrer">
  <title>LINE Summary Admin</title>
  <style>
    :root {
      --bg0: #071422;
      --bg1: #132c3f;
      --card: rgba(7, 20, 34, 0.82);
      --card-alt: rgba(19, 44, 63, 0.78);
      --line: rgba(149, 219, 255, 0.28);
      --txt: #eaf6ff;
      --txt-sub: #b8d8ea;
      --ok: #65f4c7;
      --warn: #ffd07c;
      --bad: #ff7a9f;
      --accent: #6ce6ff;
      --accent-strong: #19bdff;
      --radius: 18px;
      --shadow: 0 20px 45px rgba(0, 0, 0, 0.35);
    }

    * {
      box-sizing: border-box;
    }

    body {
      margin: 0;
      min-height: 100vh;
      color: var(--txt);
      font-family: "Zen Kaku Gothic New", sans-serif;
      background:
        radial-gradient(1200px 600px at -20% -10%, #1f5f86 0%, transparent 60%),
        radial-gradient(800px 500px at 110% 0%, #184f76 0%, transparent 55%),
        linear-gradient(145deg, var(--bg0), var(--bg1));
      padding: 26px;
    }

    .shell {
      max-width: 1320px;
      margin: 0 auto;
      display: grid;
      gap: 18px;
    }

    .hero {
      position: relative;
      overflow: hidden;
      padding: 22px 24px;
      border-radius: var(--radius);
      border: 1px solid var(--line);
      background: linear-gradient(160deg, rgba(108, 230, 255, 0.12), rgba(25, 189, 255, 0.06));
      box-shadow: var(--shadow);
    }

    .hero::after {
      content: "";
      position: absolute;
      inset: -40% -25% auto auto;
      width: 340px;
      height: 340px;
      border-radius: 50%;
      background: radial-gradient(circle, rgba(108, 230, 255, 0.34), transparent 65%);
      pointer-events: none;
    }

    .hero h1 {
      margin: 0;
      font-family: "Barlow Condensed", sans-serif;
      letter-spacing: 0.02em;
      font-size: clamp(2rem, 3.5vw, 3rem);
      line-height: 0.95;
      text-transform: uppercase;
    }

    .hero p {
      margin: 10px 0 0;
      color: var(--txt-sub);
      font-size: 0.98rem;
    }

    .status-row {
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      gap: 8px 10px;
      margin-top: 15px;
      color: var(--txt-sub);
      font-size: 0.88rem;
    }

    .pill {
      border-radius: 999px;
      padding: 5px 11px;
      border: 1px solid var(--line);
      background: rgba(7, 20, 34, 0.56);
      color: var(--txt-sub);
    }

    .pill.ok {
      border-color: color-mix(in srgb, var(--ok) 60%, transparent);
      color: var(--ok);
    }

    .pill.link {
      color: #9be8ff;
      text-decoration: none;
      display: inline-flex;
      align-items: center;
    }

    .panel-grid {
      display: grid;
      gap: 18px;
      grid-template-columns: repeat(12, minmax(0, 1fr));
    }

    .card {
      border-radius: var(--radius);
      border: 1px solid var(--line);
      background: var(--card);
      box-shadow: var(--shadow);
      padding: 18px;
    }

    .card h2 {
      margin: 0 0 12px;
      font-size: 1.1rem;
      letter-spacing: 0.02em;
    }

    .card.auth {
      grid-column: span 12;
      background: var(--card-alt);
    }

    .card.global {
      grid-column: span 12;
    }

    .card.logs {
      grid-column: span 12;
    }

    .card.rooms {
      grid-column: span 12;
    }

    .controls {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
    }

    .input,
    .select,
    .button {
      border-radius: 12px;
      border: 1px solid var(--line);
      background: rgba(7, 20, 34, 0.58);
      color: var(--txt);
      font: inherit;
      min-height: 40px;
      padding: 0 12px;
    }

    .input::placeholder {
      color: #8fb2c7;
    }

    .input.narrow {
      width: 120px;
    }

    .button {
      cursor: pointer;
      font-weight: 700;
      letter-spacing: 0.02em;
      transition: transform 140ms ease, border-color 140ms ease, background 140ms ease;
    }

    .button:hover {
      transform: translateY(-1px);
      border-color: var(--accent);
      background: rgba(25, 189, 255, 0.16);
    }

    .button.primary {
      background: linear-gradient(180deg, rgba(25, 189, 255, 0.34), rgba(11, 120, 166, 0.34));
      border-color: rgba(108, 230, 255, 0.6);
    }

    .button.warn {
      border-color: color-mix(in srgb, var(--warn) 60%, transparent);
    }

    .button.ghost {
      border-color: rgba(159, 193, 213, 0.28);
    }

    .switch {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      margin-top: 4px;
      font-size: 0.93rem;
    }

    .switch input {
      width: 18px;
      height: 18px;
      accent-color: var(--accent-strong);
    }

    .meta {
      color: var(--txt-sub);
      font-size: 0.88rem;
      margin-top: 8px;
    }

    .meta.usage-summary {
      color: #9be8ff;
      font-weight: 700;
    }

    .meta.usage-details {
      line-height: 1.45;
    }

    .table-wrap {
      overflow: auto;
      -webkit-overflow-scrolling: touch;
      border-radius: 12px;
      border: 1px solid rgba(149, 219, 255, 0.18);
      background: rgba(7, 20, 34, 0.5);
    }

    .log-table-wrap {
      max-height: 520px;
      overflow-x: auto;
      overflow-y: auto;
    }

    table {
      width: 100%;
      border-collapse: collapse;
      min-width: 860px;
    }

    .log-table {
      width: max(100%, 1160px);
      min-width: 1160px;
      table-layout: fixed;
    }

    .rooms-table {
      width: max(100%, 2240px);
      min-width: 2240px;
      table-layout: fixed;
    }

    .rooms-table th,
    .rooms-table td {
      white-space: nowrap;
    }

    .rooms-table th:nth-child(1), .rooms-table td:nth-child(1) { width: 128px; text-align: center; }
    .rooms-table th:nth-child(2), .rooms-table td:nth-child(2) { width: 300px; }
    .rooms-table th:nth-child(3), .rooms-table td:nth-child(3) { width: 90px; text-align: center; }
    .rooms-table th:nth-child(4), .rooms-table td:nth-child(4) { width: 150px; }
    .rooms-table th:nth-child(5), .rooms-table td:nth-child(5) { width: 70px; text-align: center; }
    .rooms-table th:nth-child(6), .rooms-table td:nth-child(6) { width: 110px; text-align: center; }
    .rooms-table th:nth-child(7), .rooms-table td:nth-child(7) { width: 110px; text-align: center; }
    .rooms-table th:nth-child(8), .rooms-table td:nth-child(8) { width: 90px; text-align: center; }
    .rooms-table th:nth-child(9), .rooms-table td:nth-child(9) { width: 90px; text-align: center; }
    .rooms-table th:nth-child(10), .rooms-table td:nth-child(10) { width: 160px; text-align: center; }
    .rooms-table th:nth-child(11), .rooms-table td:nth-child(11) { width: 90px; text-align: center; }
    .rooms-table th:nth-child(12), .rooms-table td:nth-child(12) { width: 110px; text-align: center; }
    .rooms-table th:nth-child(13), .rooms-table td:nth-child(13) { width: 160px; }
    .rooms-table th:nth-child(14), .rooms-table td:nth-child(14) { width: 170px; }
    .rooms-table th:nth-child(15), .rooms-table td:nth-child(15) { width: 150px; }
    .rooms-table th:nth-child(16), .rooms-table td:nth-child(16) { width: 220px; }

    /* Keep ID / Display Name fixed while horizontally scrolling */
    .rooms-table td:nth-child(1),
    .rooms-table td:nth-child(2) {
      position: sticky;
      background: rgba(7, 20, 34, 0.98);
      z-index: 2;
    }

    .rooms-table th:nth-child(1),
    .rooms-table th:nth-child(2) {
      position: sticky;
      background: rgba(7, 20, 34, 0.98);
      z-index: 6;
    }

    .rooms-table th:nth-child(1),
    .rooms-table td:nth-child(1) {
      left: 0;
    }

    .rooms-table th:nth-child(2),
    .rooms-table td:nth-child(2) {
      left: 128px;
      box-shadow: 1px 0 0 rgba(149, 219, 255, 0.2) inset;
    }

    .rooms-table .room-name,
    .rooms-table .room-hours {
      width: 100%;
      min-width: 0;
    }

    .rooms-table .room-check {
      width: 18px;
      height: 18px;
      accent-color: var(--accent-strong);
    }

    .rooms-table .room-show-id {
      min-height: 30px;
      padding: 0 10px;
      font-size: 0.74rem;
    }

    .rooms-table .room-id-tools {
      display: inline-flex;
      align-items: center;
      gap: 6px;
    }

    .rooms-table .room-drag-handle {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      width: 18px;
      height: 18px;
      border-radius: 4px;
      cursor: grab;
      color: #9bd5f8;
      border: 1px solid rgba(149, 219, 255, 0.25);
      background: rgba(7, 20, 34, 0.65);
      user-select: none;
      -webkit-user-drag: element;
      font-size: 0.72rem;
      line-height: 1;
    }

    .rooms-table tr.dragging .room-drag-handle {
      cursor: grabbing;
    }

    .rooms-table tr.dragging td {
      opacity: 0.82;
    }

    .log-table th:nth-child(1),
    .log-table td:nth-child(1) { width: 176px; }
    .log-table th:nth-child(2),
    .log-table td:nth-child(2) { width: 220px; }
    .log-table th:nth-child(4),
    .log-table td:nth-child(4) {
      width: 120px;
      white-space: nowrap;
    }
    .log-table th:nth-child(5),
    .log-table td:nth-child(5) {
      width: 90px;
      white-space: nowrap;
    }
    .log-table th:nth-child(3),
    .log-table td:nth-child(3) {
      white-space: normal;
      word-break: normal;
      overflow-wrap: break-word;
    }

    .log-table .tag {
      white-space: nowrap;
      line-height: 1.2;
    }

    th,
    td {
      border-bottom: 1px solid rgba(149, 219, 255, 0.18);
      padding: 10px;
      font-size: 0.84rem;
      vertical-align: middle;
      text-align: left;
    }

    th {
      color: #9bd5f8;
      font-size: 0.77rem;
      letter-spacing: 0.04em;
      text-transform: uppercase;
      position: sticky;
      top: 0;
      background: rgba(7, 20, 34, 0.94);
      z-index: 1;
    }

    .tag {
      display: inline-block;
      border-radius: 999px;
      font-size: 0.72rem;
      font-weight: 700;
      letter-spacing: 0.03em;
      padding: 4px 9px;
      border: 1px solid rgba(149, 219, 255, 0.3);
      color: #9fd4ee;
      background: rgba(11, 34, 52, 0.78);
      text-transform: uppercase;
    }

    .tag.ok { color: var(--ok); border-color: color-mix(in srgb, var(--ok) 60%, transparent); }
    .tag.warn { color: var(--warn); border-color: color-mix(in srgb, var(--warn) 60%, transparent); }
    .tag.bad { color: var(--bad); border-color: color-mix(in srgb, var(--bad) 60%, transparent); }

    .row-actions {
      display: inline-flex;
      flex-wrap: nowrap;
      gap: 6px;
      align-items: center;
    }

    .row-actions .button {
      min-height: 32px;
      font-size: 0.76rem;
      padding: 0 9px;
      white-space: nowrap;
    }

    .modal-backdrop {
      position: fixed;
      inset: 0;
      display: none;
      align-items: center;
      justify-content: center;
      padding: 18px;
      background: rgba(2, 10, 18, 0.68);
      z-index: 1000;
    }

    .modal-backdrop.open {
      display: flex;
    }

    .modal-card {
      width: min(560px, 100%);
      border-radius: 14px;
      border: 1px solid rgba(155, 220, 255, 0.32);
      background: linear-gradient(160deg, rgba(8, 21, 35, 0.96), rgba(12, 34, 52, 0.94));
      box-shadow: 0 24px 52px rgba(0, 0, 0, 0.45);
      padding: 16px;
    }

    .modal-title {
      margin: 0;
      font-size: 1rem;
      color: #d2ecff;
      letter-spacing: 0.02em;
    }

    .modal-meta {
      margin-top: 8px;
      font-size: 0.84rem;
      color: #9bc3da;
    }

    .modal-room-id {
      display: block;
      margin-top: 10px;
      padding: 10px 12px;
      border-radius: 10px;
      border: 1px solid rgba(155, 220, 255, 0.24);
      background: rgba(5, 17, 28, 0.7);
      color: #e3f4ff;
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
      font-size: 0.84rem;
      line-height: 1.4;
      white-space: normal;
      word-break: break-all;
    }

    .modal-actions {
      display: flex;
      justify-content: flex-end;
      gap: 8px;
      margin-top: 12px;
    }

    .muted {
      color: #91b2c7;
      font-size: 0.8rem;
    }

    .empty {
      padding: 16px;
      text-align: center;
      color: #9dbed2;
      font-size: 0.9rem;
    }

    @media (max-width: 1080px) {
      body { padding: 16px; }
      .card.global, .card.logs { grid-column: span 12; }
      .hero { padding: 16px 16px 18px; }
    }
  </style>
</head>
<body>
  <div class="shell">
    <header class="hero">
      <h1>Line Summary Console</h1>
      <p>配信時刻・配信回数（時刻数）・ルーム別配信設定・配信ログを一括管理します。</p>
      <div class="status-row">
        <span id="authState" class="pill">未接続</span>
        <span class="pill">API: <strong>/functions/v1/admin-api</strong></span>
        <span id="lastRefresh" class="pill">最終更新: なし</span>
        <a class="pill link" href="https://marugo-s.github.io/LINE-management/media.html" target="_blank" rel="noopener noreferrer">LINEメディアビューアー</a>
      </div>
    </header>

    <main class="panel-grid">
      <section class="card auth">
        <h2>管理トークン</h2>
        <div class="controls">
          <input id="tokenInput" class="input" type="password" placeholder="ADMIN_DASHBOARD_TOKEN を入力">
          <button id="saveTokenBtn" class="button primary">保存して接続</button>
          <button id="clearTokenBtn" class="button ghost">削除</button>
          <button id="reloadBtn" class="button">再読み込み</button>
          <button id="runNowBtn" class="button warn">今すぐ要約実行</button>
          <a class="button" href="https://marugo-s.github.io/LINE-management/media.html" target="_blank" rel="noopener noreferrer" style="display:inline-flex;align-items:center;text-decoration:none;">メディア閲覧</a>
        </div>
        <div class="controls" style="margin-top:8px;">
          <button id="checkGmailAccountBtn" class="button">Gmail連携先を確認</button>
          <span id="gmailAccountMeta" class="pill">Gmail連携先: 未確認</span>
        </div>
        <div class="controls" style="margin-top:8px;">
          <input id="newTokenInput" class="input" type="password" placeholder="新しい管理トークン">
          <input id="newTokenConfirmInput" class="input" type="password" placeholder="新しい管理トークン（確認）">
          <button id="changeTokenBtn" class="button warn">トークン変更</button>
        </div>
        <div class="meta">トークンはブラウザの LocalStorage に保存されます。</div>
      </section>

      <section class="card global">
        <h2>全体設定</h2>
        <label class="switch"><input id="globalEnabled" type="checkbox">配信を有効化</label>
        <div class="controls" style="margin-top:10px;">
          <input id="globalHoursInput" class="input" type="text" placeholder="例: 12,17,23">
          <select id="messageCleanupTiming" class="select" aria-label="メッセージ処理タイミング">
            <option value="after_each_delivery">配信成功ごとに処理済み化</option>
            <option value="end_of_day">1日の最終配信後に処理済み化</option>
          </select>
          <select id="lastDeliverySummaryMode" class="select" aria-label="最終配信の集計方式">
            <option value="independent">各回独立で要約（従来）</option>
            <option value="daily_rollup">最終配信のみ1日まとめ</option>
          </select>
          <select id="messageRetentionDays" class="select" aria-label="メッセージ保持期間">
            <option value="365">会話保持: 1年（推奨）</option>
            <option value="730">会話保持: 2年</option>
            <option value="1095">会話保持: 3年</option>
            <option value="0">会話保持: 無制限</option>
            <option value="60">会話保持: 60日</option>
            <option value="120">会話保持: 120日</option>
            <option value="180">会話保持: 180日</option>
          </select>
          <button id="saveGlobalBtn" class="button primary">全体設定を保存</button>
        </div>
        <div class="controls" style="margin-top:10px;">
          <label class="switch"><input id="tomorrowReminderEnabled" type="checkbox">翌日予定通知を有効化</label>
          <input id="tomorrowReminderHoursInput" class="input" type="text" placeholder="翌日予定通知の時刻 例: 19">
          <select id="tomorrowReminderOnlyIfEvents" class="select" aria-label="予定なし時の通知">
            <option value="false">予定なしの日も通知する</option>
            <option value="true">予定がある日だけ通知する</option>
          </select>
          <input id="tomorrowReminderMaxItems" class="input narrow" type="number" min="1" max="50" step="1" placeholder="表示件数">
        </div>
        <div id="globalMeta" class="meta"></div>
        <div id="storageUsageSummary" class="meta usage-summary"></div>
        <div id="storageUsageDetails" class="meta usage-details"></div>
      </section>

      <section class="card logs">
        <h2>配信ログ（最新）</h2>
        <div class="table-wrap log-table-wrap">
          <table class="log-table">
            <thead>
              <tr>
                <th>実行時刻</th>
                <th>状態</th>
                <th>理由</th>
                <th>対象</th>
                <th>送信</th>
              </tr>
            </thead>
            <tbody id="logTableBody"></tbody>
          </table>
        </div>
      </section>

      <section class="card rooms">
        <h2>ルーム別設定</h2>
        <p class="muted" style="margin:0 0 10px 0;">運用ポリシーにより「会話検索応答」「資料検索(2段階目)」「自動応答」は固定制御です。ここでは誤設定を防ぐため表示していません。</p>
        <div class="controls">
          <input id="newRoomId" class="input" type="text" placeholder="新規 room_id">
          <input id="newRoomName" class="input" type="text" placeholder="表示名（任意）">
          <input id="newRoomHours" class="input narrow" type="text" placeholder="時刻: 12,17">
          <label class="switch"><input id="newRoomEnabled" type="checkbox" checked>有効</label>
          <label class="switch"><input id="newRoomSendSummary" type="checkbox">ルーム要約配信</label>
          <label class="switch"><input id="newRoomTomorrowReminder" type="checkbox">明日予定配信</label>
          <label class="switch"><input id="newRoomMediaFileAccessEnabled" type="checkbox" checked>メディアアクセス</label>
          <label class="switch"><input id="newRoomCalendarAutoCreate" type="checkbox" checked>自動登録</label>
          <label class="switch"><input id="newRoomSilentAutoRegister" type="checkbox">無返信即時登録（低確度は仮）</label>
          <label class="switch"><input id="newRoomGmailAlertEnabled" type="checkbox">Gmail予約通知</label>
          <select id="newRoomCleanupTiming" class="select" aria-label="ルーム処理タイミング">
            <option value="">処理: 全体設定を継承</option>
            <option value="after_each_delivery">処理: 配信成功ごと</option>
            <option value="end_of_day">処理: 1日の最終配信後</option>
          </select>
          <select id="newRoomSummaryMode" class="select" aria-label="ルーム最終配信の集計方式">
            <option value="">最終回: 全体設定を継承</option>
            <option value="independent">最終回: 各回独立</option>
            <option value="daily_rollup">最終回: 1日まとめ</option>
          </select>
          <button id="addRoomBtn" class="button primary">ルーム設定を追加</button>
        </div>
        <div class="table-wrap" style="margin-top:12px;">
          <table class="rooms-table">
            <thead>
              <tr>
                <th>ID</th>
                <th>表示名</th>
                <th>未処理件数</th>
                <th>最終投稿</th>
                <th>有効</th>
                <th>ルーム要約配信</th>
                <th>明日予定配信</th>
                <th>メディアアクセス</th>
                <th>自動登録</th>
                <th>無返信即時登録</th>
                <th>Gmail予約通知</th>
                <th>配信時刻</th>
                <th>処理タイミング</th>
                <th>最終回集計</th>
                <th>操作</th>
              </tr>
            </thead>
            <tbody id="roomTableBody"></tbody>
          </table>
        </div>
      </section>
    </main>
  </div>

  <div id="roomIdModal" class="modal-backdrop" aria-hidden="true">
    <div class="modal-card" role="dialog" aria-modal="true" aria-labelledby="roomIdModalTitle">
      <h3 id="roomIdModalTitle" class="modal-title">ルームID</h3>
      <div id="roomIdModalName" class="modal-meta">表示名: -</div>
      <code id="roomIdModalValue" class="modal-room-id">-</code>
      <div class="modal-actions">
        <button id="copyRoomIdBtn" class="button">コピー</button>
        <button id="closeRoomIdBtn" class="button primary">閉じる</button>
      </div>
    </div>
  </div>

  <script>
    const API_BASE = '/functions/v1/admin-api';
    const TOKEN_KEY = 'line_summary_admin_token';
    const NEW_ROOM_DEFAULT_ENABLED_KEY = 'line_summary_new_room_default_enabled';
    const NEW_ROOM_DEFAULT_SEND_SUMMARY_KEY = 'line_summary_new_room_default_send_summary';
    const NEW_ROOM_DEFAULT_TOMORROW_REMINDER_KEY = 'line_summary_new_room_default_tomorrow_reminder';
    const NEW_ROOM_DEFAULT_MEDIA_FILE_ACCESS_KEY = 'line_summary_new_room_default_media_file_access';
    const NEW_ROOM_DEFAULT_CALENDAR_AUTO_CREATE_KEY = 'line_summary_new_room_default_calendar_auto_create';
    const NEW_ROOM_DEFAULT_SILENT_AUTO_REGISTER_KEY = 'line_summary_new_room_default_silent_auto_register';
    const NEW_ROOM_DEFAULT_GMAIL_ALERT_KEY = 'line_summary_new_room_default_gmail_alert';
    const ROOM_DEFAULT_HOURS = '';
    const dom = {
      authState: document.getElementById('authState'),
      lastRefresh: document.getElementById('lastRefresh'),
      tokenInput: document.getElementById('tokenInput'),
      newTokenInput: document.getElementById('newTokenInput'),
      newTokenConfirmInput: document.getElementById('newTokenConfirmInput'),
      changeTokenBtn: document.getElementById('changeTokenBtn'),
      saveTokenBtn: document.getElementById('saveTokenBtn'),
      clearTokenBtn: document.getElementById('clearTokenBtn'),
      reloadBtn: document.getElementById('reloadBtn'),
      runNowBtn: document.getElementById('runNowBtn'),
      checkGmailAccountBtn: document.getElementById('checkGmailAccountBtn'),
      gmailAccountMeta: document.getElementById('gmailAccountMeta'),
      globalEnabled: document.getElementById('globalEnabled'),
      globalHoursInput: document.getElementById('globalHoursInput'),
      messageCleanupTiming: document.getElementById('messageCleanupTiming'),
      lastDeliverySummaryMode: document.getElementById('lastDeliverySummaryMode'),
      messageRetentionDays: document.getElementById('messageRetentionDays'),
      tomorrowReminderEnabled: document.getElementById('tomorrowReminderEnabled'),
      tomorrowReminderHoursInput: document.getElementById('tomorrowReminderHoursInput'),
      tomorrowReminderOnlyIfEvents: document.getElementById('tomorrowReminderOnlyIfEvents'),
      tomorrowReminderMaxItems: document.getElementById('tomorrowReminderMaxItems'),
      saveGlobalBtn: document.getElementById('saveGlobalBtn'),
      globalMeta: document.getElementById('globalMeta'),
      storageUsageSummary: document.getElementById('storageUsageSummary'),
      storageUsageDetails: document.getElementById('storageUsageDetails'),
      roomTableBody: document.getElementById('roomTableBody'),
      logTableBody: document.getElementById('logTableBody'),
      newRoomId: document.getElementById('newRoomId'),
      newRoomName: document.getElementById('newRoomName'),
      newRoomHours: document.getElementById('newRoomHours'),
      newRoomEnabled: document.getElementById('newRoomEnabled'),
      newRoomSendSummary: document.getElementById('newRoomSendSummary'),
      newRoomTomorrowReminder: document.getElementById('newRoomTomorrowReminder'),
      newRoomMediaFileAccessEnabled: document.getElementById('newRoomMediaFileAccessEnabled'),
      newRoomCalendarAutoCreate: document.getElementById('newRoomCalendarAutoCreate'),
      newRoomSilentAutoRegister: document.getElementById('newRoomSilentAutoRegister'),
      newRoomGmailAlertEnabled: document.getElementById('newRoomGmailAlertEnabled'),
      newRoomCleanupTiming: document.getElementById('newRoomCleanupTiming'),
      newRoomSummaryMode: document.getElementById('newRoomSummaryMode'),
      addRoomBtn: document.getElementById('addRoomBtn'),
      roomIdModal: document.getElementById('roomIdModal'),
      roomIdModalName: document.getElementById('roomIdModalName'),
      roomIdModalValue: document.getElementById('roomIdModalValue'),
      copyRoomIdBtn: document.getElementById('copyRoomIdBtn'),
      closeRoomIdBtn: document.getElementById('closeRoomIdBtn'),
    };
    const AUTO_REFRESH_MS_VISIBLE = 5000;
    const AUTO_REFRESH_MS_HIDDEN = 15000;
    let autoRefreshTimer = null;
    let isStateLoading = false;
    let currentState = null;
    let isRoomDirty = false;
    const roomAutoSaveInFlight = new Set();

    function token() {
      return localStorage.getItem(TOKEN_KEY) || '';
    }

    function setToken(value) {
      if (!value) {
        localStorage.removeItem(TOKEN_KEY);
      } else {
        localStorage.setItem(TOKEN_KEY, value);
      }
      syncAuthState();
    }

    function loadBooleanSetting(key, fallback) {
      const raw = localStorage.getItem(key);
      if (raw == null) return fallback;
      const normalized = String(raw).trim().toLowerCase();
      if (normalized === '1' || normalized === 'true' || normalized === 'yes' || normalized === 'on') return true;
      if (normalized === '0' || normalized === 'false' || normalized === 'no' || normalized === 'off') return false;
      return fallback;
    }

    function saveBooleanSetting(key, value) {
      localStorage.setItem(key, value ? '1' : '0');
    }

    function applyNewRoomDefaultCheckboxes() {
      dom.newRoomEnabled.checked = loadBooleanSetting(NEW_ROOM_DEFAULT_ENABLED_KEY, true);
      dom.newRoomSendSummary.checked = loadBooleanSetting(NEW_ROOM_DEFAULT_SEND_SUMMARY_KEY, false);
      dom.newRoomTomorrowReminder.checked = loadBooleanSetting(NEW_ROOM_DEFAULT_TOMORROW_REMINDER_KEY, false);
      dom.newRoomMediaFileAccessEnabled.checked = loadBooleanSetting(NEW_ROOM_DEFAULT_MEDIA_FILE_ACCESS_KEY, true);
      dom.newRoomCalendarAutoCreate.checked = loadBooleanSetting(NEW_ROOM_DEFAULT_CALENDAR_AUTO_CREATE_KEY, true);
      dom.newRoomSilentAutoRegister.checked = loadBooleanSetting(NEW_ROOM_DEFAULT_SILENT_AUTO_REGISTER_KEY, false);
      dom.newRoomGmailAlertEnabled.checked = loadBooleanSetting(NEW_ROOM_DEFAULT_GMAIL_ALERT_KEY, false);
    }

    function syncAuthState() {
      const hasToken = !!token();
      dom.authState.textContent = hasToken ? '接続準備OK' : '未接続';
      dom.authState.className = hasToken ? 'pill ok' : 'pill';
    }

    function parseHoursInput(raw, allowEmpty) {
      if (!raw || !raw.trim()) {
        return allowEmpty ? null : [];
      }
      const values = raw
        .split(',')
        .map(function(v) { return Number(v.trim()); })
        .filter(function(v) { return Number.isInteger(v); });

      const uniq = [];
      for (const num of values) {
        if (num < 0 || num > 23) {
          throw new Error('時刻は 0〜23 の整数で入力してください。');
        }
        if (!uniq.includes(num)) uniq.push(num);
      }
      uniq.sort(function(a, b) { return a - b; });

      if (!allowEmpty && uniq.length === 0) {
        throw new Error('配信時刻を1つ以上入力してください。');
      }
      return uniq;
    }

    function formatDate(iso) {
      if (!iso) return '-';
      const dt = new Date(iso);
      if (Number.isNaN(dt.getTime())) return '-';
      return dt.toLocaleString('ja-JP', { hour12: false });
    }

    function formatBytes(value) {
      const n = Number(value);
      if (!Number.isFinite(n) || n < 0) return '-';
      if (n < 1024) return Math.floor(n) + ' B';
      if (n < 1024 * 1024) return (n / 1024).toFixed(1) + ' KB';
      if (n < 1024 * 1024 * 1024) return (n / (1024 * 1024)).toFixed(2) + ' MB';
      return (n / (1024 * 1024 * 1024)).toFixed(2) + ' GB';
    }

    function statusTag(status) {
      const s = String(status || '').toLowerCase();
      if (s.includes('delivered') || s.includes('success') || s.endsWith('_sent')) return 'ok';
      if (s.includes('fail') || s.includes('error')) return 'bad';
      return 'warn';
    }

    function logReasonJa(row) {
      const status = String(row && row.status ? row.status : '').toLowerCase();
      const raw = String(row && row.reason ? row.reason : '').trim();
      const hasJa = /[ぁ-んァ-ヶ一-龠々]/.test(raw);
      const map = {
        no_messages: '未処理メッセージがないため、今回は配信対象がありません。',
        not_scheduled: 'この時間は配信スケジュール外のため、配信を実行しませんでした。',
        overall_schedule_skip: 'ルーム要約候補はありましたが、全体配信の時刻外のため全体配信をスキップしました。',
        line_config_missing: 'LINE配信設定（トークンまたは送信先）の不足により配信できませんでした。',
        no_room_summary: '要約対象がなく、ルーム要約を生成できませんでした。',
        line_send_failed: 'LINEへの送信に失敗しました。',
        gmail_alert_sent: 'Gmail予約メール通知を送信しました。',
        gmail_alert_send_failed: 'Gmail予約メール通知の送信に失敗しました。',
        calendar_tomorrow_sent: '翌日予定通知を送信しました。',
        calendar_tomorrow_send_failed: '翌日予定通知の送信に失敗しました。',
        llm_config_missing: 'GROQ APIキー未設定のため要約処理を実行できませんでした。',
        db_update_failed: '配信後のメッセージ状態更新に失敗しました。',
        delivered: '配信に成功しました。',
        delivered_no_messages_to_mark: '配信は成功しましたが、更新対象のメッセージはありませんでした。',
        delivered_with_room_failures: '配信は一部成功しましたが、ルーム別配信に失敗したものがあります。',
        runtime_error: '実行中に予期しないエラーが発生しました。',
      };
      if (map[status]) return map[status];
      if (hasJa && raw) return raw;
      return '理由の詳細は内部ログを確認してください。';
    }

    async function api(path, options) {
      const t = token();
      if (!t) throw new Error('先に管理トークンを設定してください。');

      const request = options || {};
      const headers = Object.assign({
        'x-admin-token': t,
        'Content-Type': 'application/json',
      }, request.headers || {});

      const response = await fetch(
        API_BASE + path,
        Object.assign({}, request, { headers, cache: 'no-store', referrerPolicy: 'no-referrer' }),
      );
      const text = await response.text();
      let data = {};
      try {
        data = text ? JSON.parse(text) : {};
      } catch (_) {
        data = { raw: text };
      }
      if (!response.ok) {
        throw new Error(data.error || ('API error: ' + response.status));
      }
      return data;
    }

    function nextAutoRefreshMs() {
      return document.visibilityState === 'visible' ? AUTO_REFRESH_MS_VISIBLE : AUTO_REFRESH_MS_HIDDEN;
    }

    function stopAutoRefresh() {
      if (autoRefreshTimer !== null) {
        clearTimeout(autoRefreshTimer);
        autoRefreshTimer = null;
      }
    }

    function isEditingRoomForm() {
      const active = document.activeElement;
      if (!(active instanceof HTMLElement)) return false;
      if (active.isContentEditable) return true;
      if (active.matches('input, textarea, select')) return true;
      return !!active.closest('#roomTableBody');
    }

    function markRoomDirty() {
      isRoomDirty = true;
    }

    function isAutoSaveToggle(target) {
      return target.classList.contains('room-enabled')
        || target.classList.contains('room-send-summary')
        || target.classList.contains('room-tomorrow-reminder')
        || target.classList.contains('room-media-file-access-enabled')
        || target.classList.contains('room-calendar-auto-create')
        || target.classList.contains('room-silent-auto-register')
        || target.classList.contains('room-gmail-alert-enabled');
    }

    async function autoSaveRoomToggle(tr) {
      const roomId = String(tr.dataset.roomId || '').trim();
      if (!roomId) return;
      if (roomAutoSaveInFlight.has(roomId)) return;
      roomAutoSaveInFlight.add(roomId);
      try {
        await saveRoomFromRow(tr);
      } catch (error) {
        await safeLoadState({ silent: true });
        throw error;
      } finally {
        roomAutoSaveInFlight.delete(roomId);
      }
    }

    async function safeLoadState(options) {
      const opts = options || {};
      if (isStateLoading) return;
      isStateLoading = true;
      try {
        await loadState();
      } catch (e) {
        if (!opts.silent) throw e;
        console.error(e);
      } finally {
        isStateLoading = false;
      }
    }

    async function safeCheckGmailAccount(options) {
      const opts = options || {};
      try {
        await checkGmailAccount();
      } catch (e) {
        renderGmailAccountState({ error: true });
        if (!opts.silent) throw e;
        console.error(e);
      }
    }

    function scheduleAutoRefresh() {
      stopAutoRefresh();
      if (!token()) return;
      autoRefreshTimer = window.setTimeout(async function() {
        if (!isEditingRoomForm() && !isRoomDirty) {
          await safeLoadState({ silent: true });
        }
        scheduleAutoRefresh();
      }, nextAutoRefreshMs());
    }

    function renderGlobal(settings) {
      dom.globalEnabled.checked = !!settings.is_enabled;
      dom.globalHoursInput.value = Array.isArray(settings.delivery_hours)
        ? settings.delivery_hours.join(',')
        : '12,17,23';
      dom.messageCleanupTiming.value = settings.message_cleanup_timing === 'end_of_day'
        ? 'end_of_day'
        : 'after_each_delivery';
      dom.lastDeliverySummaryMode.value = settings.last_delivery_summary_mode === 'daily_rollup'
        ? 'daily_rollup'
        : 'independent';
      const retentionDays = Number(settings.message_retention_days);
      dom.messageRetentionDays.value = ([0, 60, 120, 180, 365, 730, 1095].includes(retentionDays))
        ? String(retentionDays)
        : '365';
      dom.tomorrowReminderEnabled.checked = settings.calendar_tomorrow_reminder_enabled !== false;
      dom.tomorrowReminderHoursInput.value = Array.isArray(settings.calendar_tomorrow_reminder_hours)
        ? settings.calendar_tomorrow_reminder_hours.join(',')
        : '19';
      dom.tomorrowReminderOnlyIfEvents.value = settings.calendar_tomorrow_reminder_only_if_events ? 'true' : 'false';
      const maxItems = Number(settings.calendar_tomorrow_reminder_max_items);
      dom.tomorrowReminderMaxItems.value = Number.isInteger(maxItems) && maxItems >= 1 && maxItems <= 50
        ? String(maxItems)
        : '20';
      const count = Array.isArray(settings.delivery_hours) ? settings.delivery_hours.length : 0;
      const reminderHours = Array.isArray(settings.calendar_tomorrow_reminder_hours)
        ? settings.calendar_tomorrow_reminder_hours.join(',')
        : '19';
      dom.globalMeta.textContent =
        '配信回数: ' + count + '回/日'
        + '  |  消去: ' + cleanupTimingLabel(dom.messageCleanupTiming.value)
        + '  |  最終回: ' + summaryModeLabel(dom.lastDeliverySummaryMode.value)
        + '  |  会話保持: ' + messageRetentionLabel(dom.messageRetentionDays.value)
        + '  |  翌日予定通知: '
        + (dom.tomorrowReminderEnabled.checked ? ('ON (' + reminderHours + '時)') : 'OFF')
        + '  |  予定なし時: '
        + (dom.tomorrowReminderOnlyIfEvents.value === 'true' ? '送らない' : '送る')
        + '  |  表示上限: '
        + dom.tomorrowReminderMaxItems.value + '件'
        + '  |  更新: ' + formatDate(settings.updated_at);
    }

    function renderLogs(logs) {
      dom.logTableBody.innerHTML = '';
      const rows = Array.isArray(logs) ? logs.slice(0, 30) : [];
      if (rows.length === 0) {
        dom.logTableBody.innerHTML = '<tr><td class="empty" colspan="5">ログはありません。</td></tr>';
        return;
      }
      for (const row of rows) {
        const tr = document.createElement('tr');
        const tag = statusTag(row.status);
        tr.innerHTML =
          '<td>' + formatDate(row.run_at) + '</td>' +
          '<td><span class="tag ' + tag + '">' + escapeHtml(row.status || '-') + '</span></td>' +
          '<td>' + escapeHtml(logReasonJa(row)) + '</td>' +
          '<td>' + (row.rooms_targeted ?? 0) + ' rooms</td>' +
          '<td>' + (row.line_send_success ? '成功' : (row.line_send_attempted ? '失敗' : '未実行')) + '</td>';
        dom.logTableBody.appendChild(tr);
      }
    }

    function renderStorageUsage(storageUsage, storageUsageError, generatedAt) {
      if (storageUsageError) {
        dom.storageUsageSummary.textContent = 'DB使用容量: 取得失敗';
        dom.storageUsageDetails.textContent = storageUsageError;
        return;
      }

      if (!storageUsage || typeof storageUsage !== 'object') {
        dom.storageUsageSummary.textContent = 'DB使用容量: 取得待ち';
        dom.storageUsageDetails.textContent = '';
        return;
      }

      const managedPretty = storageUsage.managed_tables_total_pretty || formatBytes(storageUsage.managed_tables_total_bytes);
      const dbPretty = storageUsage.database_size_pretty || formatBytes(storageUsage.database_size_bytes);
      dom.storageUsageSummary.textContent =
        'DB使用容量（対象テーブル合計）: ' + managedPretty + ' / DB全体: ' + dbPretty;

      const tableRows = Array.isArray(storageUsage.managed_tables) ? storageUsage.managed_tables : [];
      if (tableRows.length === 0) {
        dom.storageUsageDetails.textContent = 'テーブル内訳: なし | 更新: ' + formatDate(generatedAt);
        return;
      }

      const detailText = tableRows
        .slice(0, 8)
        .map(function(row) {
          const name = row && row.table_name ? String(row.table_name) : '-';
          const size = row && row.size_pretty ? String(row.size_pretty) : formatBytes(row ? row.size_bytes : 0);
          return name + ': ' + size;
        })
        .join(' | ');
      dom.storageUsageDetails.textContent = 'テーブル内訳: ' + detailText + ' | 更新: ' + formatDate(generatedAt);
    }

    function renderRooms(roomOverview, roomSettings) {
      const settingsMap = new Map();
      if (Array.isArray(roomSettings)) {
        for (const item of roomSettings) settingsMap.set(item.room_id, item);
      }

      dom.roomTableBody.innerHTML = '';
      const rooms = Array.isArray(roomOverview) ? roomOverview : [];
      if (rooms.length === 0) {
        dom.roomTableBody.innerHTML = '<tr><td class="empty" colspan="15">ルーム情報がありません。最初のメッセージ受信後に表示されます。</td></tr>';
        return;
      }

      const sortedRooms = rooms
        .map((room, originalIndex) => ({
          room,
          setting: settingsMap.get(room.room_id) || null,
          originalIndex,
        }))
        .sort((a, b) => {
          const orderA = parseRoomSortOrder(a.setting?.room_sort_order)
          const orderB = parseRoomSortOrder(b.setting?.room_sort_order)
          if (orderA != null && orderB != null) return orderA - orderB
          if (orderA != null) return -1
          if (orderB != null) return 1
          return a.originalIndex - b.originalIndex
        })

      sortedRooms.forEach((entry, visualIndex) => {
        const room = entry.room
        const setting = entry.setting
        const tr = document.createElement('tr');
        tr.dataset.roomId = room.room_id;
        tr.dataset.roomSortOrder = String(parseRoomSortOrder(setting?.room_sort_order) ?? visualIndex);
        tr.dataset.botReplyEnabled = String((setting?.bot_reply_enabled) !== false);
        tr.dataset.messageSearchEnabled = String((setting?.message_search_enabled) !== false);
        tr.dataset.messageSearchLibraryEnabled = String((setting?.message_search_library_enabled) !== false);
        tr.draggable = false;
        tr.innerHTML =
          '<td><span class="room-id-tools"><span class="room-drag-handle" draggable="true" title="ドラッグで並び替え" aria-label="並び替え" role="button">⋮⋮</span><button class="button room-show-id" type="button">ID表示</button></span></td>' +
          '<td><input class="input room-name" type="text" value="' + escapeHtml((setting && setting.room_name) || room.room_name || '') + '"></td>' +
          '<td>' + Number(room.pending_messages || 0) + '件</td>' +
          '<td>' + formatDate(room.last_message_at) + '</td>' +
          '<td><input class="room-enabled room-check" type="checkbox" aria-label="有効" ' + (((setting ? setting.is_enabled : room.settings_enabled) !== false) ? 'checked' : '') + '></td>' +
          '<td><input class="room-send-summary room-check" type="checkbox" aria-label="ルーム要約配信" ' + ((setting && setting.send_room_summary === true) ? 'checked' : '') + '></td>' +
          '<td><input class="room-tomorrow-reminder room-check" type="checkbox" aria-label="明日予定配信" ' + ((setting && setting.calendar_tomorrow_reminder_enabled === true) ? 'checked' : '') + '></td>' +
          '<td><input class="room-media-file-access-enabled room-check" type="checkbox" aria-label="メディアアクセス" ' + (((setting && setting.media_file_access_enabled) !== false) ? 'checked' : '') + '></td>' +
          '<td><input class="room-calendar-auto-create room-check" type="checkbox" aria-label="自動登録" ' + (((setting && setting.calendar_ai_auto_create_enabled) !== false) ? 'checked' : '') + '></td>' +
          '<td><input class="room-silent-auto-register room-check" type="checkbox" aria-label="無返信即時登録（低確度は仮）" ' + ((setting && setting.calendar_silent_auto_register_enabled === true) ? 'checked' : '') + '></td>' +
          '<td><input class="room-gmail-alert-enabled room-check" type="checkbox" aria-label="Gmail予約通知" ' + ((setting && setting.gmail_reservation_alert_enabled === true) ? 'checked' : '') + '></td>' +
          '<td><input class="input room-hours" type="text" placeholder="空欄=全体設定" value="' + escapeHtml(setting && Array.isArray(setting.delivery_hours) ? setting.delivery_hours.join(',') : ROOM_DEFAULT_HOURS) + '"></td>' +
          '<td><select class="select room-cleanup-timing">' +
          '<option value="" ' + (((setting && setting.message_cleanup_timing) ? '' : 'selected')) + '>継承</option>' +
          '<option value="after_each_delivery" ' + ((setting && setting.message_cleanup_timing === 'after_each_delivery') ? 'selected' : '') + '>配信ごとに処理</option>' +
          '<option value="end_of_day" ' + ((setting && setting.message_cleanup_timing === 'end_of_day') ? 'selected' : '') + '>最終配信後に処理</option>' +
          '</select></td>' +
          '<td><select class="select room-summary-mode">' +
          '<option value="" ' + (((setting && setting.last_delivery_summary_mode) ? '' : 'selected')) + '>継承</option>' +
          '<option value="independent" ' + ((setting && setting.last_delivery_summary_mode === 'independent') ? 'selected' : '') + '>各回独立</option>' +
          '<option value="daily_rollup" ' + ((setting && setting.last_delivery_summary_mode === 'daily_rollup') ? 'selected' : '') + '>1日まとめ</option>' +
          '</select></td>' +
          '<td><span class="row-actions">' +
          '<button class="button primary room-save">保存</button>' +
          '<button class="button ghost room-reset">継承</button>' +
          '<button class="button warn room-delete">ルーム削除</button>' +
          '</span></td>';
        dom.roomTableBody.appendChild(tr);
      });
    }

    function openRoomIdModal(tr) {
      const roomId = tr.dataset.roomId || '-';
      const nameInput = tr.querySelector('.room-name');
      const roomName = nameInput ? nameInput.value.trim() : '';
      dom.roomIdModalName.textContent = '表示名: ' + (roomName || '(未設定)');
      dom.roomIdModalValue.textContent = roomId;
      dom.roomIdModal.classList.add('open');
      dom.roomIdModal.setAttribute('aria-hidden', 'false');
    }

    function closeRoomIdModal() {
      dom.roomIdModal.classList.remove('open');
      dom.roomIdModal.setAttribute('aria-hidden', 'true');
    }

    async function copyRoomIdToClipboard() {
      const roomId = dom.roomIdModalValue.textContent || '';
      if (!roomId || roomId === '-') return;
      try {
        await navigator.clipboard.writeText(roomId);
        alert('ルームIDをコピーしました。');
      } catch (_) {
        alert('クリップボードへのコピーに失敗しました。');
      }
    }

    function escapeHtml(value) {
      return String(value)
        .replaceAll('&', '&amp;')
        .replaceAll('<', '&lt;')
        .replaceAll('>', '&gt;')
        .replaceAll('"', '&quot;')
        .replaceAll("'", '&#39;');
    }

    function renderGmailAccountState(gmailAccount) {
      if (!dom.gmailAccountMeta) return;
      dom.gmailAccountMeta.title = '';
      if (!gmailAccount || typeof gmailAccount !== 'object') {
        dom.gmailAccountMeta.textContent = 'Gmail連携先: 未確認';
        dom.gmailAccountMeta.className = 'pill';
        return;
      }

      if (gmailAccount.error) {
        dom.gmailAccountMeta.textContent = 'Gmail連携先: 取得失敗';
        dom.gmailAccountMeta.title = String(gmailAccount.error);
        dom.gmailAccountMeta.className = 'pill';
        return;
      }

      if (gmailAccount.enabled === false) {
        dom.gmailAccountMeta.textContent = 'Gmail連携先: 通知OFF';
        dom.gmailAccountMeta.className = 'pill';
        return;
      }

      if (gmailAccount.configured === false) {
        dom.gmailAccountMeta.textContent = 'Gmail連携先: 未設定';
        dom.gmailAccountMeta.className = 'pill';
        return;
      }

      const email = typeof gmailAccount.email_address === 'string' ? gmailAccount.email_address.trim() : '';
      if (email) {
        dom.gmailAccountMeta.textContent = 'Gmail連携先: ' + email;
        dom.gmailAccountMeta.className = 'pill ok';
        return;
      }

      dom.gmailAccountMeta.textContent = 'Gmail連携先: 取得不可';
      dom.gmailAccountMeta.className = 'pill';
    }

    async function checkGmailAccount() {
      const response = await api('/gmail/account');
      renderGmailAccountState(response && response.gmail_account ? response.gmail_account : null);
    }

    async function loadState() {
      const state = await api('/state?logs_limit=30');
      currentState = state;
      renderGlobal(state.global_settings || {});
      renderLogs(state.delivery_logs || []);
      renderRooms(state.room_overview || [], state.room_settings || []);
      renderStorageUsage(state.storage_usage, state.storage_usage_error, state.generated_at);
      dom.lastRefresh.textContent = '最終更新: ' + formatDate(state.generated_at);
    }

    async function saveGlobal() {
      validateGlobalModeCombination();
      const reminderMaxItems = Number(dom.tomorrowReminderMaxItems.value);
      if (!Number.isInteger(reminderMaxItems) || reminderMaxItems < 1 || reminderMaxItems > 50) {
        throw new Error('翌日予定通知の表示件数は 1〜50 の整数で入力してください。');
      }
      const payload = {
        is_enabled: !!dom.globalEnabled.checked,
        delivery_hours: parseHoursInput(dom.globalHoursInput.value, false),
        message_cleanup_timing: dom.messageCleanupTiming.value,
        last_delivery_summary_mode: dom.lastDeliverySummaryMode.value,
        message_retention_days: Number(dom.messageRetentionDays.value),
        calendar_tomorrow_reminder_enabled: !!dom.tomorrowReminderEnabled.checked,
        calendar_tomorrow_reminder_hours: parseHoursInput(dom.tomorrowReminderHoursInput.value, false),
        calendar_tomorrow_reminder_only_if_events: dom.tomorrowReminderOnlyIfEvents.value === 'true',
        calendar_tomorrow_reminder_max_items: reminderMaxItems,
      };
      await api('/settings/global', {
        method: 'PUT',
        body: JSON.stringify(payload),
      });
      await safeLoadState();
      alert('全体設定を保存しました。');
    }

    function validateGlobalModeCombination() {
      if (dom.lastDeliverySummaryMode.value === 'daily_rollup' && dom.messageCleanupTiming.value !== 'end_of_day') {
        throw new Error('「最終配信のみ1日まとめ」を使う場合、処理タイミングは「1日の最終配信後に処理済み化」を選択してください。');
      }
    }

    function cleanupTimingLabel(value) {
      return value === 'end_of_day' ? '1日の最終配信後に処理' : '配信成功ごとに処理';
    }

    function summaryModeLabel(value) {
      return value === 'daily_rollup' ? '最終回のみ1日まとめ' : '各回独立';
    }

    function messageRetentionLabel(value) {
      const days = Number(value || 0);
      if (days <= 0) return '無制限';
      if (days === 365) return '1年';
      if (days === 730) return '2年';
      if (days === 1095) return '3年';
      return days + '日';
    }

    function parseRoomSortOrder(value) {
      const num = Number(value);
      if (!Number.isInteger(num) || num < 0) return null;
      return num;
    }

    function parseDatasetBoolean(value, fallback) {
      if (value == null || value === '') return fallback;
      const normalized = String(value).trim().toLowerCase();
      if (normalized === 'true' || normalized === '1' || normalized === 'yes' || normalized === 'on') return true;
      if (normalized === 'false' || normalized === '0' || normalized === 'no' || normalized === 'off') return false;
      return fallback;
    }

    function buildRoomSettingsPayloadFromRow(tr, roomSortOrder) {
      const roomId = tr.dataset.roomId || '';
      const nameInput = tr.querySelector('.room-name');
      const enabledInput = tr.querySelector('.room-enabled');
      const sendSummaryInput = tr.querySelector('.room-send-summary');
      const tomorrowReminderInput = tr.querySelector('.room-tomorrow-reminder');
      const mediaFileAccessEnabledInput = tr.querySelector('.room-media-file-access-enabled');
      const calendarAutoCreateInput = tr.querySelector('.room-calendar-auto-create');
      const silentAutoRegisterInput = tr.querySelector('.room-silent-auto-register');
      const gmailAlertEnabledInput = tr.querySelector('.room-gmail-alert-enabled');
      const hoursInput = tr.querySelector('.room-hours');
      const cleanupTimingInput = tr.querySelector('.room-cleanup-timing');
      const summaryModeInput = tr.querySelector('.room-summary-mode');
      const roomCleanupTiming = normalizeOptionalSelectValue(cleanupTimingInput ? cleanupTimingInput.value : '');
      const roomSummaryMode = normalizeOptionalSelectValue(summaryModeInput ? summaryModeInput.value : '');
      validateRoomModeCombination(roomCleanupTiming, roomSummaryMode);

      const normalizedOrder = parseRoomSortOrder(roomSortOrder)
      tr.dataset.roomSortOrder = String(normalizedOrder ?? 0)
      const botReplyEnabled = parseDatasetBoolean(tr.dataset.botReplyEnabled, true)
      const messageSearchEnabled = parseDatasetBoolean(tr.dataset.messageSearchEnabled, false)
      const messageSearchLibraryEnabled = parseDatasetBoolean(tr.dataset.messageSearchLibraryEnabled, false)

      return {
        room_id: roomId,
        room_name: nameInput ? nameInput.value.trim() : '',
        is_enabled: !!(enabledInput && enabledInput.checked),
        send_room_summary: !!(sendSummaryInput && sendSummaryInput.checked),
        calendar_tomorrow_reminder_enabled: !!(tomorrowReminderInput && tomorrowReminderInput.checked),
        message_search_enabled: messageSearchEnabled,
        message_search_library_enabled: messageSearchLibraryEnabled,
        media_file_access_enabled: !!(mediaFileAccessEnabledInput && mediaFileAccessEnabledInput.checked),
        bot_reply_enabled: botReplyEnabled,
        calendar_ai_auto_create_enabled: !!(calendarAutoCreateInput && calendarAutoCreateInput.checked),
        calendar_silent_auto_register_enabled: !!(silentAutoRegisterInput && silentAutoRegisterInput.checked),
        gmail_reservation_alert_enabled: !!(gmailAlertEnabledInput && gmailAlertEnabledInput.checked),
        delivery_hours: parseHoursInput(hoursInput ? hoursInput.value : '', true),
        message_cleanup_timing: roomCleanupTiming,
        last_delivery_summary_mode: roomSummaryMode,
        room_sort_order: normalizedOrder,
      };
    }

    async function persistRoomOrder() {
      const rows = Array.from(dom.roomTableBody.querySelectorAll('tr'));
      for (let index = 0; index < rows.length; index += 1) {
        const tr = rows[index];
        const payload = buildRoomSettingsPayloadFromRow(tr, index);
        await api('/settings/rooms', {
          method: 'PUT',
          body: JSON.stringify(payload),
        });
      }
      isRoomDirty = false;
      await safeLoadState();
    }

    async function saveRoomFromRow(tr) {
      const visualIndex = Array.from(dom.roomTableBody.querySelectorAll('tr')).indexOf(tr);
      const payload = buildRoomSettingsPayloadFromRow(tr, visualIndex >= 0 ? visualIndex : 0);

      await api('/settings/rooms', {
        method: 'PUT',
        body: JSON.stringify(payload),
      });
      isRoomDirty = false;
      await safeLoadState();
    }

    async function resetRoomToGlobal(tr) {
      const roomId = tr.dataset.roomId || '';
      await api('/settings/rooms/' + encodeURIComponent(roomId), { method: 'DELETE' });
      isRoomDirty = false;
      await safeLoadState();
    }

    async function deleteRoomCompletely(tr) {
      const roomId = tr.dataset.roomId || '';
      const roomNameInput = tr.querySelector('.room-name');
      const roomName = roomNameInput ? roomNameInput.value.trim() : '';
      const label = roomName || roomId;
      const ok = window.confirm('ルーム「' + label + '」を削除します。\\nこの操作で当該ルームの保存メッセージと設定を削除します。よろしいですか？');
      if (!ok) return;
      await api('/rooms/' + encodeURIComponent(roomId), { method: 'DELETE' });
      isRoomDirty = false;
      await safeLoadState();
    }

    async function addRoomSetting() {
      const roomId = dom.newRoomId.value.trim();
      if (!roomId) {
        alert('room_id を入力してください。');
        return;
      }
      const payload = {
        room_id: roomId,
        room_name: dom.newRoomName.value.trim(),
        is_enabled: !!dom.newRoomEnabled.checked,
        send_room_summary: !!dom.newRoomSendSummary.checked,
        calendar_tomorrow_reminder_enabled: !!dom.newRoomTomorrowReminder.checked,
        message_search_enabled: false,
        message_search_library_enabled: false,
        media_file_access_enabled: !!dom.newRoomMediaFileAccessEnabled.checked,
        bot_reply_enabled: true,
        calendar_ai_auto_create_enabled: !!dom.newRoomCalendarAutoCreate.checked,
        calendar_silent_auto_register_enabled: !!dom.newRoomSilentAutoRegister.checked,
        gmail_reservation_alert_enabled: !!dom.newRoomGmailAlertEnabled.checked,
        delivery_hours: parseHoursInput(dom.newRoomHours.value, true),
        message_cleanup_timing: normalizeOptionalSelectValue(dom.newRoomCleanupTiming.value),
        last_delivery_summary_mode: normalizeOptionalSelectValue(dom.newRoomSummaryMode.value),
        room_sort_order: Array.from(dom.roomTableBody.querySelectorAll('tr')).length,
      };
      validateRoomModeCombination(payload.message_cleanup_timing, payload.last_delivery_summary_mode);
      await api('/settings/rooms', {
        method: 'PUT',
        body: JSON.stringify(payload),
      });
      isRoomDirty = false;
      dom.newRoomId.value = '';
      dom.newRoomName.value = '';
      dom.newRoomHours.value = '';
      applyNewRoomDefaultCheckboxes();
      dom.newRoomCleanupTiming.value = '';
      dom.newRoomSummaryMode.value = '';
      await safeLoadState();
    }

    function normalizeOptionalSelectValue(value) {
      return value ? value : null;
    }

    function validateRoomModeCombination(roomCleanupTiming, roomSummaryMode) {
      if (roomSummaryMode !== 'daily_rollup') return;
      const fallbackCleanup =
        (currentState && currentState.global_settings && currentState.global_settings.message_cleanup_timing)
        || dom.messageCleanupTiming.value
        || 'after_each_delivery';
      const effectiveCleanup = roomCleanupTiming || fallbackCleanup;
      if (effectiveCleanup !== 'end_of_day') {
        throw new Error('ルーム設定で「最終回: 1日まとめ」を使う場合、処理タイミングは「1日の最終配信後」または「継承（全体が最終配信後）」を選択してください。');
      }
    }

    function formatManualRunResultMessage(latest) {
      const lines = [
        '手動実行結果',
        '状態: ' + String(latest && latest.status ? latest.status : '-'),
        '実行時刻: ' + formatDate(latest && latest.run_at ? latest.run_at : ''),
        '理由: ' + logReasonJa(latest || {}),
      ];
      return lines.join('\n');
    }

    async function waitForLatestLogAfter(beforeLogId, maxAttempts, intervalMs) {
      const targetId = Number(beforeLogId);
      if (!Number.isFinite(targetId) || targetId < 0) return null;
      for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
        if (attempt > 0) {
          await new Promise((resolve) => setTimeout(resolve, intervalMs));
        }
        try {
          const state = await api('/state?logs_limit=30');
          const logs = Array.isArray(state && state.delivery_logs) ? state.delivery_logs : [];
          const row = logs.find(function(item) {
            return Number(item && item.id) > targetId;
          });
          if (row) return row;
        } catch (_) {
          // ignore transient fetch errors while waiting for async cron log
        }
      }
      return null;
    }

    async function runNow() {
      const result = await api('/actions/run-summary', { method: 'POST', body: JSON.stringify({ force: true }) });
      let latest = result && result.latest_log ? result.latest_log : null;
      if (!latest) {
        latest = await waitForLatestLogAfter(result && result.before_log_id, 12, 2000);
      }
      await safeLoadState();
      if (latest && latest.status) {
        alert(formatManualRunResultMessage(latest));
        return;
      }
      if (result && result.warning) {
        alert('手動実行結果:\n' + result.warning + '\n少し待ってから「再読み込み」を押してください。');
        return;
      }
      alert('手動実行を受け付けました。ログ反映まで数秒かかる場合があります。');
    }

    async function changeAdminToken() {
      const newToken = dom.newTokenInput.value.trim();
      const confirmToken = dom.newTokenConfirmInput.value.trim();
      if (!newToken) throw new Error('新しい管理トークンを入力してください。');
      if (newToken.length < 8) throw new Error('新しい管理トークンは8文字以上で入力してください。');
      if (newToken !== confirmToken) throw new Error('新しい管理トークン（確認）が一致しません。');

      await api('/auth/token', {
        method: 'PUT',
        body: JSON.stringify({ new_token: newToken }),
      });
      setToken(newToken);
      dom.newTokenInput.value = '';
      dom.newTokenConfirmInput.value = '';
      await safeLoadState();
      alert('管理トークンを変更しました。次回以降は新しいトークンを使用してください。');
    }

    dom.saveTokenBtn.addEventListener('click', async function() {
      setToken(dom.tokenInput.value.trim());
      dom.tokenInput.value = '';
      try {
        await safeLoadState();
        await safeCheckGmailAccount({ silent: true });
        scheduleAutoRefresh();
      } catch (e) {
        stopAutoRefresh();
        alert(e.message || String(e));
      }
    });

    dom.clearTokenBtn.addEventListener('click', function() {
      setToken('');
      stopAutoRefresh();
      dom.logTableBody.innerHTML = '';
      dom.roomTableBody.innerHTML = '';
      dom.globalMeta.textContent = '';
      dom.storageUsageSummary.textContent = '';
      dom.storageUsageDetails.textContent = '';
      dom.lastRefresh.textContent = '最終更新: なし';
      renderGmailAccountState(null);
    });

    dom.reloadBtn.addEventListener('click', async function() {
      try {
        await safeLoadState();
      } catch (e) {
        alert(e.message || String(e));
      }
    });

    dom.runNowBtn.addEventListener('click', async function() {
      try {
        await runNow();
      } catch (e) {
        alert(e.message || String(e));
      }
    });

    dom.checkGmailAccountBtn.addEventListener('click', async function() {
      try {
        await checkGmailAccount();
      } catch (e) {
        alert(e.message || String(e));
      }
    });

    dom.changeTokenBtn.addEventListener('click', async function() {
      try {
        await changeAdminToken();
      } catch (e) {
        alert(e.message || String(e));
      }
    });

    dom.saveGlobalBtn.addEventListener('click', async function() {
      try {
        await saveGlobal();
      } catch (e) {
        alert(e.message || String(e));
      }
    });

    dom.addRoomBtn.addEventListener('click', async function() {
      try {
        await addRoomSetting();
      } catch (e) {
        alert(e.message || String(e));
      }
    });

    dom.roomTableBody.addEventListener('click', async function(event) {
      const target = event.target;
      if (!(target instanceof HTMLElement)) return;
      const tr = target.closest('tr');
      if (!tr) return;

      try {
        if (target.classList.contains('room-save')) {
          await saveRoomFromRow(tr);
          alert('ルーム設定を保存しました。');
        } else if (target.classList.contains('room-reset')) {
          await resetRoomToGlobal(tr);
          alert('ルーム設定を削除し、全体設定継承に戻しました。');
        } else if (target.classList.contains('room-delete')) {
          await deleteRoomCompletely(tr);
          alert('ルームを削除しました。');
        } else if (target.classList.contains('room-show-id')) {
          openRoomIdModal(tr);
        }
      } catch (e) {
        alert(e.message || String(e));
      }
    });

    dom.roomTableBody.addEventListener('input', function(event) {
      const target = event.target;
      if (!(target instanceof HTMLElement)) return;
      if (target.closest('tr')) markRoomDirty();
    });

    dom.roomTableBody.addEventListener('change', function(event) {
      const target = event.target;
      if (!(target instanceof HTMLElement)) return;
      const tr = target.closest('tr');
      if (!tr) return;

      if (target instanceof HTMLInputElement && target.type === 'checkbox' && isAutoSaveToggle(target)) {
        autoSaveRoomToggle(tr).catch(function(e) {
          alert(e.message || String(e));
        });
        return;
      }

      markRoomDirty();
    });

    let draggingRoomRow = null;

    dom.roomTableBody.addEventListener('dragstart', function(event) {
      const targetNode = event.target;
      const target = targetNode instanceof Element ? targetNode : (targetNode && targetNode.parentElement);
      if (!target) return;
      const tr = target.closest('tr');
      const handle = target.closest('.room-drag-handle');
      if (!tr || !handle) {
        event.preventDefault();
        return;
      }
      draggingRoomRow = tr;
      tr.classList.add('dragging');
      markRoomDirty();
      if (event.dataTransfer) {
        event.dataTransfer.effectAllowed = 'move';
        event.dataTransfer.setData('text/plain', tr.dataset.roomId || '');
      }
    });

    dom.roomTableBody.addEventListener('dragover', function(event) {
      if (!draggingRoomRow) return;
      const target = event.target;
      if (!(target instanceof HTMLElement)) return;
      const overRow = target.closest('tr');
      if (!overRow || overRow === draggingRoomRow) return;
      event.preventDefault();
      const rect = overRow.getBoundingClientRect();
      const insertAfter = event.clientY > rect.top + rect.height / 2;
      if (insertAfter) {
        overRow.after(draggingRoomRow);
      } else {
        overRow.before(draggingRoomRow);
      }
    });

    dom.roomTableBody.addEventListener('drop', function(event) {
      if (!draggingRoomRow) return;
      event.preventDefault();
    });

    dom.roomTableBody.addEventListener('dragend', async function() {
      if (!draggingRoomRow) return;
      draggingRoomRow.classList.remove('dragging');
      draggingRoomRow = null;
      try {
        await persistRoomOrder();
      } catch (e) {
        alert(e.message || String(e));
      }
    });

    [
      dom.newRoomId,
      dom.newRoomName,
      dom.newRoomHours,
      dom.newRoomEnabled,
      dom.newRoomSendSummary,
      dom.newRoomTomorrowReminder,
      dom.newRoomMediaFileAccessEnabled,
      dom.newRoomCalendarAutoCreate,
      dom.newRoomSilentAutoRegister,
      dom.newRoomGmailAlertEnabled,
      dom.newRoomCleanupTiming,
      dom.newRoomSummaryMode,
    ].forEach(function(el) {
      if (!el) return;
      el.addEventListener('input', markRoomDirty);
      el.addEventListener('change', markRoomDirty);
    });

    dom.newRoomEnabled.addEventListener('change', function() {
      saveBooleanSetting(NEW_ROOM_DEFAULT_ENABLED_KEY, !!dom.newRoomEnabled.checked);
    });
    dom.newRoomSendSummary.addEventListener('change', function() {
      saveBooleanSetting(NEW_ROOM_DEFAULT_SEND_SUMMARY_KEY, !!dom.newRoomSendSummary.checked);
    });
    dom.newRoomTomorrowReminder.addEventListener('change', function() {
      saveBooleanSetting(NEW_ROOM_DEFAULT_TOMORROW_REMINDER_KEY, !!dom.newRoomTomorrowReminder.checked);
    });
    dom.newRoomMediaFileAccessEnabled.addEventListener('change', function() {
      saveBooleanSetting(NEW_ROOM_DEFAULT_MEDIA_FILE_ACCESS_KEY, !!dom.newRoomMediaFileAccessEnabled.checked);
    });
    dom.newRoomCalendarAutoCreate.addEventListener('change', function() {
      saveBooleanSetting(NEW_ROOM_DEFAULT_CALENDAR_AUTO_CREATE_KEY, !!dom.newRoomCalendarAutoCreate.checked);
    });
    dom.newRoomSilentAutoRegister.addEventListener('change', function() {
      saveBooleanSetting(NEW_ROOM_DEFAULT_SILENT_AUTO_REGISTER_KEY, !!dom.newRoomSilentAutoRegister.checked);
    });
    dom.newRoomGmailAlertEnabled.addEventListener('change', function() {
      saveBooleanSetting(NEW_ROOM_DEFAULT_GMAIL_ALERT_KEY, !!dom.newRoomGmailAlertEnabled.checked);
    });

    dom.closeRoomIdBtn.addEventListener('click', function() {
      closeRoomIdModal();
    });

    dom.copyRoomIdBtn.addEventListener('click', async function() {
      await copyRoomIdToClipboard();
    });

    dom.roomIdModal.addEventListener('click', function(event) {
      if (event.target === dom.roomIdModal) {
        closeRoomIdModal();
      }
    });

    document.addEventListener('visibilitychange', function() {
      if (!token()) return;
      scheduleAutoRefresh();
      if (document.visibilityState === 'visible' && !isEditingRoomForm() && !isRoomDirty) {
        safeLoadState({ silent: true });
      }
    });

    window.addEventListener('focus', function() {
      if (!token() || isEditingRoomForm() || isRoomDirty) return;
      safeLoadState({ silent: true });
    });

    document.addEventListener('keydown', function(event) {
      if (event.key === 'Escape' && dom.roomIdModal.classList.contains('open')) {
        closeRoomIdModal();
      }
    });

    syncAuthState();
    applyNewRoomDefaultCheckboxes();
    renderGmailAccountState(null);
    if (token()) {
      safeLoadState().then(function() {
        scheduleAutoRefresh();
        safeCheckGmailAccount({ silent: true });
      }).catch(function(e) {
        stopAutoRefresh();
        alert(e.message || String(e));
      });
    }
  </script>
</body>
</html>`

const headers = {
  "content-type": "text/html; charset=utf-8",
  "cache-control": "no-store",
  "x-admin-ui": "1",
}

Deno.serve(() => new Response(html, { headers }))
