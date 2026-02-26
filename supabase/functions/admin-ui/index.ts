import "jsr:@supabase/functions-js/edge-runtime.d.ts"

const html = String.raw`<!doctype html>
<html lang="ja">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>LINE Summary Admin</title>
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Zen+Kaku+Gothic+New:wght@400;500;700;900&family=Barlow+Condensed:wght@600;700&display=swap');

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
      width: max(100%, 1700px);
      min-width: 1700px;
      table-layout: fixed;
    }

    .rooms-table th,
    .rooms-table td {
      white-space: nowrap;
    }

    .rooms-table th:nth-child(1), .rooms-table td:nth-child(1) { width: 290px; }
    .rooms-table th:nth-child(2), .rooms-table td:nth-child(2) { width: 300px; }
    .rooms-table th:nth-child(3), .rooms-table td:nth-child(3) { width: 90px; text-align: center; }
    .rooms-table th:nth-child(4), .rooms-table td:nth-child(4) { width: 150px; }
    .rooms-table th:nth-child(5), .rooms-table td:nth-child(5) { width: 70px; text-align: center; }
    .rooms-table th:nth-child(6), .rooms-table td:nth-child(6) { width: 110px; text-align: center; }
    .rooms-table th:nth-child(7), .rooms-table td:nth-child(7) { width: 160px; }
    .rooms-table th:nth-child(8), .rooms-table td:nth-child(8) { width: 170px; }
    .rooms-table th:nth-child(9), .rooms-table td:nth-child(9) { width: 150px; }
    .rooms-table th:nth-child(10), .rooms-table td:nth-child(10) { width: 220px; }

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

    .log-table th:nth-child(1),
    .log-table td:nth-child(1) { width: 54px; }
    .log-table th:nth-child(2),
    .log-table td:nth-child(2) { width: 176px; }
    .log-table th:nth-child(3),
    .log-table td:nth-child(3) { width: 220px; }
    .log-table th:nth-child(5),
    .log-table td:nth-child(5) {
      width: 120px;
      white-space: nowrap;
    }
    .log-table th:nth-child(6),
    .log-table td:nth-child(6) {
      width: 90px;
      white-space: nowrap;
    }
    .log-table th:nth-child(4),
    .log-table td:nth-child(4) {
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
          <select id="messageCleanupTiming" class="select" aria-label="メッセージ消去タイミング">
            <option value="after_each_delivery">配信成功ごとに消去（従来）</option>
            <option value="end_of_day">1日の最終配信後に消去</option>
          </select>
          <select id="lastDeliverySummaryMode" class="select" aria-label="最終配信の集計方式">
            <option value="independent">各回独立で要約（従来）</option>
            <option value="daily_rollup">最終配信のみ1日まとめ</option>
          </select>
          <button id="saveGlobalBtn" class="button primary">全体設定を保存</button>
        </div>
        <div id="globalMeta" class="meta"></div>
      </section>

      <section class="card logs">
        <h2>配信ログ（最新）</h2>
        <div class="table-wrap log-table-wrap">
          <table class="log-table">
            <thead>
              <tr>
                <th>ID</th>
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
        <div class="controls">
          <input id="newRoomId" class="input" type="text" placeholder="新規 room_id">
          <input id="newRoomName" class="input" type="text" placeholder="表示名（任意）">
          <input id="newRoomHours" class="input narrow" type="text" placeholder="時刻: 12,17">
          <label class="switch"><input id="newRoomEnabled" type="checkbox" checked>有効</label>
          <label class="switch"><input id="newRoomSendSummary" type="checkbox">ルーム要約配信</label>
          <select id="newRoomCleanupTiming" class="select" aria-label="ルーム消去タイミング">
            <option value="">消去: 全体設定を継承</option>
            <option value="after_each_delivery">消去: 配信成功ごと</option>
            <option value="end_of_day">消去: 1日の最終配信後</option>
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
                <th>room_id</th>
                <th>表示名</th>
                <th>未処理件数</th>
                <th>最終投稿</th>
                <th>有効</th>
                <th>ルーム要約配信</th>
                <th>配信時刻</th>
                <th>消去タイミング</th>
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

  <script>
    const API_BASE = '/functions/v1/admin-api';
    const TOKEN_KEY = 'line_summary_admin_token';
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
      globalEnabled: document.getElementById('globalEnabled'),
      globalHoursInput: document.getElementById('globalHoursInput'),
      messageCleanupTiming: document.getElementById('messageCleanupTiming'),
      lastDeliverySummaryMode: document.getElementById('lastDeliverySummaryMode'),
      saveGlobalBtn: document.getElementById('saveGlobalBtn'),
      globalMeta: document.getElementById('globalMeta'),
      roomTableBody: document.getElementById('roomTableBody'),
      logTableBody: document.getElementById('logTableBody'),
      newRoomId: document.getElementById('newRoomId'),
      newRoomName: document.getElementById('newRoomName'),
      newRoomHours: document.getElementById('newRoomHours'),
      newRoomEnabled: document.getElementById('newRoomEnabled'),
      newRoomSendSummary: document.getElementById('newRoomSendSummary'),
      newRoomCleanupTiming: document.getElementById('newRoomCleanupTiming'),
      newRoomSummaryMode: document.getElementById('newRoomSummaryMode'),
      addRoomBtn: document.getElementById('addRoomBtn'),
    };
    const AUTO_REFRESH_MS_VISIBLE = 5000;
    const AUTO_REFRESH_MS_HIDDEN = 15000;
    let autoRefreshTimer = null;
    let isStateLoading = false;
    let currentState = null;

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

    function statusTag(status) {
      const s = String(status || '').toLowerCase();
      if (s.includes('delivered') || s.includes('success')) return 'ok';
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

      const response = await fetch(API_BASE + path, Object.assign({}, request, { headers }));
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

    function scheduleAutoRefresh() {
      stopAutoRefresh();
      if (!token()) return;
      autoRefreshTimer = window.setTimeout(async function() {
        if (!isEditingRoomForm()) {
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
      const count = Array.isArray(settings.delivery_hours) ? settings.delivery_hours.length : 0;
      dom.globalMeta.textContent =
        '配信回数: ' + count + '回/日'
        + '  |  消去: ' + cleanupTimingLabel(dom.messageCleanupTiming.value)
        + '  |  最終回: ' + summaryModeLabel(dom.lastDeliverySummaryMode.value)
        + '  |  更新: ' + formatDate(settings.updated_at);
    }

    function renderLogs(logs) {
      dom.logTableBody.innerHTML = '';
      const rows = Array.isArray(logs) ? logs.slice(0, 30) : [];
      if (rows.length === 0) {
        dom.logTableBody.innerHTML = '<tr><td class="empty" colspan="6">ログはありません。</td></tr>';
        return;
      }
      for (const row of rows) {
        const tr = document.createElement('tr');
        const tag = statusTag(row.status);
        tr.innerHTML =
          '<td>' + (row.id ?? '-') + '</td>' +
          '<td>' + formatDate(row.run_at) + '</td>' +
          '<td><span class="tag ' + tag + '">' + escapeHtml(row.status || '-') + '</span></td>' +
          '<td>' + escapeHtml(logReasonJa(row)) + '</td>' +
          '<td>' + (row.rooms_targeted ?? 0) + ' rooms</td>' +
          '<td>' + (row.line_send_success ? '成功' : (row.line_send_attempted ? '失敗' : '未実行')) + '</td>';
        dom.logTableBody.appendChild(tr);
      }
    }

    function renderRooms(roomOverview, roomSettings) {
      const settingsMap = new Map();
      if (Array.isArray(roomSettings)) {
        for (const item of roomSettings) settingsMap.set(item.room_id, item);
      }

      dom.roomTableBody.innerHTML = '';
      const rooms = Array.isArray(roomOverview) ? roomOverview : [];
      if (rooms.length === 0) {
        dom.roomTableBody.innerHTML = '<tr><td class="empty" colspan="10">ルーム情報がありません。最初のメッセージ受信後に表示されます。</td></tr>';
        return;
      }

      for (const room of rooms) {
        const setting = settingsMap.get(room.room_id) || null;
        const tr = document.createElement('tr');
        tr.dataset.roomId = room.room_id;
        tr.innerHTML =
          '<td><code>' + escapeHtml(room.room_id) + '</code></td>' +
          '<td><input class="input room-name" type="text" value="' + escapeHtml((setting && setting.room_name) || room.room_name || '') + '"></td>' +
          '<td>' + Number(room.pending_messages || 0) + '件</td>' +
          '<td>' + formatDate(room.last_message_at) + '</td>' +
          '<td><input class="room-enabled room-check" type="checkbox" aria-label="有効" ' + (((setting ? setting.is_enabled : room.settings_enabled) !== false) ? 'checked' : '') + '></td>' +
          '<td><input class="room-send-summary room-check" type="checkbox" aria-label="ルーム要約配信" ' + ((setting && setting.send_room_summary === true) ? 'checked' : '') + '></td>' +
          '<td><input class="input room-hours" type="text" placeholder="空欄=全体設定" value="' + escapeHtml(setting && Array.isArray(setting.delivery_hours) ? setting.delivery_hours.join(',') : ROOM_DEFAULT_HOURS) + '"></td>' +
          '<td><select class="select room-cleanup-timing">' +
          '<option value="" ' + (((setting && setting.message_cleanup_timing) ? '' : 'selected')) + '>継承</option>' +
          '<option value="after_each_delivery" ' + ((setting && setting.message_cleanup_timing === 'after_each_delivery') ? 'selected' : '') + '>配信ごと</option>' +
          '<option value="end_of_day" ' + ((setting && setting.message_cleanup_timing === 'end_of_day') ? 'selected' : '') + '>最終配信後</option>' +
          '</select></td>' +
          '<td><select class="select room-summary-mode">' +
          '<option value="" ' + (((setting && setting.last_delivery_summary_mode) ? '' : 'selected')) + '>継承</option>' +
          '<option value="independent" ' + ((setting && setting.last_delivery_summary_mode === 'independent') ? 'selected' : '') + '>各回独立</option>' +
          '<option value="daily_rollup" ' + ((setting && setting.last_delivery_summary_mode === 'daily_rollup') ? 'selected' : '') + '>1日まとめ</option>' +
          '</select></td>' +
          '<td><span class="row-actions">' +
          '<button class="button primary room-save">保存</button>' +
          '<button class="button ghost room-reset">継承</button>' +
          '<button class="button warn room-delete">削除</button>' +
          '</span></td>';
        dom.roomTableBody.appendChild(tr);
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

    async function loadState() {
      const state = await api('/state?logs_limit=30');
      currentState = state;
      renderGlobal(state.global_settings || {});
      renderLogs(state.delivery_logs || []);
      renderRooms(state.room_overview || [], state.room_settings || []);
      dom.lastRefresh.textContent = '最終更新: ' + formatDate(state.generated_at);
    }

    async function saveGlobal() {
      validateGlobalModeCombination();
      const payload = {
        is_enabled: !!dom.globalEnabled.checked,
        delivery_hours: parseHoursInput(dom.globalHoursInput.value, false),
        message_cleanup_timing: dom.messageCleanupTiming.value,
        last_delivery_summary_mode: dom.lastDeliverySummaryMode.value,
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
        throw new Error('「最終配信のみ1日まとめ」を使う場合、消去タイミングは「1日の最終配信後に消去」を選択してください。');
      }
    }

    function cleanupTimingLabel(value) {
      return value === 'end_of_day' ? '1日の最終配信後' : '配信成功ごと';
    }

    function summaryModeLabel(value) {
      return value === 'daily_rollup' ? '最終回のみ1日まとめ' : '各回独立';
    }

    async function saveRoomFromRow(tr) {
      const roomId = tr.dataset.roomId || '';
      const nameInput = tr.querySelector('.room-name');
      const enabledInput = tr.querySelector('.room-enabled');
      const sendSummaryInput = tr.querySelector('.room-send-summary');
      const hoursInput = tr.querySelector('.room-hours');
      const cleanupTimingInput = tr.querySelector('.room-cleanup-timing');
      const summaryModeInput = tr.querySelector('.room-summary-mode');
      const roomCleanupTiming = normalizeOptionalSelectValue(cleanupTimingInput ? cleanupTimingInput.value : '');
      const roomSummaryMode = normalizeOptionalSelectValue(summaryModeInput ? summaryModeInput.value : '');
      validateRoomModeCombination(roomCleanupTiming, roomSummaryMode);
      const payload = {
        room_id: roomId,
        room_name: nameInput ? nameInput.value.trim() : '',
        is_enabled: !!(enabledInput && enabledInput.checked),
        send_room_summary: !!(sendSummaryInput && sendSummaryInput.checked),
        delivery_hours: parseHoursInput(hoursInput ? hoursInput.value : '', true),
        message_cleanup_timing: roomCleanupTiming,
        last_delivery_summary_mode: roomSummaryMode,
      };

      await api('/settings/rooms', {
        method: 'PUT',
        body: JSON.stringify(payload),
      });
      await safeLoadState();
    }

    async function resetRoomToGlobal(tr) {
      const roomId = tr.dataset.roomId || '';
      await api('/settings/rooms/' + encodeURIComponent(roomId), { method: 'DELETE' });
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
        delivery_hours: parseHoursInput(dom.newRoomHours.value, true),
        message_cleanup_timing: normalizeOptionalSelectValue(dom.newRoomCleanupTiming.value),
        last_delivery_summary_mode: normalizeOptionalSelectValue(dom.newRoomSummaryMode.value),
      };
      validateRoomModeCombination(payload.message_cleanup_timing, payload.last_delivery_summary_mode);
      await api('/settings/rooms', {
        method: 'PUT',
        body: JSON.stringify(payload),
      });
      dom.newRoomId.value = '';
      dom.newRoomName.value = '';
      dom.newRoomHours.value = '';
      dom.newRoomEnabled.checked = true;
      dom.newRoomSendSummary.checked = false;
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
        throw new Error('ルーム設定で「最終回: 1日まとめ」を使う場合、消去タイミングは「1日の最終配信後」または「継承（全体が最終配信後）」を選択してください。');
      }
    }

    async function runNow() {
      const result = await api('/actions/run-summary', { method: 'POST', body: JSON.stringify({ force: true }) });
      await safeLoadState();
      alert('手動実行結果: HTTP ' + result.status);
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
      dom.lastRefresh.textContent = '最終更新: なし';
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
        }
      } catch (e) {
        alert(e.message || String(e));
      }
    });

    document.addEventListener('visibilitychange', function() {
      if (!token()) return;
      scheduleAutoRefresh();
      if (document.visibilityState === 'visible' && !isEditingRoomForm()) {
        safeLoadState({ silent: true });
      }
    });

    window.addEventListener('focus', function() {
      if (!token() || isEditingRoomForm()) return;
      safeLoadState({ silent: true });
    });

    syncAuthState();
    if (token()) {
      safeLoadState().then(function() {
        scheduleAutoRefresh();
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
