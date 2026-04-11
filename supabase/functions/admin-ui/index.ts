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
      padding-bottom: 8px;
      border-bottom: 1px solid rgba(149, 219, 255, 0.18);
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

    .card.permissions {
      grid-column: span 12;
    }

    .controls {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
    }

    .setting-stack {
      display: grid;
      gap: 10px;
    }

    .setting-block {
      border: 1px solid rgba(149, 219, 255, 0.14);
      border-radius: 12px;
      background: rgba(7, 20, 34, 0.42);
      padding: 10px;
    }

    .setting-block-title {
      margin: 0 0 8px;
      font-size: 0.82rem;
      color: #9fd2eb;
      letter-spacing: 0.03em;
      text-transform: uppercase;
    }

    .auth .controls .input {
      flex: 1 1 220px;
    }

    .auth .controls .button {
      flex: 0 0 auto;
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

    .button.media-highlight {
      border-color: rgba(133, 255, 192, 0.68);
      background: linear-gradient(180deg, rgba(73, 224, 146, 0.34), rgba(24, 135, 85, 0.34));
      color: #dbffe8;
      box-shadow: 0 0 0 1px rgba(133, 255, 192, 0.18) inset;
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
    .meta.preline { white-space: pre-line; line-height: 1.45; }

    .meta.usage-summary {
      color: #9be8ff;
      font-weight: 700;
    }

    .meta.usage-details {
      line-height: 1.45;
    }

    .storage-chart {
      margin-top: 10px;
      padding: 12px;
      border: 1px solid rgba(149, 219, 255, 0.18);
      border-radius: 12px;
      background: rgba(7, 20, 34, 0.4);
      display: grid;
      gap: 10px;
      grid-template-columns: 150px minmax(0, 1fr);
      align-items: center;
    }

    .storage-pie {
      width: 132px;
      height: 132px;
      border-radius: 50%;
      border: 1px solid rgba(149, 219, 255, 0.22);
      margin: 0 auto;
      background: conic-gradient(#2e4f66 0deg 360deg);
      position: relative;
      box-shadow:
        inset 0 0 0 1px rgba(255, 255, 255, 0.05),
        0 0 24px rgba(74, 196, 255, 0.28);
      overflow: hidden;
    }

    .storage-pie::before {
      content: "";
      position: absolute;
      inset: -10%;
      background: radial-gradient(circle at 30% 25%, rgba(255, 255, 255, 0.22), transparent 45%);
      mix-blend-mode: screen;
      pointer-events: none;
    }

    .storage-pie::after {
      content: "";
      position: absolute;
      inset: 24px;
      border-radius: 50%;
      background: rgba(7, 20, 34, 0.95);
      border: 1px solid rgba(149, 219, 255, 0.18);
      z-index: 1;
    }

    .storage-pie-center {
      position: absolute;
      inset: 30px;
      border-radius: 50%;
      z-index: 2;
      display: flex;
      flex-direction: column;
      justify-content: center;
      align-items: center;
      text-align: center;
      gap: 2px;
      pointer-events: none;
    }

    .storage-pie-center-label {
      font-size: 0.64rem;
      color: #93bfd8;
      letter-spacing: 0.04em;
    }

    .storage-pie-center-value {
      font-size: 0.82rem;
      font-weight: 700;
      color: #d7f0ff;
      line-height: 1.2;
    }

    .storage-legend {
      display: grid;
      gap: 6px;
      min-width: 0;
    }

    .storage-legend-item {
      display: flex;
      align-items: center;
      gap: 8px;
      font-size: 0.82rem;
      color: #d6ecfa;
      min-width: 0;
    }

    .storage-legend-color {
      width: 10px;
      height: 10px;
      border-radius: 3px;
      flex: 0 0 auto;
    }

    .storage-legend-label {
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
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
      width: 100%;
      min-width: 0;
      table-layout: fixed;
    }

    .rooms-table {
      width: max(100%, 1520px);
      min-width: 1520px;
      table-layout: fixed;
    }

    .rooms-table th,
    .rooms-table td {
      white-space: nowrap;
    }

    .rooms-table th:nth-child(1), .rooms-table td:nth-child(1) { width: 120px; text-align: center; }
    .rooms-table th:nth-child(2), .rooms-table td:nth-child(2) { width: 220px; }
    .rooms-table th:nth-child(3), .rooms-table td:nth-child(3) { width: 92px; text-align: center; }
    .rooms-table th:nth-child(4), .rooms-table td:nth-child(4) { width: 150px; }
    .rooms-table th:nth-child(5), .rooms-table td:nth-child(5) { width: 190px; text-align: center; }
    .rooms-table th:nth-child(6), .rooms-table td:nth-child(6) { width: 170px; }
    .rooms-table th:nth-child(7), .rooms-table td:nth-child(7) { width: 180px; }
    .rooms-table th:nth-child(8), .rooms-table td:nth-child(8) { width: 260px; }

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
      left: 120px;
      box-shadow: 1px 0 0 rgba(149, 219, 255, 0.2) inset;
    }

    .rooms-table .room-name,
    .rooms-table .room-hours {
      width: 100%;
      min-width: 0;
    }

    .rooms-table .room-show-id {
      min-height: 30px;
      padding: 0 10px;
      font-size: 0.74rem;
    }

    .room-config-badge {
      display: flex;
      align-items: center;
      justify-content: center;
      min-width: 120px;
      min-height: 30px;
      padding: 0 10px;
      border-radius: 999px;
      border: 1px solid rgba(149, 219, 255, 0.24);
      background: rgba(7, 20, 34, 0.55);
      color: #bde6ff;
      font-size: 0.76rem;
      margin: 6px auto 0;
      width: fit-content;
    }

    .room-config-badge.low {
      color: #ffcf85;
      border-color: rgba(255, 207, 133, 0.38);
      background: rgba(77, 48, 19, 0.48);
    }

    .room-config-badge.mid {
      color: #d7f5ff;
      border-color: rgba(133, 216, 255, 0.34);
      background: rgba(9, 48, 74, 0.48);
    }

    .room-config-badge.high {
      color: #ccffe5;
      border-color: rgba(133, 255, 192, 0.42);
      background: rgba(17, 66, 45, 0.48);
    }

    .new-room-config-summary {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-height: 34px;
      padding: 0 12px;
      border-radius: 999px;
      border: 1px solid rgba(149, 219, 255, 0.24);
      background: rgba(7, 20, 34, 0.55);
      color: #bde6ff;
      font-size: 0.78rem;
      white-space: nowrap;
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
    .log-table td:nth-child(1) { width: 164px; }
    .log-table th:nth-child(2),
    .log-table td:nth-child(2) { width: 210px; }
    .log-table th:nth-child(3),
    .log-table td:nth-child(3) {
      width: 220px;
      white-space: normal;
      word-break: break-word;
      overflow-wrap: anywhere;
    }
    .log-table th:nth-child(4),
    .log-table td:nth-child(4) {
      width: 95px;
      white-space: nowrap;
    }
    .log-table th:nth-child(5),
    .log-table td:nth-child(5) {
      width: 62px;
      white-space: nowrap;
    }

    .log-table .tag {
      white-space: nowrap;
      line-height: 1.2;
      max-width: 100%;
      overflow: hidden;
      text-overflow: ellipsis;
      display: inline-block;
      vertical-align: middle;
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

    .user-permission-grid {
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(360px, 1fr));
      gap: 12px;
    }

    .user-permission-card {
      border: 1px solid rgba(149, 219, 255, 0.18);
      border-radius: 12px;
      background: rgba(7, 20, 34, 0.72);
      padding: 12px;
      display: grid;
      gap: 10px;
    }

    .user-permission-header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
    }

    .user-permission-name {
      font-weight: 700;
      color: #d7edff;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }

    .user-permission-controls {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 8px;
    }

    .user-permission-control {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
      border: 1px solid rgba(149, 219, 255, 0.12);
      border-radius: 9px;
      padding: 6px 8px;
      font-size: 0.78rem;
      color: #b8dbef;
    }

    .user-permission-foot {
      display: grid;
      grid-template-columns: 200px minmax(0, 1fr);
      gap: 10px;
      align-items: center;
    }

    .user-permission-check {
      width: 18px;
      height: 18px;
      accent-color: var(--accent-strong);
    }

    .user-note-input {
      width: 100%;
      min-width: 0;
      max-width: 100%;
    }

    .user-room-scope-cell {
      display: flex;
      flex-direction: column;
      align-items: center;
      gap: 6px;
      width: 100%;
    }

    .user-room-scope-cell .button {
      min-height: 32px;
      padding: 0 12px;
    }

    .room-scope-badge {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-width: 98px;
      min-height: 30px;
      padding: 0 8px;
      border-radius: 999px;
      border: 1px solid rgba(149, 219, 255, 0.24);
      background: rgba(7, 20, 34, 0.55);
      color: #bde6ff;
      font-size: 0.75rem;
      max-width: 100%;
    }

    .room-scope-badge.warn {
      color: #ffd27d;
      border-color: rgba(255, 210, 125, 0.36);
    }

    @media (max-width: 960px) {
      .user-permission-controls {
        grid-template-columns: repeat(2, minmax(0, 1fr));
      }
      .user-permission-foot {
        grid-template-columns: 1fr;
      }
    }

    .room-scope-modal-list {
      margin-top: 10px;
      max-height: min(56vh, 420px);
      overflow: auto;
      border: 1px solid rgba(155, 220, 255, 0.22);
      border-radius: 10px;
      background: rgba(8, 21, 35, 0.58);
      padding: 8px;
      display: grid;
      gap: 6px;
    }

    .room-scope-option {
      display: flex;
      align-items: center;
      gap: 8px;
      font-size: 0.84rem;
      color: #d7edff;
      padding: 4px 6px;
      border-radius: 8px;
    }

    .room-scope-option:hover {
      background: rgba(87, 216, 255, 0.08);
    }

    .room-scope-option input {
      width: 16px;
      height: 16px;
      accent-color: #28c2ff;
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

    @media (max-width: 760px) {
      .storage-chart {
        grid-template-columns: 1fr;
        justify-items: center;
      }
      .storage-legend {
        width: 100%;
      }
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
        <div class="setting-stack">
          <div class="setting-block">
            <p class="setting-block-title">接続と実行</p>
            <div class="controls">
              <input id="tokenInput" class="input" type="password" placeholder="ADMIN_DASHBOARD_TOKEN を入力">
              <button id="saveTokenBtn" class="button primary">保存して接続</button>
              <button id="clearTokenBtn" class="button ghost">削除</button>
            </div>
            <div class="controls" style="margin-top:8px;">
              <button id="reloadBtn" class="button">再読み込み</button>
              <button id="runNowBtn" class="button warn">今すぐ要約実行</button>
              <a class="button media-highlight" href="https://marugo-s.github.io/LINE-management/media.html" target="_blank" rel="noopener noreferrer" style="display:inline-flex;align-items:center;text-decoration:none;">メディア閲覧</a>
            </div>
          </div>
          <div class="setting-block">
            <p class="setting-block-title">連携アカウント</p>
            <div class="controls">
              <button id="checkGmailAccountBtn" class="button">Gmail連携先を確認</button>
              <span id="gmailAccountMeta" class="pill">Gmail連携先: 未確認</span>
            </div>
          </div>
          <div class="setting-block">
            <p class="setting-block-title">管理トークン変更</p>
            <div class="controls">
              <input id="newTokenInput" class="input" type="password" placeholder="新しい管理トークン">
              <input id="newTokenConfirmInput" class="input" type="password" placeholder="新しい管理トークン（確認）">
              <button id="changeTokenBtn" class="button warn">トークン変更</button>
            </div>
          </div>
        </div>
        <div class="meta">トークンはブラウザの LocalStorage に保存されます。</div>
      </section>

      <section class="card global">
        <h2>全体設定</h2>
        <div class="setting-stack">
          <div class="setting-block">
            <p class="setting-block-title">配信設定</p>
            <label class="switch"><input id="globalEnabled" type="checkbox">配信を有効化</label>
            <div class="controls" style="margin-top:8px;">
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
          </div>
          <div class="setting-block">
            <p class="setting-block-title">翌日予定通知</p>
            <div class="controls">
              <label class="switch"><input id="tomorrowReminderEnabled" type="checkbox">翌日予定通知を有効化</label>
              <input id="tomorrowReminderHoursInput" class="input" type="text" placeholder="翌日予定通知の時刻 例: 19">
              <select id="tomorrowReminderOnlyIfEvents" class="select" aria-label="予定なし時の通知">
                <option value="false">予定なしの日も通知する</option>
                <option value="true">予定がある日だけ通知する</option>
              </select>
              <input id="tomorrowReminderMaxItems" class="input narrow" type="number" min="1" max="50" step="1" placeholder="表示件数">
            </div>
          </div>
        </div>
        <div id="globalMeta" class="meta preline"></div>
        <div id="storageUsageSummary" class="meta usage-summary"></div>
        <div id="storageUsageDetails" class="meta usage-details"></div>
        <div class="storage-chart">
          <div id="storageUsagePie" class="storage-pie" aria-label="テーブル容量内訳円グラフ">
            <div id="storageUsagePieCenter" class="storage-pie-center">
              <div class="storage-pie-center-label">対象容量</div>
              <div id="storageUsagePieCenterValue" class="storage-pie-center-value">-</div>
            </div>
          </div>
          <div id="storageUsageLegend" class="storage-legend"></div>
        </div>
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
          <button id="openNewRoomConfigBtn" class="button" type="button">追加時の設定</button>
          <span id="newRoomConfigSummary" class="new-room-config-summary">-</span>
          <button id="addRoomBtn" class="button primary">ルーム設定を追加</button>
          <button id="refreshRoomNamesBtn" class="button">ルーム名再取得</button>
        </div>
        <div class="table-wrap" style="margin-top:12px;">
          <table class="rooms-table">
            <thead>
              <tr>
                <th>ID</th>
                <th>表示名</th>
                <th>未処理件数</th>
                <th>最終投稿</th>
                <th>設定</th>
                <th>配信時刻</th>
                <th>最終回集計</th>
                <th>操作</th>
              </tr>
            </thead>
            <tbody id="roomTableBody"></tbody>
          </table>
        </div>
      </section>

      <section class="card permissions">
        <h2>ユーザー権限（LINE user 単位）</h2>
        <p class="muted" style="margin:0 0 8px 0;">Botに友だち追加・メッセージ送信したLINEユーザーが表示されます。チェック変更はその場で即時保存されます。</p>
        <div id="userPermissionTableBody" class="user-permission-grid"></div>
        <div class="controls" style="margin-top:8px;">
          <button id="reloadUserPermissionsBtn" class="button">再読込</button>
          <button id="backfillUserPermissionsBtn" class="button">既存ユーザー取込</button>
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

  <div id="userRoomScopeModal" class="modal-backdrop" aria-hidden="true">
    <div class="modal-card" role="dialog" aria-modal="true" aria-labelledby="userRoomScopeModalTitle">
      <h3 id="userRoomScopeModalTitle" class="modal-title">会話検索の対象外ルーム選択</h3>
      <div id="userRoomScopeModalMeta" class="modal-meta">対象ユーザー: -</div>
      <div id="userRoomScopeModalList" class="room-scope-modal-list"></div>
      <div class="modal-actions">
        <button id="cancelUserRoomScopeBtn" class="button">キャンセル</button>
        <button id="saveUserRoomScopeBtn" class="button primary">保存</button>
      </div>
    </div>
  </div>

  <div id="roomConfigModal" class="modal-backdrop" aria-hidden="true">
    <div class="modal-card" role="dialog" aria-modal="true" aria-labelledby="roomConfigModalTitle">
      <h3 id="roomConfigModalTitle" class="modal-title">ルーム設定</h3>
      <div id="roomConfigModalMeta" class="modal-meta">対象ルーム: -</div>
      <div class="room-scope-modal-list" style="margin-top:12px;">
        <label class="room-scope-option"><input id="roomConfigEnabled" type="checkbox">有効</label>
        <label class="room-scope-option"><input id="roomConfigSendSummary" type="checkbox">ルーム要約配信</label>
        <label class="room-scope-option"><input id="roomConfigTomorrowReminder" type="checkbox">明日予定配信</label>
        <label class="room-scope-option"><input id="roomConfigMediaAccess" type="checkbox">メディアアクセス</label>
        <label class="room-scope-option"><input id="roomConfigAutoCreate" type="checkbox">自動登録</label>
        <label class="room-scope-option"><input id="roomConfigSilentAutoRegister" type="checkbox">無返信即時登録（低確度は仮）</label>
        <label class="room-scope-option"><input id="roomConfigGmailAlert" type="checkbox">Gmail予約通知</label>
      </div>
      <div class="modal-actions">
        <button id="cancelRoomConfigBtn" class="button">キャンセル</button>
        <button id="saveRoomConfigBtn" class="button primary">保存</button>
      </div>
    </div>
  </div>

  <div id="newRoomConfigModal" class="modal-backdrop" aria-hidden="true">
    <div class="modal-card" role="dialog" aria-modal="true" aria-labelledby="newRoomConfigModalTitle">
      <h3 id="newRoomConfigModalTitle" class="modal-title">新規ルーム追加時の設定</h3>
      <div class="modal-meta">ここで選んだ値が次の追加に使われます。</div>
      <div class="room-scope-modal-list" style="margin-top:12px;">
        <label class="room-scope-option"><input id="newRoomEnabled" type="checkbox">有効</label>
        <label class="room-scope-option"><input id="newRoomSendSummary" type="checkbox">ルーム要約配信</label>
        <label class="room-scope-option"><input id="newRoomTomorrowReminder" type="checkbox">明日予定配信</label>
        <label class="room-scope-option"><input id="newRoomMediaFileAccessEnabled" type="checkbox">メディアアクセス</label>
        <label class="room-scope-option"><input id="newRoomCalendarAutoCreate" type="checkbox">自動登録</label>
        <label class="room-scope-option"><input id="newRoomSilentAutoRegister" type="checkbox">無返信即時登録（低確度は仮）</label>
        <label class="room-scope-option"><input id="newRoomGmailAlertEnabled" type="checkbox">Gmail予約通知</label>
      </div>
      <div class="controls" style="margin-top:10px;">
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
      </div>
      <div class="modal-actions">
        <button id="cancelNewRoomConfigBtn" class="button">閉じる</button>
        <button id="saveNewRoomConfigBtn" class="button primary">適用</button>
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
      storageUsagePie: document.getElementById('storageUsagePie'),
      storageUsagePieCenterValue: document.getElementById('storageUsagePieCenterValue'),
      storageUsageLegend: document.getElementById('storageUsageLegend'),
      roomTableBody: document.getElementById('roomTableBody'),
      logTableBody: document.getElementById('logTableBody'),
      userPermissionTableBody: document.getElementById('userPermissionTableBody'),
      reloadUserPermissionsBtn: document.getElementById('reloadUserPermissionsBtn'),
      backfillUserPermissionsBtn: document.getElementById('backfillUserPermissionsBtn'),
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
      openNewRoomConfigBtn: document.getElementById('openNewRoomConfigBtn'),
      newRoomConfigSummary: document.getElementById('newRoomConfigSummary'),
      newRoomConfigModal: document.getElementById('newRoomConfigModal'),
      cancelNewRoomConfigBtn: document.getElementById('cancelNewRoomConfigBtn'),
      saveNewRoomConfigBtn: document.getElementById('saveNewRoomConfigBtn'),
      addRoomBtn: document.getElementById('addRoomBtn'),
      refreshRoomNamesBtn: document.getElementById('refreshRoomNamesBtn'),
      roomIdModal: document.getElementById('roomIdModal'),
      roomIdModalName: document.getElementById('roomIdModalName'),
      roomIdModalValue: document.getElementById('roomIdModalValue'),
      copyRoomIdBtn: document.getElementById('copyRoomIdBtn'),
      closeRoomIdBtn: document.getElementById('closeRoomIdBtn'),
      userRoomScopeModal: document.getElementById('userRoomScopeModal'),
      userRoomScopeModalMeta: document.getElementById('userRoomScopeModalMeta'),
      userRoomScopeModalList: document.getElementById('userRoomScopeModalList'),
      cancelUserRoomScopeBtn: document.getElementById('cancelUserRoomScopeBtn'),
      saveUserRoomScopeBtn: document.getElementById('saveUserRoomScopeBtn'),
      roomConfigModal: document.getElementById('roomConfigModal'),
      roomConfigModalMeta: document.getElementById('roomConfigModalMeta'),
      roomConfigEnabled: document.getElementById('roomConfigEnabled'),
      roomConfigSendSummary: document.getElementById('roomConfigSendSummary'),
      roomConfigTomorrowReminder: document.getElementById('roomConfigTomorrowReminder'),
      roomConfigMediaAccess: document.getElementById('roomConfigMediaAccess'),
      roomConfigAutoCreate: document.getElementById('roomConfigAutoCreate'),
      roomConfigSilentAutoRegister: document.getElementById('roomConfigSilentAutoRegister'),
      roomConfigGmailAlert: document.getElementById('roomConfigGmailAlert'),
      cancelRoomConfigBtn: document.getElementById('cancelRoomConfigBtn'),
      saveRoomConfigBtn: document.getElementById('saveRoomConfigBtn'),
    };
    const AUTO_REFRESH_MS_VISIBLE = 5000;
    const AUTO_REFRESH_MS_HIDDEN = 15000;
    let autoRefreshTimer = null;
    let isStateLoading = false;
    let currentState = null;
    let isGlobalDirty = false;
    let isRoomDirty = false;
    let isUserPermissionDirty = false;
    let activeUserRoomScopeRow = null;
    let activeRoomConfigRow = null;

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
      refreshNewRoomConfigSummary();
    }

    function buildNewRoomConfigSummary() {
      const enabledCount =
        (dom.newRoomEnabled.checked ? 1 : 0) +
        (dom.newRoomSendSummary.checked ? 1 : 0) +
        (dom.newRoomTomorrowReminder.checked ? 1 : 0) +
        (dom.newRoomMediaFileAccessEnabled.checked ? 1 : 0) +
        (dom.newRoomCalendarAutoCreate.checked ? 1 : 0) +
        (dom.newRoomSilentAutoRegister.checked ? 1 : 0) +
        (dom.newRoomGmailAlertEnabled.checked ? 1 : 0);
      const cleanup = dom.newRoomCleanupTiming.value === 'end_of_day'
        ? '最終配信後'
        : dom.newRoomCleanupTiming.value === 'after_each_delivery'
        ? '配信ごと'
        : '継承';
      const summary = dom.newRoomSummaryMode.value === 'daily_rollup'
        ? '1日まとめ'
        : dom.newRoomSummaryMode.value === 'independent'
        ? '各回独立'
        : '継承';
      return enabledCount + '/7 有効 ・ 処理:' + cleanup + ' ・ 最終回:' + summary;
    }

    function refreshNewRoomConfigSummary() {
      if (!dom.newRoomConfigSummary) return;
      dom.newRoomConfigSummary.textContent = buildNewRoomConfigSummary();
    }

    function openNewRoomConfigModal() {
      dom.newRoomConfigModal.classList.add('open');
      dom.newRoomConfigModal.setAttribute('aria-hidden', 'false');
    }

    function closeNewRoomConfigModal() {
      dom.newRoomConfigModal.classList.remove('open');
      dom.newRoomConfigModal.setAttribute('aria-hidden', 'true');
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

    function renderStorageUsageChart(tableRows, centerValueText) {
      const rows = Array.isArray(tableRows) ? tableRows : [];
      dom.storageUsagePieCenterValue.textContent = centerValueText || '-';
      if (!rows.length) {
        dom.storageUsagePie.style.background = 'conic-gradient(#2e4f66 0deg 360deg)';
        dom.storageUsageLegend.innerHTML = '<div class="storage-legend-item"><span class="storage-legend-label">内訳データなし</span></div>';
        return;
      }
      const colors = ['#45c2ff', '#66e0a8', '#ffd27d', '#bba4ff', '#ff9fb1', '#7de2e2', '#9bc6ff', '#d4f08a'];
      const sorted = rows
        .map(function(row) {
          return {
            table_name: String(row && row.table_name ? row.table_name : '-'),
            size_bytes: Number(row && row.size_bytes ? row.size_bytes : 0),
            size_pretty: String(row && row.size_pretty ? row.size_pretty : formatBytes(row && row.size_bytes ? row.size_bytes : 0)),
          };
        })
        .filter(function(row) { return Number.isFinite(row.size_bytes) && row.size_bytes > 0; })
        .sort(function(a, b) { return b.size_bytes - a.size_bytes; });
      if (!sorted.length) {
        dom.storageUsagePie.style.background = 'conic-gradient(#2e4f66 0deg 360deg)';
        dom.storageUsageLegend.innerHTML = '<div class="storage-legend-item"><span class="storage-legend-label">内訳データなし</span></div>';
        return;
      }
      const maxSlices = 7;
      const topRows = sorted.slice(0, maxSlices);
      const others = sorted.slice(maxSlices);
      const otherBytes = others.reduce(function(sum, row) { return sum + row.size_bytes; }, 0);
      if (otherBytes > 0) {
        topRows.push({
          table_name: 'others',
          size_bytes: otherBytes,
          size_pretty: formatBytes(otherBytes),
        });
      }
      const total = topRows.reduce(function(sum, row) { return sum + row.size_bytes; }, 0);
      let current = 0;
      const segments = [];
      const legends = [];
      for (let i = 0; i < topRows.length; i += 1) {
        const row = topRows[i];
        const ratio = total > 0 ? row.size_bytes / total : 0;
        const end = current + ratio * 360;
        const color = colors[i % colors.length];
        segments.push(color + ' ' + current.toFixed(2) + 'deg ' + end.toFixed(2) + 'deg');
        const label = row.table_name === 'others' ? 'その他' : row.table_name;
        const percentage = (ratio * 100).toFixed(1);
        legends.push(
          '<div class="storage-legend-item">' +
          '<span class="storage-legend-color" style="background:' + color + ';"></span>' +
          '<span class="storage-legend-label">' + escapeHtml(label + ' (' + percentage + '% / ' + row.size_pretty + ')') + '</span>' +
          '</div>'
        );
        current = end;
      }
      dom.storageUsagePie.style.background = 'conic-gradient(' + segments.join(', ') + ')';
      dom.storageUsageLegend.innerHTML = legends.join('');
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

      const finalPath = (request.method || 'GET').toUpperCase() === 'GET' && path.startsWith('/state')
        ? (path.includes('?') ? (path + '&_ts=' + Date.now()) : (path + '?_ts=' + Date.now()))
        : path;
      const response = await fetch(
        API_BASE + finalPath,
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

    function markGlobalDirty() {
      isGlobalDirty = true;
    }

    function markUserPermissionDirty() {
      isUserPermissionDirty = true;
    }

    const roomAutoSaveTimers = new Map();

    function scheduleRoomAutoSave(tr, delayMs) {
      const roomId = String(tr && tr.dataset ? tr.dataset.roomId || '' : '').trim();
      if (!roomId) return;
      const wait = Number.isFinite(Number(delayMs)) ? Math.max(0, Number(delayMs)) : 450;
      const prev = roomAutoSaveTimers.get(roomId);
      if (prev) clearTimeout(prev);
      const timer = setTimeout(function() {
        roomAutoSaveTimers.delete(roomId);
        saveRoomFromRow(tr, { reload: false }).catch(function(error) {
          alert(error.message || String(error));
          safeLoadState({ silent: true });
        });
      }, wait);
      roomAutoSaveTimers.set(roomId, timer);
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
        if (!isEditingRoomForm() && !isGlobalDirty && !isRoomDirty && !isUserPermissionDirty) {
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
        '配信: ' + count + '回/日'
        + '  |  消去: ' + cleanupTimingLabel(dom.messageCleanupTiming.value)
        + '  |  最終回: ' + summaryModeLabel(dom.lastDeliverySummaryMode.value)
        + '\n'
        + '会話保持: ' + messageRetentionLabel(dom.messageRetentionDays.value)
        + '  |  翌日予定通知: '
        + (dom.tomorrowReminderEnabled.checked ? ('ON (' + reminderHours + '時)') : 'OFF')
        + '  |  予定なし時: '
        + (dom.tomorrowReminderOnlyIfEvents.value === 'true' ? '送らない' : '送る')
        + '  |  表示上限: '
        + dom.tomorrowReminderMaxItems.value + '件';
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
        renderStorageUsageChart([], '-');
        return;
      }

      if (!storageUsage || typeof storageUsage !== 'object') {
        dom.storageUsageSummary.textContent = 'DB使用容量: 取得待ち';
        dom.storageUsageDetails.textContent = '';
        renderStorageUsageChart([], '-');
        return;
      }

      const managedPretty = storageUsage.managed_tables_total_pretty || formatBytes(storageUsage.managed_tables_total_bytes);
      const dbPretty = storageUsage.database_size_pretty || formatBytes(storageUsage.database_size_bytes);
      dom.storageUsageSummary.textContent =
        'DB使用容量（対象テーブル合計）: ' + managedPretty + ' / DB全体: ' + dbPretty;

      const tableRows = Array.isArray(storageUsage.managed_tables) ? storageUsage.managed_tables : [];
      renderStorageUsageChart(tableRows, managedPretty);
      if (tableRows.length === 0) {
        dom.storageUsageDetails.textContent = '';
        return;
      }

      dom.storageUsageDetails.textContent = '';
    }

    function renderUserPermissions(items) {
      const rows = Array.isArray(items) ? items : [];
      dom.userPermissionTableBody.innerHTML = '';
      if (rows.length === 0) {
        dom.userPermissionTableBody.innerHTML = '<div class="empty">友だち追加済みユーザーはまだありません。</div>';
        isUserPermissionDirty = false;
        return;
      }
      for (const row of rows) {
        const card = document.createElement('div');
        card.className = 'user-permission-card';
        card.dataset.lineUserId = String(row.line_user_id || '').trim();
        card.dataset.displayName = String(row.display_name || '').trim();
        const excludedRoomIds = Array.isArray(row.excluded_message_search_room_ids)
          ? row.excluded_message_search_room_ids.map(function(v) { return String(v || '').trim(); }).filter(function(v) { return !!v; })
          : [];
        card.dataset.excludedMessageSearchRoomIds = JSON.stringify(excludedRoomIds);
        const scopeBadgeClass = excludedRoomIds.length > 0 ? 'room-scope-badge warn' : 'room-scope-badge';
        const scopeBadgeText = excludedRoomIds.length > 0
          ? ('除外 ' + excludedRoomIds.length + '件')
          : '全ルーム対象';
        card.innerHTML =
          '<div class="user-permission-header">' +
          '<div class="user-permission-name">' + escapeHtml(row.display_name || '-') + '</div>' +
          '<div><button class="button user-show-id" type="button">ID表示</button></div>' +
          '</div>' +
          '<div class="user-permission-controls">' +
          '<label class="user-permission-control"><span>利用可</span><input class="user-permission-check user-is-active" type="checkbox" ' + (row.is_active !== false ? 'checked' : '') + '></label>' +
          '<label class="user-permission-control"><span>会話検索</span><input class="user-permission-check user-can-message-search" type="checkbox" ' + (row.can_message_search !== false ? 'checked' : '') + '></label>' +
          '<label class="user-permission-control"><span>資料検索</span><input class="user-permission-check user-can-library-search" type="checkbox" ' + (row.can_library_search !== false ? 'checked' : '') + '></label>' +
          '<label class="user-permission-control"><span>予定作成</span><input class="user-permission-check user-can-calendar-create" type="checkbox" ' + (row.can_calendar_create !== false ? 'checked' : '') + '></label>' +
          '<label class="user-permission-control"><span>予定更新</span><input class="user-permission-check user-can-calendar-update" type="checkbox" ' + (row.can_calendar_update !== false ? 'checked' : '') + '></label>' +
          '<label class="user-permission-control"><span>メディア</span><input class="user-permission-check user-can-media-access" type="checkbox" ' + (row.can_media_access !== false ? 'checked' : '') + '></label>' +
          '</div>' +
          '<div class="user-permission-foot">' +
          '<div class="user-room-scope-cell"><button class="button user-room-scope" type="button">ルーム選択</button><div class="' + scopeBadgeClass + '">' + escapeHtml(scopeBadgeText) + '</div></div>' +
          '<input class="input user-note-input" type="text" value="' + escapeHtml(row.note || '') + '" placeholder="メモ">' +
          '</div>' +
          '<div><span class="tag ok">自動保存</span></div>';
        dom.userPermissionTableBody.appendChild(card);
      }
      isUserPermissionDirty = false;
    }

    function openLineUserIdModal(tr) {
      const lineUserId = String(tr.dataset.lineUserId || '').trim() || '-';
      const displayName = String(tr.dataset.displayName || '').trim() || '(未設定)';
      const titleEl = document.getElementById('roomIdModalTitle');
      if (titleEl) titleEl.textContent = 'LINEユーザーID';
      dom.roomIdModalName.textContent = '表示名: ' + displayName;
      dom.roomIdModalValue.textContent = lineUserId;
      dom.roomIdModal.classList.add('open');
      dom.roomIdModal.setAttribute('aria-hidden', 'false');
    }

    function getRoomOptionsForUserScope() {
      const rooms = currentState && Array.isArray(currentState.room_overview) ? currentState.room_overview : [];
      return rooms.map(function(room) {
        const roomId = String(room && room.room_id ? room.room_id : '').trim();
        const roomName = String(room && room.room_name ? room.room_name : '').trim();
        return {
          room_id: roomId,
          room_label: roomName || roomId || '(未設定)',
        };
      }).filter(function(room) { return !!room.room_id; });
    }

    function openUserRoomScopeModal(tr) {
      activeUserRoomScopeRow = tr;
      const lineUserId = String(tr.dataset.lineUserId || '').trim();
      const displayName = String(tr.dataset.displayName || '').trim() || '(未設定)';
      dom.userRoomScopeModalMeta.textContent = '対象ユーザー: ' + displayName + ' (' + lineUserId + ')';

      let excludedRoomIds = [];
      try {
        const parsed = JSON.parse(String(tr.dataset.excludedMessageSearchRoomIds || '[]'));
        excludedRoomIds = Array.isArray(parsed)
          ? parsed.map(function(v) { return String(v || '').trim(); }).filter(function(v) { return !!v; })
          : [];
      } catch (_) {
        excludedRoomIds = [];
      }
      const excludedSet = new Set(excludedRoomIds);
      const options = getRoomOptionsForUserScope();
      if (!options.length) {
        dom.userRoomScopeModalList.innerHTML = '<div class="empty">ルームがまだありません。</div>';
      } else {
        dom.userRoomScopeModalList.innerHTML = options.map(function(room) {
          const checked = excludedSet.has(room.room_id) ? 'checked' : '';
          return (
            '<label class="room-scope-option">' +
            '<input class="room-scope-check" type="checkbox" data-room-id="' + escapeHtml(room.room_id) + '" ' + checked + '>' +
            '<span>' + escapeHtml(room.room_label) + '</span>' +
            '</label>'
          );
        }).join('');
      }
      dom.userRoomScopeModal.classList.add('open');
      dom.userRoomScopeModal.setAttribute('aria-hidden', 'false');
    }

    function closeUserRoomScopeModal() {
      activeUserRoomScopeRow = null;
      dom.userRoomScopeModal.classList.remove('open');
      dom.userRoomScopeModal.setAttribute('aria-hidden', 'true');
    }

    async function saveUserRoomScopeSelection() {
      if (!activeUserRoomScopeRow) return;
      const checks = Array.from(dom.userRoomScopeModalList.querySelectorAll('.room-scope-check'));
      const excludedRoomIds = checks
        .filter(function(el) { return el instanceof HTMLInputElement && el.checked; })
        .map(function(el) { return String(el.getAttribute('data-room-id') || '').trim(); })
        .filter(function(v) { return !!v; });
      activeUserRoomScopeRow.dataset.excludedMessageSearchRoomIds = JSON.stringify(excludedRoomIds);
      await saveSingleUserPermissionRow(activeUserRoomScopeRow);
      alert('検索対象ルームを保存しました。');
      closeUserRoomScopeModal();
    }

    function getRoomConfigStateFromRow(tr) {
      const enabledInput = tr.querySelector('.room-enabled');
      const sendSummaryInput = tr.querySelector('.room-send-summary');
      const tomorrowReminderInput = tr.querySelector('.room-tomorrow-reminder');
      const mediaFileAccessEnabledInput = tr.querySelector('.room-media-file-access-enabled');
      const calendarAutoCreateInput = tr.querySelector('.room-calendar-auto-create');
      const silentAutoRegisterInput = tr.querySelector('.room-silent-auto-register');
      const gmailAlertEnabledInput = tr.querySelector('.room-gmail-alert-enabled');
      return {
        is_enabled: enabledInput ? !!enabledInput.checked : parseDatasetBoolean(tr.dataset.roomEnabled, true),
        send_room_summary: sendSummaryInput ? !!sendSummaryInput.checked : parseDatasetBoolean(tr.dataset.roomSendSummary, false),
        calendar_tomorrow_reminder_enabled: tomorrowReminderInput ? !!tomorrowReminderInput.checked : parseDatasetBoolean(tr.dataset.roomTomorrowReminder, false),
        media_file_access_enabled: mediaFileAccessEnabledInput ? !!mediaFileAccessEnabledInput.checked : parseDatasetBoolean(tr.dataset.roomMediaAccess, true),
        calendar_ai_auto_create_enabled: calendarAutoCreateInput ? !!calendarAutoCreateInput.checked : parseDatasetBoolean(tr.dataset.roomCalendarAutoCreate, true),
        calendar_silent_auto_register_enabled: silentAutoRegisterInput ? !!silentAutoRegisterInput.checked : parseDatasetBoolean(tr.dataset.roomSilentAutoRegister, false),
        gmail_reservation_alert_enabled: gmailAlertEnabledInput ? !!gmailAlertEnabledInput.checked : parseDatasetBoolean(tr.dataset.roomGmailAlertEnabled, false),
      };
    }

    function applyRoomConfigStateToRow(tr, config) {
      tr.dataset.roomEnabled = String(!!config.is_enabled);
      tr.dataset.roomSendSummary = String(!!config.send_room_summary);
      tr.dataset.roomTomorrowReminder = String(!!config.calendar_tomorrow_reminder_enabled);
      tr.dataset.roomMediaAccess = String(!!config.media_file_access_enabled);
      tr.dataset.roomCalendarAutoCreate = String(!!config.calendar_ai_auto_create_enabled);
      tr.dataset.roomSilentAutoRegister = String(!!config.calendar_silent_auto_register_enabled);
      tr.dataset.roomGmailAlertEnabled = String(!!config.gmail_reservation_alert_enabled);
      const badge = tr.querySelector('.room-config-badge');
      if (badge) {
        const tone = roomConfigToneClass(getRoomConfigEnabledCount(config));
        badge.className = 'room-config-badge ' + tone;
        badge.textContent = buildRoomConfigSummary(config);
      }
    }

    function getRoomConfigEnabledCount(config) {
      let enabledCount = 0;
      if (config.is_enabled) enabledCount += 1;
      if (config.send_room_summary) enabledCount += 1;
      if (config.calendar_tomorrow_reminder_enabled) enabledCount += 1;
      if (config.media_file_access_enabled) enabledCount += 1;
      if (config.calendar_ai_auto_create_enabled) enabledCount += 1;
      if (config.calendar_silent_auto_register_enabled) enabledCount += 1;
      if (config.gmail_reservation_alert_enabled) enabledCount += 1;
      return enabledCount;
    }

    function roomConfigToneClass(enabledCount) {
      if (enabledCount >= 6) return 'high';
      if (enabledCount >= 3) return 'mid';
      return 'low';
    }

    function buildRoomConfigSummary(config) {
      const enabledCount = getRoomConfigEnabledCount(config);
      return enabledCount + '/7 有効';
    }

    function renderRooms(roomOverview, roomSettings) {
      const settingsMap = new Map();
      if (Array.isArray(roomSettings)) {
        for (const item of roomSettings) settingsMap.set(item.room_id, item);
      }

      dom.roomTableBody.innerHTML = '';
      const rooms = Array.isArray(roomOverview) ? roomOverview : [];
      if (rooms.length === 0) {
        dom.roomTableBody.innerHTML = '<tr><td class="empty" colspan="8">ルーム情報がありません。最初のメッセージ受信後に表示されます。</td></tr>';
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
        tr.dataset.roomEnabled = String(((setting ? setting.is_enabled : room.settings_enabled) !== false));
        tr.dataset.roomSendSummary = String((setting && setting.send_room_summary === true));
        tr.dataset.roomTomorrowReminder = String((setting && setting.calendar_tomorrow_reminder_enabled === true));
        tr.dataset.roomMediaAccess = String(((setting && setting.media_file_access_enabled) !== false));
        tr.dataset.roomCalendarAutoCreate = String(((setting && setting.calendar_ai_auto_create_enabled) !== false));
        tr.dataset.roomSilentAutoRegister = String((setting && setting.calendar_silent_auto_register_enabled === true));
        tr.dataset.roomGmailAlertEnabled = String((setting && setting.gmail_reservation_alert_enabled === true));
        tr.dataset.messageCleanupTiming = String((setting && setting.message_cleanup_timing) || '');
        const configSummary = buildRoomConfigSummary({
          is_enabled: parseDatasetBoolean(tr.dataset.roomEnabled, true),
          send_room_summary: parseDatasetBoolean(tr.dataset.roomSendSummary, false),
          calendar_tomorrow_reminder_enabled: parseDatasetBoolean(tr.dataset.roomTomorrowReminder, false),
          media_file_access_enabled: parseDatasetBoolean(tr.dataset.roomMediaAccess, true),
          calendar_ai_auto_create_enabled: parseDatasetBoolean(tr.dataset.roomCalendarAutoCreate, true),
          calendar_silent_auto_register_enabled: parseDatasetBoolean(tr.dataset.roomSilentAutoRegister, false),
          gmail_reservation_alert_enabled: parseDatasetBoolean(tr.dataset.roomGmailAlertEnabled, false),
        });
        const configToneClass = roomConfigToneClass(getRoomConfigEnabledCount({
          is_enabled: parseDatasetBoolean(tr.dataset.roomEnabled, true),
          send_room_summary: parseDatasetBoolean(tr.dataset.roomSendSummary, false),
          calendar_tomorrow_reminder_enabled: parseDatasetBoolean(tr.dataset.roomTomorrowReminder, false),
          media_file_access_enabled: parseDatasetBoolean(tr.dataset.roomMediaAccess, true),
          calendar_ai_auto_create_enabled: parseDatasetBoolean(tr.dataset.roomCalendarAutoCreate, true),
          calendar_silent_auto_register_enabled: parseDatasetBoolean(tr.dataset.roomSilentAutoRegister, false),
          gmail_reservation_alert_enabled: parseDatasetBoolean(tr.dataset.roomGmailAlertEnabled, false),
        }));
        tr.draggable = false;
        tr.innerHTML =
          '<td><span class="room-id-tools"><span class="room-drag-handle" draggable="true" title="ドラッグで並び替え" aria-label="並び替え" role="button">⋮⋮</span><button class="button room-show-id" type="button">ID表示</button></span></td>' +
          '<td><input class="input room-name" type="text" value="' + escapeHtml((setting && setting.room_name) || room.room_name || '') + '"></td>' +
          '<td>' + Number(room.pending_messages || 0) + '件</td>' +
          '<td>' + formatDate(room.last_message_at) + '</td>' +
          '<td><button class="button room-config-open" type="button">設定</button><div class="room-config-badge ' + configToneClass + '">' + escapeHtml(configSummary) + '</div></td>' +
          '<td><input class="input room-hours" type="text" placeholder="空欄=全体設定" value="' + escapeHtml(setting && Array.isArray(setting.delivery_hours) ? setting.delivery_hours.join(',') : ROOM_DEFAULT_HOURS) + '"></td>' +
          '<td><select class="select room-summary-mode">' +
          '<option value="" ' + (((setting && setting.last_delivery_summary_mode) ? '' : 'selected')) + '>継承</option>' +
          '<option value="independent" ' + ((setting && setting.last_delivery_summary_mode === 'independent') ? 'selected' : '') + '>各回独立</option>' +
          '<option value="daily_rollup" ' + ((setting && setting.last_delivery_summary_mode === 'daily_rollup') ? 'selected' : '') + '>1日まとめ</option>' +
          '</select></td>' +
          '<td><span class="row-actions">' +
          '<button class="button ghost room-reset">継承</button>' +
          '<button class="button warn room-delete">ルーム削除</button>' +
          '</span></td>';
        dom.roomTableBody.appendChild(tr);
      });
    }

    function openRoomConfigModal(tr) {
      activeRoomConfigRow = tr;
      const roomId = String(tr.dataset.roomId || '').trim();
      const nameInput = tr.querySelector('.room-name');
      const roomName = nameInput ? String(nameInput.value || '').trim() : '';
      dom.roomConfigModalMeta.textContent = '対象ルーム: ' + (roomName || roomId || '(未設定)');
      const config = getRoomConfigStateFromRow(tr);
      dom.roomConfigEnabled.checked = !!config.is_enabled;
      dom.roomConfigSendSummary.checked = !!config.send_room_summary;
      dom.roomConfigTomorrowReminder.checked = !!config.calendar_tomorrow_reminder_enabled;
      dom.roomConfigMediaAccess.checked = !!config.media_file_access_enabled;
      dom.roomConfigAutoCreate.checked = !!config.calendar_ai_auto_create_enabled;
      dom.roomConfigSilentAutoRegister.checked = !!config.calendar_silent_auto_register_enabled;
      dom.roomConfigGmailAlert.checked = !!config.gmail_reservation_alert_enabled;
      dom.roomConfigModal.classList.add('open');
      dom.roomConfigModal.setAttribute('aria-hidden', 'false');
    }

    function closeRoomConfigModal() {
      activeRoomConfigRow = null;
      dom.roomConfigModal.classList.remove('open');
      dom.roomConfigModal.setAttribute('aria-hidden', 'true');
    }

    async function saveRoomConfigModal() {
      if (!activeRoomConfigRow) return;
      const nextConfig = {
        is_enabled: !!dom.roomConfigEnabled.checked,
        send_room_summary: !!dom.roomConfigSendSummary.checked,
        calendar_tomorrow_reminder_enabled: !!dom.roomConfigTomorrowReminder.checked,
        media_file_access_enabled: !!dom.roomConfigMediaAccess.checked,
        calendar_ai_auto_create_enabled: !!dom.roomConfigAutoCreate.checked,
        calendar_silent_auto_register_enabled: !!dom.roomConfigSilentAutoRegister.checked,
        gmail_reservation_alert_enabled: !!dom.roomConfigGmailAlert.checked,
      };
      applyRoomConfigStateToRow(activeRoomConfigRow, nextConfig);
      await saveRoomFromRow(activeRoomConfigRow);
      alert('ルーム設定を保存しました。');
      closeRoomConfigModal();
    }

    function openRoomIdModal(tr) {
      const roomId = tr.dataset.roomId || '-';
      const nameInput = tr.querySelector('.room-name');
      const roomName = nameInput ? nameInput.value.trim() : '';
      const titleEl = document.getElementById('roomIdModalTitle');
      if (titleEl) titleEl.textContent = 'ルームID';
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
      renderUserPermissions(state.user_permissions || []);
      renderStorageUsage(state.storage_usage, state.storage_usage_error, state.generated_at);
      dom.lastRefresh.textContent = '最終更新: ' + formatDate(state.generated_at);
      isGlobalDirty = false;
    }

    function buildUserPermissionPayloadFromRow(tr) {
      const lineUserId = String(tr.dataset.lineUserId || '').trim();
      const displayNameRaw = String(tr.dataset.displayName || '').trim();
      let excludedMessageSearchRoomIds = [];
      try {
        const parsed = JSON.parse(String(tr.dataset.excludedMessageSearchRoomIds || '[]'));
        excludedMessageSearchRoomIds = Array.isArray(parsed)
          ? parsed.map(function(v) { return String(v || '').trim(); }).filter(function(v) { return !!v; })
          : [];
      } catch (_) {
        excludedMessageSearchRoomIds = [];
      }
      const noteInput = tr.querySelector('.user-note-input');
      const isActiveInput = tr.querySelector('.user-is-active');
      const canMessageSearchInput = tr.querySelector('.user-can-message-search');
      const canLibrarySearchInput = tr.querySelector('.user-can-library-search');
      const canCalendarCreateInput = tr.querySelector('.user-can-calendar-create');
      const canCalendarUpdateInput = tr.querySelector('.user-can-calendar-update');
      const canMediaAccessInput = tr.querySelector('.user-can-media-access');
      if (!lineUserId) throw new Error('line_user_id の取得に失敗しました。');
      return {
        line_user_id: lineUserId,
        display_name: (displayNameRaw && displayNameRaw !== '-') ? displayNameRaw : null,
        is_active: !!(isActiveInput && isActiveInput.checked),
        can_message_search: !!(canMessageSearchInput && canMessageSearchInput.checked),
        can_library_search: !!(canLibrarySearchInput && canLibrarySearchInput.checked),
        can_calendar_create: !!(canCalendarCreateInput && canCalendarCreateInput.checked),
        can_calendar_update: !!(canCalendarUpdateInput && canCalendarUpdateInput.checked),
        can_media_access: !!(canMediaAccessInput && canMediaAccessInput.checked),
        excluded_message_search_room_ids: excludedMessageSearchRoomIds,
        note: noteInput ? String(noteInput.value || '').trim() || null : null,
      };
    }

    async function saveSingleUserPermissionRow(tr, options) {
      const opts = options || {};
      const payload = buildUserPermissionPayloadFromRow(tr);
      await api('/permissions/users', {
        method: 'PUT',
        body: JSON.stringify(payload),
      });
      if (opts.reload !== false) {
        await safeLoadState();
      } else {
        isUserPermissionDirty = false;
      }
    }

    async function backfillUserPermissions() {
      const response = await api('/permissions/users/backfill', {
        method: 'POST',
        body: JSON.stringify({}),
      });
      await safeLoadState();
      const stats = response && response.backfill ? response.backfill : {};
      alert(
        '既存ユーザー取込が完了しました。'
        + '\n検出: ' + Number(stats.scanned_user_ids || 0) + '件'
        + '\n新規追加: ' + Number(stats.inserted || 0) + '件'
        + '\n既存: ' + Number(stats.already_existing || 0) + '件'
        + '\n表示名更新: ' + Number(stats.display_name_updated || 0) + '件'
      );
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
      isGlobalDirty = false;
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
      const hoursInput = tr.querySelector('.room-hours');
      const summaryModeInput = tr.querySelector('.room-summary-mode');
      const roomConfig = getRoomConfigStateFromRow(tr);
      const roomCleanupTiming = normalizeOptionalSelectValue(String(tr.dataset.messageCleanupTiming || ''));
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
        is_enabled: roomConfig.is_enabled,
        send_room_summary: roomConfig.send_room_summary,
        calendar_tomorrow_reminder_enabled: roomConfig.calendar_tomorrow_reminder_enabled,
        message_search_enabled: messageSearchEnabled,
        message_search_library_enabled: messageSearchLibraryEnabled,
        media_file_access_enabled: roomConfig.media_file_access_enabled,
        bot_reply_enabled: botReplyEnabled,
        calendar_ai_auto_create_enabled: roomConfig.calendar_ai_auto_create_enabled,
        calendar_silent_auto_register_enabled: roomConfig.calendar_silent_auto_register_enabled,
        gmail_reservation_alert_enabled: roomConfig.gmail_reservation_alert_enabled,
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

    async function saveRoomFromRow(tr, options) {
      const opts = options || {};
      const visualIndex = Array.from(dom.roomTableBody.querySelectorAll('tr')).indexOf(tr);
      const payload = buildRoomSettingsPayloadFromRow(tr, visualIndex >= 0 ? visualIndex : 0);

      await api('/settings/rooms', {
        method: 'PUT',
        body: JSON.stringify(payload),
      });
      isRoomDirty = false;
      if (opts.reload !== false) {
        await safeLoadState();
      }
    }

    async function resetRoomToGlobal(tr) {
      const roomId = tr.dataset.roomId || '';
      await api('/settings/rooms/' + encodeURIComponent(roomId), { method: 'DELETE' });
      isRoomDirty = false;
      try {
        await refreshRoomNames(roomId);
      } catch (_) {
        // ignore name refresh failure on reset
      }
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
      refreshNewRoomConfigSummary();
      await safeLoadState();
    }

    async function refreshRoomNames(roomId) {
      const payload = roomId ? { room_id: roomId } : {};
      const response = await api('/rooms/refresh-names', {
        method: 'POST',
        body: JSON.stringify(payload),
      });
      return response && response.refresh ? response.refresh : null;
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
      dom.userPermissionTableBody.innerHTML = '';
      dom.globalMeta.textContent = '';
      dom.storageUsageSummary.textContent = '';
      dom.storageUsageDetails.textContent = '';
      dom.lastRefresh.textContent = '最終更新: なし';
      renderGmailAccountState(null);
      isUserPermissionDirty = false;
    });

    dom.reloadBtn.addEventListener('click', async function() {
      try {
        await safeLoadState();
      } catch (e) {
        alert(e.message || String(e));
      }
    });

    dom.reloadUserPermissionsBtn.addEventListener('click', async function() {
      try {
        await safeLoadState();
      } catch (e) {
        alert(e.message || String(e));
      }
    });

    dom.backfillUserPermissionsBtn.addEventListener('click', async function() {
      try {
        await backfillUserPermissions();
      } catch (e) {
        alert(e.message || String(e));
      }
    });

    dom.userPermissionTableBody.addEventListener('click', async function(event) {
      const target = event.target;
      if (!(target instanceof HTMLElement)) return;
      const card = target.closest('.user-permission-card');
      if (!card) return;
      if (target.classList.contains('user-show-id')) {
        openLineUserIdModal(card);
        return;
      }
      if (target.classList.contains('user-room-scope')) {
        openUserRoomScopeModal(card);
        return;
      }
    });

    dom.userPermissionTableBody.addEventListener('input', function(event) {
      const target = event.target;
      if (!(target instanceof HTMLElement)) return;
      if (target.closest('.user-permission-card')) markUserPermissionDirty();
    });

    dom.userPermissionTableBody.addEventListener('change', function(event) {
      const target = event.target;
      if (!(target instanceof HTMLElement)) return;
      const card = target.closest('.user-permission-card');
      if (!card) return;
      if (
        target.classList.contains('user-permission-check') ||
        target.classList.contains('user-note-input')
      ) {
        markUserPermissionDirty();
        saveSingleUserPermissionRow(card, { reload: false }).catch(function(e) {
          alert(e.message || String(e));
          safeLoadState({ silent: true });
        });
        return;
      }
      markUserPermissionDirty();
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

    [
      dom.globalEnabled,
      dom.globalHoursInput,
      dom.messageCleanupTiming,
      dom.lastDeliverySummaryMode,
      dom.messageRetentionDays,
      dom.tomorrowReminderEnabled,
      dom.tomorrowReminderHoursInput,
      dom.tomorrowReminderOnlyIfEvents,
      dom.tomorrowReminderMaxItems,
    ].forEach(function(el) {
      if (!el) return;
      el.addEventListener('input', markGlobalDirty);
      el.addEventListener('change', markGlobalDirty);
    });

    dom.addRoomBtn.addEventListener('click', async function() {
      try {
        await addRoomSetting();
      } catch (e) {
        alert(e.message || String(e));
      }
    });

    dom.refreshRoomNamesBtn.addEventListener('click', async function() {
      try {
        const stats = await refreshRoomNames();
        await safeLoadState();
        alert(
          'ルーム名の再取得が完了しました。'
          + '\n対象: ' + Number(stats && stats.attempted || 0) + '件'
          + '\n更新: ' + Number(stats && stats.refreshed || 0) + '件'
          + '\n未取得: ' + Number(stats && stats.not_found || 0) + '件'
        );
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
        if (target.classList.contains('room-reset')) {
          await resetRoomToGlobal(tr);
          alert('ルーム設定を削除し、全体設定継承に戻しました。');
        } else if (target.classList.contains('room-delete')) {
          await deleteRoomCompletely(tr);
          alert('ルームを削除しました。');
        } else if (target.classList.contains('room-config-open')) {
          openRoomConfigModal(tr);
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
      const tr = target.closest('tr');
      if (!tr) return;
      markRoomDirty();
      if (target.classList.contains('room-name') || target.classList.contains('room-hours')) {
        scheduleRoomAutoSave(tr, 500);
      }
    });

    dom.roomTableBody.addEventListener('change', function(event) {
      const target = event.target;
      if (!(target instanceof HTMLElement)) return;
      const tr = target.closest('tr');
      if (!tr) return;
      markRoomDirty();
      if (
        target.classList.contains('room-name') ||
        target.classList.contains('room-hours') ||
        target.classList.contains('room-summary-mode')
      ) {
        scheduleRoomAutoSave(tr, 0);
      }
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
    [
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
      el.addEventListener('change', refreshNewRoomConfigSummary);
    });

    dom.closeRoomIdBtn.addEventListener('click', function() {
      closeRoomIdModal();
    });

    dom.openNewRoomConfigBtn.addEventListener('click', function() {
      openNewRoomConfigModal();
    });

    dom.cancelNewRoomConfigBtn.addEventListener('click', function() {
      closeNewRoomConfigModal();
    });

    dom.saveNewRoomConfigBtn.addEventListener('click', function() {
      refreshNewRoomConfigSummary();
      closeNewRoomConfigModal();
    });

    dom.cancelRoomConfigBtn.addEventListener('click', function() {
      closeRoomConfigModal();
    });

    dom.saveRoomConfigBtn.addEventListener('click', async function() {
      try {
        await saveRoomConfigModal();
      } catch (e) {
        alert(e.message || String(e));
      }
    });

    dom.cancelUserRoomScopeBtn.addEventListener('click', function() {
      closeUserRoomScopeModal();
    });

    dom.saveUserRoomScopeBtn.addEventListener('click', async function() {
      try {
        await saveUserRoomScopeSelection();
      } catch (e) {
        alert(e.message || String(e));
      }
    });

    dom.copyRoomIdBtn.addEventListener('click', async function() {
      await copyRoomIdToClipboard();
    });

    dom.roomIdModal.addEventListener('click', function(event) {
      if (event.target === dom.roomIdModal) {
        closeRoomIdModal();
      }
    });

    dom.newRoomConfigModal.addEventListener('click', function(event) {
      if (event.target === dom.newRoomConfigModal) {
        closeNewRoomConfigModal();
      }
    });

    dom.roomConfigModal.addEventListener('click', function(event) {
      if (event.target === dom.roomConfigModal) {
        closeRoomConfigModal();
      }
    });

    dom.userRoomScopeModal.addEventListener('click', function(event) {
      if (event.target === dom.userRoomScopeModal) {
        closeUserRoomScopeModal();
      }
    });

    document.addEventListener('visibilitychange', function() {
      if (!token()) return;
      scheduleAutoRefresh();
      if (document.visibilityState === 'visible' && !isEditingRoomForm() && !isGlobalDirty && !isRoomDirty && !isUserPermissionDirty) {
        safeLoadState({ silent: true });
      }
    });

    window.addEventListener('focus', function() {
      if (!token() || isEditingRoomForm() || isGlobalDirty || isRoomDirty || isUserPermissionDirty) return;
      safeLoadState({ silent: true });
    });

    document.addEventListener('keydown', function(event) {
      if (event.key === 'Escape' && dom.roomIdModal.classList.contains('open')) {
        closeRoomIdModal();
      }
      if (event.key === 'Escape' && dom.newRoomConfigModal.classList.contains('open')) {
        closeNewRoomConfigModal();
      }
      if (event.key === 'Escape' && dom.roomConfigModal.classList.contains('open')) {
        closeRoomConfigModal();
      }
      if (event.key === 'Escape' && dom.userRoomScopeModal.classList.contains('open')) {
        closeUserRoomScopeModal();
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
