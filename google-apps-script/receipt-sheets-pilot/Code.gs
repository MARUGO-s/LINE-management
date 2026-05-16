/**
 * 1店舗パイロット: スプレッドシート ↔ Supabase 同期（GAS 経由）
 *
 * シートの読み書きは SpreadsheetApp（このスクリプト）が行い、
 * Supabase Edge は DB のみ更新します（Google Sheets API の有効化は不要）。
 *
 * スクリプト プロパティ:
 *   SUPABASE_RECEIPT_SHEETS_SYNC_URL  … https://<project>.supabase.co/functions/v1/receipt-sheets-sync-cron
 *   RECEIPT_SHEETS_SYNC_SECRET         … Supabase Edge secret と同じ値
 */

var TAB_BUDGETS = ['月間予算', 'monthly_budgets'];
var TAB_PAST = ['過去売上', 'past_sales'];
var TAB_DAILY = ['日次売上', 'daily_sales'];
var TAB_LOG = ['同期ログ', 'sync_log'];

function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu('売上連携（パイロット）')
    .addItem('双方向同期（予算・過去売上を取込＋日次を書出）', 'syncBoth')
    .addItem('取込のみ（予算・過去売上 → DB）', 'syncPull')
    .addItem('書出のみ（日次売上 → シート）', 'syncPush')
    .addToUi();
}

function syncBoth() {
  runSyncViaGas_('both');
}

function syncPull() {
  runSyncViaGas_('pull');
}

function syncPush() {
  runSyncViaGas_('push');
}

function runSyncViaGas_(direction) {
  var url = PropertiesService.getScriptProperties().getProperty('SUPABASE_RECEIPT_SHEETS_SYNC_URL');
  var secret = PropertiesService.getScriptProperties().getProperty('RECEIPT_SHEETS_SYNC_SECRET');
  if (!url || !secret) {
    SpreadsheetApp.getUi().alert(
      'スクリプト プロパティに SUPABASE_RECEIPT_SHEETS_SYNC_URL と RECEIPT_SHEETS_SYNC_SECRET を設定してください。',
    );
    return;
  }

  var payload = { direction: direction, via_gas: true };
  if (direction === 'pull' || direction === 'both') {
    payload.monthly_budget_rows = readSheetData_(TAB_BUDGETS, 2, 1, 500, 9);
    payload.past_sales_rows = readSheetData_(TAB_PAST, 2, 1, 500, 4);
  } else if (direction === 'push') {
    // 書出のみでも休業日は月間予算の行位置が必要
    payload.monthly_budget_rows = readSheetData_(TAB_BUDGETS, 2, 1, 500, 9);
  }

  var response = UrlFetchApp.fetch(url, {
    method: 'post',
    contentType: 'application/json',
    headers: {
      Authorization: 'Bearer ' + secret,
      'x-receipt-sheets-sync-key': secret,
    },
    payload: JSON.stringify(payload),
    muteHttpExceptions: true,
  });

  var code = response.getResponseCode();
  var text = response.getContentText();
  if (code < 200 || code >= 300) {
    SpreadsheetApp.getUi().alert('同期失敗 (' + code + ')\n\n' + text.slice(0, 1200));
    return;
  }

  var result;
  try {
    result = JSON.parse(text);
  } catch (e) {
    SpreadsheetApp.getUi().alert('応答の解析に失敗しました\n\n' + text.slice(0, 800));
    return;
  }

  if (result && result.skipped === true) {
    SpreadsheetApp.getUi().alert(
      '同期は実行されませんでした\n\n' + (result.hint || result.reason || text).slice(0, 1200),
    );
    return;
  }

  try {
    if (result.sheet_export) {
      applySheetExport_(result.sheet_export, result.store_partition_key);
    }
    if (result.sync_log_row) {
      appendSyncLogRow_(result.sync_log_row);
    }
  } catch (e) {
    SpreadsheetApp.getUi().alert(
      'DB 同期は完了しましたが、シートへの書込に失敗しました:\n\n' + String(e),
    );
    return;
  }

  SpreadsheetApp.getUi().alert('同期完了\n\n' + text.slice(0, 1200));
}

function readSheetData_(tabNames, startRow, startCol, numRows, numCols) {
  var sheet = findSheetByName_(tabNames);
  var values = sheet.getRange(startRow, startCol, numRows, numCols).getValues();
  var out = [];
  for (var i = 0; i < values.length; i++) {
    if (rowHasContent_(values[i])) {
      out.push(serializeSheetRow_(values[i]));
    }
  }
  return out;
}

/** Date 型セルを yyyy-MM 文字列にしてサーバー側の月照合を安定させる */
function serializeSheetRow_(row) {
  var out = [];
  for (var c = 0; c < row.length; c++) {
    out.push(serializeSheetCell_(row[c], c === 0));
  }
  return out;
}

function serializeSheetCell_(value, isMonthColumn) {
  if (value instanceof Date && !isNaN(value.getTime())) {
    if (isMonthColumn) {
      return Utilities.formatDate(value, 'Asia/Tokyo', 'yyyy-MM');
    }
    return Utilities.formatDate(value, 'Asia/Tokyo', 'yyyy-MM-dd');
  }
  return value;
}

function rowHasContent_(row) {
  for (var c = 0; c < row.length; c++) {
    if (row[c] !== '' && row[c] !== null) {
      return true;
    }
  }
  return false;
}

function applySheetExport_(sheetExport, storePartitionKey) {
  if (sheetExport.daily_sales) {
    var daily = sheetExport.daily_sales;
    var dailySheet = findSheetByName_(TAB_DAILY);
    var dailyData = [daily.header].concat(daily.rows);
    if (dailyData.length > 0 && dailyData[0].length > 0) {
      dailySheet
        .getRange(1, 1, dailyData.length, dailyData[0].length)
        .setValues(dailyData);
    }
  }

  if (sheetExport.closed_dates_updates && sheetExport.closed_dates_updates.length > 0) {
    applyClosedDatesUpdates_(sheetExport.closed_dates_updates);
    return;
  }

  var closedByMonth = sheetExport.closed_dates_by_month || {};
  if (Object.keys(closedByMonth).length > 0) {
    applyClosedDatesToBudgetSheet_(closedByMonth, storePartitionKey);
  }
}

function applyClosedDatesUpdates_(updates) {
  var sheet = findSheetByName_(TAB_BUDGETS);
  styleClosedDatesColumn_(sheet);
  for (var u = 0; u < updates.length; u++) {
    var item = updates[u];
    var row = Number(item.row);
    if (!row || row < 2) {
      continue;
    }
    var cell = sheet.getRange(row, 8);
    cell.setValue(item.value || '');
    styleClosedDatesCell_(cell);
  }
}

function styleClosedDatesColumn_(sheet) {
  if (sheet.getColumnWidth(8) < 120) {
    sheet.setColumnWidth(8, 148);
  }
}

function styleClosedDatesCell_(range) {
  range.setWrap(true);
  range.setVerticalAlignment('top');
  range.setHorizontalAlignment('left');
}

function applyClosedDatesToBudgetSheet_(datesByMonth, storePartitionKey) {
  var sheet = findSheetByName_(TAB_BUDGETS);
  var values = sheet.getRange(2, 1, 500, 9).getValues();
  var pilotKey = String(storePartitionKey || '').trim().toLowerCase();

  for (var i = 0; i < values.length; i++) {
    var month = normalizeMonthCell_(values[i][0]);
    var storeKey = String(values[i][2] || '')
      .trim()
      .toLowerCase();
    if (!month || storeKey !== pilotKey) {
      continue;
    }
    if (!datesByMonth.hasOwnProperty(month)) {
      continue;
    }
    var closed = datesByMonth[month] || [];
    var cellRange = sheet.getRange(i + 2, 8);
    cellRange.setValue(closed.length > 0 ? closed.join(',') : '');
    styleClosedDatesCell_(cellRange);
  }
  styleClosedDatesColumn_(sheet);
}

function appendSyncLogRow_(row) {
  var sheet = findSheetByName_(TAB_LOG);
  var header = sheet.getRange(1, 1, 1, 6).getValues()[0];
  if (!header[0] || String(header[0]).trim() === '') {
    sheet
      .getRange(1, 1, 1, 6)
      .setValues([['同期日時', '方向', '店舗キー', '取込結果', '書出結果', 'メモ']]);
  }
  sheet.appendRow(row);
}

function findSheetByName_(names) {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  for (var i = 0; i < names.length; i++) {
    var sh = ss.getSheetByName(names[i]);
    if (sh) {
      return sh;
    }
  }
  throw new Error('シートが見つかりません: ' + names.join(' / '));
}

function normalizeMonthCell_(raw) {
  if (raw instanceof Date && !isNaN(raw.getTime())) {
    return Utilities.formatDate(raw, 'Asia/Tokyo', 'yyyy-MM');
  }
  if (typeof raw === 'number' && raw > 20000 && raw < 120000) {
    var ms = Math.round((raw - 25569) * 86400 * 1000);
    var d = new Date(ms);
    if (!isNaN(d.getTime())) {
      return Utilities.formatDate(d, 'Asia/Tokyo', 'yyyy-MM');
    }
  }
  var s = String(raw || '').trim();
  var m = s.match(/^(\d{4})-(\d{2})$/);
  if (m) {
    return s;
  }
  var isoDay = s.match(/^(\d{4})-(\d{2})-\d{2}/);
  if (isoDay) {
    return isoDay[1] + '-' + isoDay[2];
  }
  var loose = s.match(/^(\d{4})[\/\-](\d{1,2})$/);
  if (loose) {
    var mo = Math.min(12, Math.max(1, parseInt(loose[2], 10)));
    return loose[1] + '-' + ('0' + mo).slice(-2);
  }
  return null;
}
