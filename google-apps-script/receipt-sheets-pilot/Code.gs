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
var TAB_OPS = ['操作', 'ops', '操作パネル'];

function onOpen() {
  try {
    ensureOperationSheetCurrent_();
  } catch (e) {
    Logger.log('ensureOperationSheetCurrent_: ' + e);
  }
  SpreadsheetApp.getUi()
    .createMenu('売上連携（パイロット）')
    .addItem('双方向同期（予算・過去売上を取込＋日次を書出）', 'syncBoth')
    .addItem('取込のみ（予算・過去売上 → DB）', 'syncPull')
    .addItem('書出のみ（日次売上 → シート）', 'syncPush')
    .addSeparator()
    .addItem('操作シートを作成・更新（スマホ用）', 'setupOperationSheet')
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

/**
 * @param {string} direction pull|push|both
 * @param {{silent?: boolean}} opts silent=true のとき alert せず {ok,message} を返す（操作シート・スマホ用）
 * @return {{ok: boolean, message: string}}
 */
function runSyncViaGas_(direction, opts) {
  opts = opts || {};
  var silent = opts.silent === true;
  var out = { ok: false, message: '' };

  var url = PropertiesService.getScriptProperties().getProperty('SUPABASE_RECEIPT_SHEETS_SYNC_URL');
  var secret = PropertiesService.getScriptProperties().getProperty('RECEIPT_SHEETS_SYNC_SECRET');
  if (!url || !secret) {
    out.message =
      'スクリプト プロパティに SUPABASE_RECEIPT_SHEETS_SYNC_URL と RECEIPT_SHEETS_SYNC_SECRET を設定してください。';
    if (!silent) SpreadsheetApp.getUi().alert(out.message);
    return out;
  }

  var payload = { direction: direction, via_gas: true };
  if (direction === 'pull' || direction === 'both') {
    upgradePastSalesSheetLayout_();
    upgradeMonthlyBudgetSheetLayout_();
    fillMonthlyBudgetOperatingDays_(findSheetByName_(TAB_BUDGETS));
    payload.monthly_budget_rows = readSheetData_(TAB_BUDGETS, 2, 1, 500, 10);
    payload.past_sales_rows = readSheetData_(TAB_PAST, 2, 1, 500, 7);
  } else if (direction === 'push') {
    upgradeMonthlyBudgetSheetLayout_();
    fillMonthlyBudgetOperatingDays_(findSheetByName_(TAB_BUDGETS));
    payload.monthly_budget_rows = readSheetData_(TAB_BUDGETS, 2, 1, 500, 10);
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
    out.message = '同期失敗 (' + code + ')\n\n' + text.slice(0, 1200);
    if (!silent) SpreadsheetApp.getUi().alert(out.message);
    return out;
  }

  var result;
  try {
    result = JSON.parse(text);
  } catch (e) {
    out.message = '応答の解析に失敗しました\n\n' + text.slice(0, 800);
    if (!silent) SpreadsheetApp.getUi().alert(out.message);
    return out;
  }

  if (result && result.skipped === true) {
    out.message = '同期は実行されませんでした\n\n' + (result.hint || result.reason || text).slice(0, 1200);
    if (!silent) SpreadsheetApp.getUi().alert(out.message);
    return out;
  }

  try {
    if (result.sheet_export) {
      applySheetExport_(result.sheet_export, result.store_partition_key);
    }
    if (result.sync_log_row) {
      appendSyncLogRow_(result.sync_log_row);
    }
  } catch (e) {
    out.message = 'DB 同期は完了しましたが、シートへの書込に失敗しました:\n\n' + String(e);
    if (!silent) SpreadsheetApp.getUi().alert(out.message);
    return out;
  }

  out.ok = true;
  out.message = '同期完了';
  if (!silent) SpreadsheetApp.getUi().alert('同期完了');
  return out;
}

/** 月間予算を A〜J（I=営業日数, J=有効）。取込時に自動実行。 */
function upgradeMonthlyBudgetSheetLayout_() {
  var sheet = findSheetByName_(TAB_BUDGETS);
  var headerI = String(sheet.getRange(1, 9).getValue() || '').trim();
  var headerJ = String(sheet.getRange(1, 10).getValue() || '').trim();

  if (headerI === '営業日数' && headerJ === '有効') {
    return '月間予算タブはすでに10列形式です（I=営業日数, J=有効）。';
  }

  if (headerI === '有効') {
    sheet.insertColumnBefore(9);
    sheet.getRange(1, 9).setValue('営業日数');
    sheet.getRange(1, 10).setValue('有効');
    fillMonthlyBudgetOperatingDays_(sheet);
    normalizeAllClosedDatesCellsToSingleLine_(sheet);
    return (
      'I列に「営業日数」を追加しました。\n' +
      '「有効」は J列 に移っています。\n' +
      'H列の休業日から営業日数を自動計算して I列 に書き込みました。'
    );
  }

  sheet.getRange(1, 1, 1, 10).setValues([[
    '対象月',
    '店舗名',
    '店舗キー',
    '月間予算円',
    '平日比率',
    '休日前比率',
    '休日比率',
    '休業日',
    '営業日数',
    '有効',
  ]]);
  fillMonthlyBudgetOperatingDays_(sheet);
  normalizeAllClosedDatesCellsToSingleLine_(sheet);
  return '1行目のヘッダーを10列形式にしました。休業日から営業日数を自動計算します。';
}

/** H列の休業日を1行表記に揃える（既存の改行入りセルを修正） */
function normalizeAllClosedDatesCellsToSingleLine_(sheet) {
  if (!sheet) return;
  var lastRow = sheet.getLastRow();
  if (lastRow < 2) return;
  var numRows = lastRow - 1;
  var range = sheet.getRange(2, 8, numRows, 1);
  var values = range.getValues();
  var changed = false;
  for (var i = 0; i < values.length; i++) {
    var normalized = normalizeClosedDatesCellText_(values[i][0]);
    if (normalized !== String(values[i][0] == null ? '' : values[i][0])) {
      values[i][0] = normalized;
      changed = true;
    }
  }
  if (changed) {
    range.setValues(values);
  }
  for (var r = 2; r <= lastRow; r++) {
    styleClosedDatesCell_(sheet.getRange(r, 8));
  }
}

/** H列（休業日）から I列（営業日数）を再計算 */
function fillMonthlyBudgetOperatingDays_(sheet) {
  if (!sheet) return;
  var headerI = String(sheet.getRange(1, 9).getValue() || '').trim();
  if (headerI !== '営業日数') return;

  var lastRow = sheet.getLastRow();
  if (lastRow < 2) return;

  var numRows = lastRow - 1;
  var values = sheet.getRange(2, 1, numRows, 10).getValues();
  for (var i = 0; i < values.length; i++) {
    var row = values[i];
    if (!rowHasContent_(row)) continue;
    var month = normalizeMonthCell_(row[0]);
    if (!month) continue;
    var opDays = countOperatingDaysFromClosedCell_(month, row[7]);
    row[8] = opDays > 0 ? opDays : '';
    row[7] = normalizeClosedDatesCellText_(row[7]);
  }
  sheet.getRange(2, 1, values.length, 10).setValues(values);
  normalizeAllClosedDatesCellsToSingleLine_(sheet);
}

function applyBudgetOperatingDaysUpdates_(updates) {
  var sheet = findSheetByName_(TAB_BUDGETS);
  for (var u = 0; u < updates.length; u++) {
    var item = updates[u];
    var row = Number(item.row);
    if (!row || row < 2) continue;
    sheet.getRange(row, 9).setValue(item.operating_days);
  }
}

function countOperatingDaysFromClosedCell_(month, closedRaw) {
  var closedDates = parseClosedDatesCellGas_(month, closedRaw);
  var monthDays = buildDateKeysForMonth_(month);
  return Math.max(0, monthDays.length - closedDates.length);
}

function buildDateKeysForMonth_(month) {
  var matched = String(month || '').match(/^(\d{4})-(\d{2})$/);
  if (!matched) return [];
  var year = Number(matched[1]);
  var monthNum = Number(matched[2]);
  var lastDay = new Date(Date.UTC(year, monthNum, 0)).getUTCDate();
  var keys = [];
  for (var day = 1; day <= lastDay; day++) {
    keys.push(
      String(year).padStart(4, '0') +
        '-' +
        String(monthNum).padStart(2, '0') +
        '-' +
        String(day).padStart(2, '0'),
    );
  }
  return keys;
}

function parseClosedDatesCellGas_(month, raw) {
  var s = String(raw == null ? '' : raw).trim();
  if (!s) return [];
  var allowed = {};
  var monthDays = buildDateKeysForMonth_(month);
  for (var i = 0; i < monthDays.length; i++) {
    allowed[monthDays[i]] = true;
  }
  var monthNum = Number(month.slice(5, 7));
  var tokens = s
    .split(/[\n\r]+/)
    .reduce(function(acc, line) {
      return acc.concat(line.split(/[,、]+/));
    }, [])
    .map(function(p) {
      return String(p).trim();
    })
    .filter(function(p) {
      return p.length > 0;
    });
  var out = [];
  for (var t = 0; t < tokens.length; t++) {
    out = out.concat(expandClosedDateTokenGas_(tokens[t], month, monthNum, allowed));
  }
  var uniq = {};
  for (var j = 0; j < out.length; j++) {
    if (allowed[out[j]]) uniq[out[j]] = true;
  }
  return Object.keys(uniq).sort();
}

function expandClosedDateTokenGas_(token, month, monthNum, allowed) {
  var rangeFull = /^(\d{1,2})\/(\d{1,2})[〜~～\-－](\d{1,2})\/(\d{1,2})$/.exec(token);
  if (rangeFull) {
    var m1 = Number(rangeFull[1]);
    var d1 = Number(rangeFull[2]);
    var d2 = Number(rangeFull[4]);
    if (m1 === monthNum) return daysInRangeGas_(month, d1, d2, allowed);
    return [];
  }
  var rangeShort = /^(\d{1,2})\/(\d{1,2})[〜~～\-－](\d{1,2})$/.exec(token);
  if (rangeShort) {
    var m2 = Number(rangeShort[1]);
    var dStart = Number(rangeShort[2]);
    var dEnd = Number(rangeShort[3]);
    if (m2 === monthNum) return daysInRangeGas_(month, dStart, dEnd, allowed);
    return [];
  }
  var dayOnlyRange = /^(\d{1,2})[〜~～\-－](\d{1,2})$/.exec(token);
  if (dayOnlyRange) {
    return daysInRangeGas_(month, Number(dayOnlyRange[1]), Number(dayOnlyRange[2]), allowed);
  }

  var key = token;
  if (/^\d{1,2}\/\d{1,2}$/.test(token)) {
    var parts = token.split('/');
    var mRaw = Number(parts[0]);
    var dRaw = Number(parts[1]);
    if (mRaw === monthNum) {
      key = month + '-' + ('0' + dRaw).slice(-2);
    } else {
      key = month + '-' + ('0' + mRaw).slice(-2) + '-' + ('0' + dRaw).slice(-2);
    }
  } else if (/^\d{1,2}-\d{1,2}$/.test(token)) {
    key = month + '-' + ('0' + token.split('-')[1]).slice(-2);
  }
  if (/^\d{4}-\d{2}-\d{2}$/.test(key) && allowed[key]) {
    return [key];
  }
  return [];
}

function daysInRangeGas_(month, startDay, endDay, allowed) {
  var from = Math.min(startDay, endDay);
  var to = Math.max(startDay, endDay);
  var out = [];
  for (var d = from; d <= to; d++) {
    var iso = month + '-' + ('0' + d).slice(-2);
    if (allowed[iso]) out.push(iso);
  }
  return out;
}

/** 過去売上を A〜G（F=営業日数, G=有効）。取込時に自動実行。 */
function upgradePastSalesSheetLayout_() {
  var sheet = findSheetByName_(TAB_PAST);
  var headerF = String(sheet.getRange(1, 6).getValue() || '').trim();
  var headerG = String(sheet.getRange(1, 7).getValue() || '').trim();

  if (headerF === '営業日数' && headerG === '有効') {
    return '過去売上タブはすでに7列形式です（F=営業日数, G=有効）。';
  }

  if (headerF === '有効') {
    sheet.insertColumnBefore(6);
    sheet.getRange(1, 6).setValue('営業日数');
    sheet.getRange(1, 7).setValue('有効');
    return (
      'F列に「営業日数」を追加しました。\n' +
      'もとの「有効」は G列 に移っています。\n\n' +
      '各月の F列にその月の営業日数（例: 20）を入力してから取込してください。'
    );
  }

  sheet
    .getRange(1, 1, 1, 7)
    .setValues([['対象月', '店舗キー', '総売上円', '会計組数', '客数', '営業日数', '有効']]);
  return '1行目のヘッダーを7列形式にしました。F列に営業日数、G列に有効（TRUE）を入力してください。';
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
      coerceDailySalesNumericColumnsInArray_(dailyData);
      dailySheet
        .getRange(1, 1, dailyData.length, dailyData[0].length)
        .setValues(dailyData);
      var numDataRows = dailyData.length - 1;
      if (numDataRows > 0) {
        paintNegativeCellsRed_(dailySheet, 2, numDataRows, 4, 8);
        applyNegativeRedFormatting_(dailySheet, 2, Math.max(numDataRows, 499), 4, 8);
      }
    }
  }

  if (sheetExport.budget_operating_days_updates && sheetExport.budget_operating_days_updates.length > 0) {
    applyBudgetOperatingDaysUpdates_(sheetExport.budget_operating_days_updates);
  }

  if (sheetExport.closed_dates_updates && sheetExport.closed_dates_updates.length > 0) {
    applyClosedDatesUpdates_(sheetExport.closed_dates_updates);
    fillMonthlyBudgetOperatingDays_(findSheetByName_(TAB_BUDGETS));
    return;
  }

  var closedByMonth = sheetExport.closed_dates_by_month || {};
  if (Object.keys(closedByMonth).length > 0) {
    applyClosedDatesToBudgetSheet_(closedByMonth, storePartitionKey);
    fillMonthlyBudgetOperatingDays_(findSheetByName_(TAB_BUDGETS));
  }
}

/** 文字列の "-40000" などを数値に（条件付き書式・色付け用） */
function coerceSheetNumber_(value) {
  if (typeof value === 'number' && !isNaN(value)) {
    return value;
  }
  if (value === '' || value === null || value === undefined) {
    return null;
  }
  var s = String(value).replace(/,/g, '').trim();
  if (s === '' || s === '-') {
    return null;
  }
  var n = Number(s);
  return isFinite(n) ? n : null;
}

/** 日次売上の数値列（D〜I）を数値型で書き込む */
var DAILY_SALES_NUMERIC_COL_INDEXES = [3, 4, 5, 6, 7, 8];

function coerceDailySalesNumericColumnsInArray_(dailyData) {
  for (var r = 1; r < dailyData.length; r++) {
    var row = dailyData[r];
    for (var i = 0; i < DAILY_SALES_NUMERIC_COL_INDEXES.length; i++) {
      var ci = DAILY_SALES_NUMERIC_COL_INDEXES[i];
      if (ci >= row.length) {
        continue;
      }
      var n = coerceSheetNumber_(row[ci]);
      if (n !== null) {
        row[ci] = n;
      }
    }
  }
}

function coerceDailySalesNumericColumnsInSheet_(sheet, startRow, endRow) {
  var numRows = endRow - startRow + 1;
  var numCols = DAILY_SALES_NUMERIC_COL_INDEXES.length;
  var startCol = DAILY_SALES_NUMERIC_COL_INDEXES[0] + 1;
  var range = sheet.getRange(startRow, startCol, numRows, numCols);
  var values = range.getValues();
  var changed = false;
  for (var r = 0; r < values.length; r++) {
    for (var c = 0; c < values[r].length; c++) {
      var n = coerceSheetNumber_(values[r][c]);
      if (n !== null && values[r][c] !== n) {
        values[r][c] = n;
        changed = true;
      }
    }
  }
  if (changed) {
    range.setValues(values);
  }
}

/** 書込直後にマイナスセルを赤文字（文字列のマイナスにも対応） */
function paintNegativeCellsRed_(sheet, startRow, numRows, startCol, endCol) {
  var numCols = endCol - startCol + 1;
  var range = sheet.getRange(startRow, startCol, numRows, numCols);
  var values = range.getValues();
  var colors = [];
  for (var r = 0; r < values.length; r++) {
    var rowColors = [];
    for (var c = 0; c < values[r].length; c++) {
      var n = coerceSheetNumber_(values[r][c]);
      rowColors.push(n !== null && n < 0 ? '#c62828' : '#000000');
    }
    colors.push(rowColors);
  }
  range.setFontColors(colors);
}

/**
 * 条件付き書式（数値・文字列どちらのマイナスも拾う）。書出のたびに差し替え。
 */
function applyNegativeRedFormatting_(sheet, startRow, numRows, startCol, endCol) {
  var numCols = endCol - startCol + 1;
  var range = sheet.getRange(startRow, startCol, numRows, numCols);
  var colLetter = columnToLetter_(startCol);
  var formula = '=' + colLetter + startRow + '<0';
  var rules = sheet.getConditionalFormatRules();
  var kept = [];
  for (var i = 0; i < rules.length; i++) {
    var cond = rules[i].getBooleanCondition();
    if (cond) {
      var type = cond.getCriteriaType();
      if (
        type === SpreadsheetApp.BooleanCriteria.NUMBER_LESS ||
        type === SpreadsheetApp.BooleanCriteria.CUSTOM_FORMULA
      ) {
        var rs = rules[i].getRanges();
        var onThisSheet = false;
        for (var j = 0; j < rs.length; j++) {
          if (rs[j].getSheet().getName() === sheet.getName()) {
            onThisSheet = true;
            break;
          }
        }
        if (onThisSheet) {
          continue;
        }
      }
    }
    kept.push(rules[i]);
  }
  kept.push(
    SpreadsheetApp.newConditionalFormatRule()
      .whenFormulaSatisfied(formula)
      .setFontColor('#c62828')
      .setRanges([range])
      .build(),
  );
  sheet.setConditionalFormatRules(kept);
}

function columnToLetter_(column) {
  var temp = '';
  var col = column;
  while (col > 0) {
    var mod = (col - 1) % 26;
    temp = String.fromCharCode(65 + mod) + temp;
    col = Math.floor((col - 1) / 26);
  }
  return temp;
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
    cell.setValue(normalizeClosedDatesCellText_(item.value || ''));
    styleClosedDatesCell_(cell);
  }
}

/** 休業日セル内の改行を「、」に置換して1行表示用に整える */
function normalizeClosedDatesCellText_(raw) {
  return String(raw == null ? '' : raw)
    .replace(/[\n\r]+/g, '、')
    .replace(/、+/g, '、')
    .replace(/^、|、$/g, '');
}

/** Edge と同形式: 5/3〜5/6、5/10（1行） */
function formatClosedDatesForSheetCellGas_(isoDates, month) {
  if (!isoDates || !isoDates.length) return '';
  var dayNums = [];
  for (var i = 0; i < isoDates.length; i++) {
    var iso = String(isoDates[i]);
    if (iso.indexOf(month + '-') !== 0) continue;
    var day = Number(iso.slice(8, 10));
    if (day >= 1 && day <= 31) dayNums.push(day);
  }
  if (!dayNums.length) return isoDates.join('、');
  var monthNum = Number(month.slice(5, 7));
  return compressClosedDaysToSegmentsGas_(dayNums, monthNum).join('、');
}

function compressClosedDaysToSegmentsGas_(days, monthNum) {
  var unique = days.slice().sort(function(a, b) {
    return a - b;
  });
  var seen = {};
  var deduped = [];
  for (var u = 0; u < unique.length; u++) {
    if (!seen[unique[u]]) {
      seen[unique[u]] = true;
      deduped.push(unique[u]);
    }
  }
  var segments = [];
  var start = deduped[0];
  var end = deduped[0];
  var fmt = function(day) {
    return monthNum + '/' + day;
  };
  for (var i = 1; i <= deduped.length; i++) {
    var d = deduped[i];
    if (i < deduped.length && d === end + 1) {
      end = d;
      continue;
    }
    segments.push(start === end ? fmt(start) : fmt(start) + '〜' + fmt(end));
    if (i < deduped.length) {
      start = d;
      end = d;
    }
  }
  return segments;
}

function styleClosedDatesColumn_(sheet) {
  if (sheet.getColumnWidth(8) < 120) {
    sheet.setColumnWidth(8, 148);
  }
}

function styleClosedDatesCell_(range) {
  range.setWrap(false);
  range.setVerticalAlignment('middle');
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
    cellRange.setValue(closed.length > 0 ? formatClosedDatesForSheetCellGas_(closed, month) : '');
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

/** 旧レイアウトや余分なチェックボックスがあれば直す */
function ensureOperationSheetCurrent_() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getSheetByName('操作');
  if (!sheet) {
    setupOperationSheet_();
  } else if (isOperationSheetStale_(sheet)) {
    setupOperationSheet_();
  } else if (operationSheetHasExtraCheckboxes_(sheet)) {
    clearOperationSheetExtras_(sheet);
  } else {
    return;
  }
  if (!hasOperationSheetTrigger_()) {
    installOperationSheetTriggerSilent_();
  }
}

function isOperationSheetStale_(sheet) {
  if (!sheet) {
    return true;
  }
  var h3 = String(sheet.getRange(1, 3).getValue() || '').trim();
  var h4 = String(sheet.getRange(1, 4).getValue() || '').trim();
  if (h4 === '説明' || h3 !== '状態') {
    return true;
  }
  if (String(sheet.getRange(2, 1).getValue() || '').trim() !== '双方向同期') {
    return true;
  }
  if (String(sheet.getRange(4, 1).getValue() || '').trim() !== '書出のみ') {
    return true;
  }
  if (String(sheet.getRange(5, 1).getValue() || '').trim() !== '') {
    return true;
  }
  return false;
}

/** 5行目以降に残った旧チェックボックス（clear では消えない） */
function operationSheetHasExtraCheckboxes_(sheet) {
  if (!sheet || sheet.getMaxRows() < 5) {
    return false;
  }
  for (var r = 5; r <= Math.min(sheet.getLastRow() + 2, 30); r++) {
    var dv = sheet.getRange(r, 2).getDataValidation();
    if (dv && dv.getCriteriaType() === SpreadsheetApp.DataValidationCriteria.CHECKBOX) {
      return true;
    }
  }
  return false;
}

function clearOperationSheetExtras_(sheet) {
  var maxR = sheet.getMaxRows();
  if (maxR <= 4) {
    return;
  }
  var numRows = maxR - 4;
  var cols = Math.min(sheet.getMaxColumns(), 12);
  var extra = sheet.getRange(5, 1, numRows, cols);
  extra.clearContent();
  extra.clearFormat();
  extra.clearDataValidations();
}

/** 「操作」タブ：スマホアプリからチェックを入れて同期（PCメニューと同じ処理） */
function setupOperationSheet() {
  try {
    var msg = setupOperationSheet_();
    if (!hasOperationSheetTrigger_()) {
      installOperationSheetTriggerSilent_();
    }
    SpreadsheetApp.getUi().alert(msg);
  } catch (e) {
    SpreadsheetApp.getUi().alert('操作シートの作成に失敗しました:\n\n' + String(e));
  }
}

function setupOperationSheet_() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getSheetByName('操作');
  if (!sheet) {
    sheet = ss.insertSheet('操作', 0);
  } else {
    ss.setActiveSheet(sheet);
  }

  sheet.clear();
  clearOperationSheetExtras_(sheet);
  sheet.getRange(1, 1, 1, 3).setValues([['操作', '実行', '状態']]);
  sheet.getRange(1, 1, 1, 3).setFontWeight('bold').setBackground('#1a73e8').setFontColor('#ffffff');

  var rows = [
    ['双方向同期', false, ''],
    ['取込のみ', false, ''],
    ['書出のみ', false, ''],
  ];
  var dataRows = rows.length;
  sheet.getRange(2, 1, dataRows, 3).setValues(rows);

  var cb = SpreadsheetApp.newDataValidation().requireCheckbox().build();
  sheet.getRange(2, 2, dataRows, 1).setDataValidation(cb);

  clearOperationSheetExtras_(sheet);

  sheet.setColumnWidth(1, 160);
  sheet.setColumnWidth(2, 56);
  sheet.setColumnWidth(3, 80);
  sheet.setFrozenRows(1);

  return '「操作」シートを更新しました。B列のチェックで同期できます。';
}

function isOperationSheet_(name) {
  var n = String(name || '').trim();
  for (var i = 0; i < TAB_OPS.length; i++) {
    if (n === TAB_OPS[i]) return true;
  }
  return false;
}

function hasOperationSheetTrigger_() {
  var triggers = ScriptApp.getProjectTriggers();
  for (var i = 0; i < triggers.length; i++) {
    if (triggers[i].getHandlerFunction() === 'handleOperationSheetEdit') return true;
  }
  return false;
}

function installOperationSheetTriggerSilent_() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var triggers = ScriptApp.getProjectTriggers();
  for (var i = 0; i < triggers.length; i++) {
    if (triggers[i].getHandlerFunction() === 'handleOperationSheetEdit') {
      ScriptApp.deleteTrigger(triggers[i]);
    }
  }
  ScriptApp.newTrigger('handleOperationSheetEdit').forSpreadsheet(ss).onEdit().create();
}

/** インストール型 onEdit（操作シートの B2:B4 チェック） */
function handleOperationSheetEdit(e) {
  if (!e || !e.range) return;
  var sheet = e.range.getSheet();
  if (!isOperationSheet_(sheet.getName())) return;
  if (e.range.getColumn() !== 2) return;
  var row = e.range.getRow();
  if (row < 2 || row > 4) return;
  if (e.value !== true && e.value !== 'TRUE') return;

  e.range.setValue(false);
  var statusCell = sheet.getRange(row, 3);
  statusCell.setValue('実行中…');
  SpreadsheetApp.flush();

  var result;
  try {
    result = runOperationSheetAction_(row);
  } catch (err) {
    result = { ok: false, message: String(err) };
  }

  statusCell.setValue(result.ok ? '完了' : result.message || 'エラー');
}

function runOperationSheetAction_(row) {
  if (row === 2) return runSyncViaGas_('both', { silent: true });
  if (row === 3) return runSyncViaGas_('pull', { silent: true });
  if (row === 4) return runSyncViaGas_('push', { silent: true });
  return { ok: false, message: '不明な操作行です' };
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
