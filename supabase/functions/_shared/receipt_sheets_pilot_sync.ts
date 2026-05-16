import { createClient } from "https://esm.sh/@supabase/supabase-js@2.44.0"
import {
  allocateDailyBudgetsForMonth,
  getDefaultJapaneseHolidaySet,
  parseStoreClosedDatesForMonth,
  type SalesBudgetAllocationWeights,
} from "./sales_budget_allocation.ts"
import {
  appendSpreadsheetValues,
  batchUpdateSpreadsheetValues,
  formatSheetA1Range,
  getSpreadsheetValues,
  updateSpreadsheetValues,
  type SheetValues,
} from "./google_sheets_client.ts"

/** Google スプレッドシートのタブ名（日本語・正） */
export const SHEET_MONTHLY_BUDGETS = "月間予算"
export const SHEET_PAST_SALES = "過去売上"
export const SHEET_DAILY_SALES = "日次売上"
export const SHEET_SYNC_LOG = "同期ログ"
export const SHEET_README = "使い方"

const TAB_ALIASES_BUDGETS = [SHEET_MONTHLY_BUDGETS, "monthly_budgets"]
const TAB_ALIASES_PAST = [SHEET_PAST_SALES, "past_sales"]
const TAB_ALIASES_DAILY = [SHEET_DAILY_SALES, "daily_sales"]
const TAB_ALIASES_LOG = [SHEET_SYNC_LOG, "sync_log"]

function isSheetNotFoundError(e: unknown): boolean {
  const msg = String(e)
  return msg.includes("Unable to parse range") ||
    msg.includes("(404)") ||
    msg.includes("NOT_FOUND") ||
    msg.includes("not found")
}

async function getSheetValuesForTab(
  spreadsheetId: string,
  tabCandidates: string[],
  rangeSuffix: string,
): Promise<{ values: SheetValues; tabName: string }> {
  for (const tab of tabCandidates) {
    try {
      const values = await getSpreadsheetValues(
        spreadsheetId,
        formatSheetA1Range(tab, rangeSuffix),
      )
      return { values, tabName: tab }
    } catch (e) {
      if (!isSheetNotFoundError(e)) throw e
    }
  }
  throw new Error(`シートが見つかりません: ${tabCandidates.join(" / ")}`)
}

async function updateSheetValuesForTab(
  spreadsheetId: string,
  tabCandidates: string[],
  rangeSuffix: string,
  values: SheetValues,
): Promise<string> {
  for (const tab of tabCandidates) {
    try {
      await updateSpreadsheetValues(spreadsheetId, formatSheetA1Range(tab, rangeSuffix), values)
      return tab
    } catch (e) {
      if (!isSheetNotFoundError(e)) throw e
    }
  }
  throw new Error(`シートが見つかりません: ${tabCandidates.join(" / ")}`)
}

async function appendSheetValuesForTab(
  spreadsheetId: string,
  tabCandidates: string[],
  rangeSuffix: string,
  values: SheetValues,
): Promise<string> {
  for (const tab of tabCandidates) {
    try {
      await appendSpreadsheetValues(spreadsheetId, formatSheetA1Range(tab, rangeSuffix), values)
      return tab
    } catch (e) {
      if (!isSheetNotFoundError(e)) throw e
    }
  }
  throw new Error(`シートが見つかりません: ${tabCandidates.join(" / ")}`)
}

export type ReceiptSheetsSyncDirection = "pull" | "push" | "both"

export type ReceiptSheetsPilotConfig = {
  spreadsheetId: string
  storePartitionKey: string
  storeDisplayName: string
}

export type ReceiptSheetsSyncResult = {
  ok: boolean
  store_partition_key: string
  spreadsheet_id: string
  direction: ReceiptSheetsSyncDirection
  pull?: {
    budgets_applied: number
    budgets_skipped: number
    past_sales_applied: number
    past_sales_skipped: number
    errors: string[]
  }
  push?: {
    months_written: string[]
    rows_written: number
    closed_dates_rows_updated: number
  }
  closed_dates_export?: {
    rows_updated: number
    dates_by_month: Record<string, string[]>
    pilot_store_key: string
  }
  log_appended: boolean
  generated_at: string
  via_gas?: boolean
  sheet_export?: ReceiptSheetsGasSheetExport
  sync_log_row?: string[]
}

export type ReceiptSheetsPilotStoreConfig = {
  storePartitionKey: string
  storeDisplayName: string
}

export type ReceiptSheetsGasPullInput = {
  monthly_budget_rows: SheetValues
  past_sales_rows: SheetValues
}

export type ReceiptSheetsGasClosedDateUpdate = {
  row: number
  month: string
  value: string
}

export type ReceiptSheetsGasSheetExport = {
  daily_sales?: { header: string[]; rows: string[][] }
  closed_dates_by_month: Record<string, string[]>
  /** GAS が行番号で H 列へ直接書込（月セルの Date 型ずれを回避） */
  closed_dates_updates?: ReceiptSheetsGasClosedDateUpdate[]
}

function readReceiptSheetsPilotStoreConfig(): ReceiptSheetsPilotStoreConfig | null {
  const storePartitionKey = (
    Deno.env.get("RECEIPT_SHEETS_PILOT_STORE_KEY") ?? "bistrocavacava"
  ).trim().toLowerCase()
  const storeDisplayName = (
    Deno.env.get("RECEIPT_SHEETS_PILOT_STORE_NAME") ?? "BISTRO CAVA CAVA"
  ).trim()
  if (!/^[a-z0-9]{2,120}$/.test(storePartitionKey)) return null
  return { storePartitionKey, storeDisplayName }
}

export function readReceiptSheetsPilotConfig(): ReceiptSheetsPilotConfig | null {
  const store = readReceiptSheetsPilotStoreConfig()
  const spreadsheetId = (Deno.env.get("RECEIPT_SHEETS_PILOT_SPREADSHEET_ID") ?? "").trim()
  if (!store || !spreadsheetId) return null
  return { spreadsheetId, ...store }
}

export async function runReceiptSheetsPilotSync(
  supabase: ReturnType<typeof createClient>,
  direction: ReceiptSheetsSyncDirection,
): Promise<ReceiptSheetsSyncResult> {
  const config = readReceiptSheetsPilotConfig()
  if (!config) {
    throw new Error(
      "RECEIPT_SHEETS_PILOT_SPREADSHEET_ID is not set. See docs/RECEIPT_SHEETS_PILOT.md.",
    )
  }

  const result: ReceiptSheetsSyncResult = {
    ok: true,
    store_partition_key: config.storePartitionKey,
    spreadsheet_id: config.spreadsheetId,
    direction,
    log_appended: false,
    generated_at: new Date().toISOString(),
  }

  const logLines: string[] = []

  if (direction === "pull" || direction === "both") {
    result.pull = await pullFromSheetsToDb(supabase, config, logLines)
  }
  if (direction === "push" || direction === "both") {
    result.push = await pushDailySalesToSheet(supabase, config, logLines)
  }
  if (direction === "pull" || direction === "push" || direction === "both") {
    result.closed_dates_export = await exportClosedDatesFromDbToBudgetSheet(supabase, config)
    logLines.push(
      `closed_export rows=${result.closed_dates_export.rows_updated} months=${
        Object.keys(result.closed_dates_export.dates_by_month).join(",") || "(none)"
      }`,
    )
  }

  try {
    await appendSyncLog(config.spreadsheetId, [
      new Date().toISOString(),
      direction,
      config.storePartitionKey,
      result.pull
        ? `budgets=${result.pull.budgets_applied} past=${result.pull.past_sales_applied} err=${result.pull.errors.length}`
        : "-",
      result.push ? `rows=${result.push.rows_written} months=${result.push.months_written.join(",")}` : "-",
      logLines.slice(0, 3).join(" | ") || "ok",
    ])
    result.log_appended = true
  } catch (e) {
    console.error("appendSyncLog failed:", e)
    result.ok = false
  }

  return result
}

/** GAS がシートを読み書きし、サーバーは DB のみ操作（Google Sheets API 不要） */
export async function runReceiptSheetsPilotSyncViaGas(
  supabase: ReturnType<typeof createClient>,
  direction: ReceiptSheetsSyncDirection,
  input: Partial<ReceiptSheetsGasPullInput>,
): Promise<ReceiptSheetsSyncResult> {
  const store = readReceiptSheetsPilotStoreConfig()
  if (!store) {
    throw new Error("RECEIPT_SHEETS_PILOT_STORE_KEY is invalid or missing.")
  }
  const config: ReceiptSheetsPilotConfig = {
    spreadsheetId: (Deno.env.get("RECEIPT_SHEETS_PILOT_SPREADSHEET_ID") ?? "").trim() || "gas-client",
    ...store,
  }

  const result: ReceiptSheetsSyncResult = {
    ok: true,
    store_partition_key: config.storePartitionKey,
    spreadsheet_id: config.spreadsheetId,
    direction,
    log_appended: false,
    generated_at: new Date().toISOString(),
    via_gas: true,
  }

  const logLines: string[] = []
  const sheetExport: ReceiptSheetsGasSheetExport = { closed_dates_by_month: {} }

  if (direction === "pull" || direction === "both") {
    if (!input.monthly_budget_rows || !input.past_sales_rows) {
      throw new Error(
        "via_gas pull requires monthly_budget_rows and past_sales_rows from the spreadsheet.",
      )
    }
    result.pull = await processPullRowsToDb(
      supabase,
      config,
      input.monthly_budget_rows,
      input.past_sales_rows,
      logLines,
    )
  }

  if (direction === "push" || direction === "both") {
    const built = await buildDailySalesExportRows(supabase, config)
    result.push = {
      months_written: built.months_written,
      rows_written: built.rows_written,
      closed_dates_rows_updated: 0,
    }
    sheetExport.daily_sales = { header: built.header, rows: built.rows }
    logLines.push(`push rows=${built.rows_written}`)
  }

  const monthsForClosed = await listMonthsForClosedExport(
    supabase,
    config.storePartitionKey,
    input.monthly_budget_rows,
  )
  const closedExport = await buildClosedDatesExportFromDb(
    supabase,
    config.storePartitionKey,
    monthsForClosed,
  )
  result.closed_dates_export = closedExport
  sheetExport.closed_dates_by_month = closedExport.dates_by_month
  if (input.monthly_budget_rows && input.monthly_budget_rows.length > 0) {
    sheetExport.closed_dates_updates = buildClosedDatesSheetUpdates(
      input.monthly_budget_rows,
      config.storePartitionKey,
      closedExport.dates_by_month,
    )
  }
  result.sheet_export = sheetExport

  result.sync_log_row = [
    result.generated_at,
    direction,
    config.storePartitionKey,
    result.pull
      ? `budgets=${result.pull.budgets_applied} past=${result.pull.past_sales_applied} err=${result.pull.errors.length}`
      : "-",
    result.push ? `rows=${result.push.rows_written} months=${result.push.months_written.join(",")}` : "-",
    logLines.slice(0, 3).join(" | ") || "ok (gas)",
  ]

  return result
}

async function pullFromSheetsToDb(
  supabase: ReturnType<typeof createClient>,
  config: ReceiptSheetsPilotConfig,
  logLines: string[],
): Promise<NonNullable<ReceiptSheetsSyncResult["pull"]>> {
  const { values: budgetRows } = await getSheetValuesForTab(
    config.spreadsheetId,
    TAB_ALIASES_BUDGETS,
    "A2:I500",
  )
  const { values: pastRows } = await getSheetValuesForTab(
    config.spreadsheetId,
    TAB_ALIASES_PAST,
    "A2:D500",
  )
  return processPullRowsToDb(supabase, config, budgetRows, pastRows, logLines)
}

async function processPullRowsToDb(
  supabase: ReturnType<typeof createClient>,
  config: ReceiptSheetsPilotConfig,
  budgetRows: SheetValues,
  pastRows: SheetValues,
  logLines: string[],
): Promise<NonNullable<ReceiptSheetsSyncResult["pull"]>> {
  const errors: string[] = []
  let budgetsApplied = 0
  let budgetsSkipped = 0
  let pastApplied = 0
  let pastSkipped = 0

  for (let i = 0; i < budgetRows.length; i += 1) {
    const row = budgetRows[i]
    const rowNum = i + 2
    const month = normalizeMonthCell(row[0])
    const storeKey = normalizePilotStoreKey(row[2])
    const enabled = parseEnabledCell(row[8])
    if (!enabled) {
      budgetsSkipped += 1
      continue
    }
    if (storeKey !== config.storePartitionKey) {
      budgetsSkipped += 1
      continue
    }
    if (!month) {
      errors.push(`${SHEET_MONTHLY_BUDGETS} 行${rowNum}: 月の形式が不正です`)
      budgetsSkipped += 1
      continue
    }
    const budgetYen = parseNonNegativeInt(row[3])
    if (budgetYen <= 0) {
      errors.push(`${SHEET_MONTHLY_BUDGETS} 行${rowNum}: 予算は正の整数にしてください`)
      budgetsSkipped += 1
      continue
    }
    try {
      const closedCellRaw = String(row[7] ?? "").trim()
      const sheetSpecifiedClosed = closedCellRaw.length > 0
      let storeClosedDates = parseClosedDatesCell(row[7], month)
      if (!sheetSpecifiedClosed) {
        storeClosedDates = await loadStoreClosedDatesForMonth(
          supabase,
          config.storePartitionKey,
          month,
        )
      }
      await upsertBudgetRow(supabase, {
        store_partition_key: config.storePartitionKey,
        month,
        budget_yen: budgetYen,
        weekday_weight: parsePositiveWeight(row[4], 1),
        pre_holiday_weight: parsePositiveWeight(row[5], 1.5),
        holiday_weight: parsePositiveWeight(row[6], 2),
        store_closed_dates: storeClosedDates,
      })
      budgetsApplied += 1
    } catch (e) {
      errors.push(`${SHEET_MONTHLY_BUDGETS} 行${rowNum}: ${String(e)}`)
    }
  }

  const pastEntries: Array<{ sales_month: string; gross_sales_yen: number | null }> = []
  for (let i = 0; i < pastRows.length; i += 1) {
    const row = pastRows[i]
    const rowNum = i + 2
    const salesMonth = normalizeMonthCell(row[0])
    const storeKey = String(row[1] ?? "").trim().toLowerCase()
    const enabled = parseEnabledCell(row[3])
    if (!enabled || storeKey !== config.storePartitionKey) {
      pastSkipped += 1
      continue
    }
    if (!salesMonth) {
      errors.push(`${SHEET_PAST_SALES} 行${rowNum}: 対象月の形式が不正です`)
      pastSkipped += 1
      continue
    }
    const rawGross = String(row[2] ?? "").trim()
    if (rawGross === "") {
      pastEntries.push({ sales_month: salesMonth, gross_sales_yen: null })
      pastApplied += 1
    } else {
      const gross = parseNonNegativeInt(rawGross)
      pastEntries.push({ sales_month: salesMonth, gross_sales_yen: gross })
      pastApplied += 1
    }
  }

  if (pastEntries.length > 0) {
    try {
      await upsertManualMonthEntries(supabase, config.storePartitionKey, pastEntries)
    } catch (e) {
      errors.push(`${SHEET_PAST_SALES}: ${String(e)}`)
    }
  }

  logLines.push(`pull budgets=${budgetsApplied} past=${pastApplied}`)
  return {
    budgets_applied: budgetsApplied,
    budgets_skipped: budgetsSkipped,
    past_sales_applied: pastApplied,
    past_sales_skipped: pastSkipped,
    errors,
  }
}

async function buildDailySalesExportRows(
  supabase: ReturnType<typeof createClient>,
  config: ReceiptSheetsPilotStoreConfig,
): Promise<{
  header: string[]
  rows: string[][]
  months_written: string[]
  rows_written: number
}> {
  const months = listPilotSyncMonthsJst()
  const header = [
    "日付",
    "店舗キー",
    "店舗名",
    "総売上",
    "組数",
    "客数",
    "日別予算",
    "差額",
    "レシート件数",
    "更新日時",
  ]
  const rows: string[][] = []
  const updatedAt = new Date().toISOString()

  for (const month of months) {
    const series = await buildDailySeriesForStoreMonth(supabase, config.storePartitionKey, month)
    const budgetRow = await fetchBudgetRow(supabase, config.storePartitionKey, month)
    let dailyBudgetMap: Map<string, number> | null = null
    if (budgetRow && budgetRow.budget_yen > 0) {
      const weights: SalesBudgetAllocationWeights = {
        weekday: budgetRow.weekday_weight,
        pre_holiday: budgetRow.pre_holiday_weight,
        holiday: budgetRow.holiday_weight,
      }
      dailyBudgetMap = allocateDailyBudgetsForMonth(
        month,
        budgetRow.budget_yen,
        weights,
        getDefaultJapaneseHolidaySet(),
        new Set(budgetRow.store_closed_dates),
      )
    }

    for (const day of series) {
      const budgetYen = dailyBudgetMap?.get(day.date) ?? 0
      const variance = day.gross_sales_yen - budgetYen
      rows.push([
        day.date,
        config.storePartitionKey,
        config.storeDisplayName,
        String(day.gross_sales_yen),
        String(day.party_count),
        String(day.guest_count),
        String(budgetYen),
        String(variance),
        String(day.receipt_count),
        updatedAt,
      ])
    }
  }

  return {
    header,
    rows,
    months_written: months,
    rows_written: rows.length,
  }
}

async function pushDailySalesToSheet(
  supabase: ReturnType<typeof createClient>,
  config: ReceiptSheetsPilotConfig,
  logLines: string[],
): Promise<NonNullable<ReceiptSheetsSyncResult["push"]>> {
  const built = await buildDailySalesExportRows(supabase, config)
  const allRows: string[][] = [built.header, ...built.rows]
  await updateSheetValuesForTab(config.spreadsheetId, TAB_ALIASES_DAILY, "A1", allRows)
  logLines.push(`push rows=${built.rows_written}`)
  return {
    months_written: built.months_written,
    rows_written: built.rows_written,
    closed_dates_rows_updated: 0,
  }
}

function collectPilotMonthsFromBudgetRows(
  budgetRows: SheetValues,
  storePartitionKey: string,
): string[] {
  const months = new Set(listPilotSyncMonthsJst())
  for (const row of budgetRows) {
    const month = normalizeMonthCell(row[0])
    const storeKey = normalizePilotStoreKey(row[2])
    if (month && storeKey === storePartitionKey) {
      months.add(month)
    }
  }
  return [...months].sort()
}

async function listMonthsForClosedExport(
  supabase: ReturnType<typeof createClient>,
  storePartitionKey: string,
  budgetRows?: SheetValues,
): Promise<string[]> {
  const months = new Set(
    collectPilotMonthsFromBudgetRows(budgetRows ?? [], storePartitionKey),
  )
  const { data, error } = await supabase
    .from("line_sales_month_budgets")
    .select("target_month")
    .eq("store_partition_key", storePartitionKey)
  if (error) {
    throw new Error(`Failed to list budget months: ${error.message}`)
  }
  for (const row of Array.isArray(data) ? data : []) {
    const month = normalizeMonthCell((row as Record<string, unknown>).target_month)
    if (month) months.add(month)
  }

  const { data: closedRows, error: closedErr } = await supabase
    .from("line_sales_month_store_closed_days")
    .select("target_month")
    .eq("store_partition_key", storePartitionKey)
  if (closedErr) {
    throw new Error(`Failed to list closed-day months: ${closedErr.message}`)
  }
  for (const row of Array.isArray(closedRows) ? closedRows : []) {
    const month = normalizeMonthCell((row as Record<string, unknown>).target_month)
    if (month) months.add(month)
  }

  return [...months].sort()
}

function buildClosedDatesSheetUpdates(
  budgetRows: SheetValues,
  storePartitionKey: string,
  datesByMonth: Record<string, string[]>,
): ReceiptSheetsGasClosedDateUpdate[] {
  const updates: ReceiptSheetsGasClosedDateUpdate[] = []
  for (let i = 0; i < budgetRows.length; i += 1) {
    const month = normalizeMonthCell(budgetRows[i][0])
    const storeKey = normalizePilotStoreKey(budgetRows[i][2])
    if (!month || storeKey !== storePartitionKey) continue
    if (!(month in datesByMonth)) continue
    updates.push({
      row: i + 2,
      month,
      value: formatClosedDatesForSheetCell(datesByMonth[month] ?? [], month),
    })
  }
  return updates
}

async function buildClosedDatesExportFromDb(
  supabase: ReturnType<typeof createClient>,
  storePartitionKey: string,
  months: string[],
): Promise<NonNullable<ReceiptSheetsSyncResult["closed_dates_export"]>> {
  const datesByMonth: Record<string, string[]> = {}
  for (const month of months) {
    const closed = await loadStoreClosedDatesForMonth(supabase, storePartitionKey, month)
    datesByMonth[month] = closed
  }
  return {
    rows_updated: months.length,
    dates_by_month: datesByMonth,
    pilot_store_key: storePartitionKey,
  }
}

/** DB の休業日を「月間予算」シートの休業日列（H）へ書き戻す */
async function exportClosedDatesFromDbToBudgetSheet(
  supabase: ReturnType<typeof createClient>,
  config: ReceiptSheetsPilotConfig,
): Promise<NonNullable<ReceiptSheetsSyncResult["closed_dates_export"]>> {
  const datesByMonth: Record<string, string[]> = {}
  const batch: Array<{ range: string; values: SheetValues }> = []

  const { values: budgetRows, tabName } = await getSheetValuesForTab(
    config.spreadsheetId,
    TAB_ALIASES_BUDGETS,
    "A2:I500",
  )

  let rowsUpdated = 0
  for (let i = 0; i < budgetRows.length; i += 1) {
    const row = budgetRows[i]
    const month = normalizeMonthCell(row[0])
    const storeKey = normalizePilotStoreKey(row[2])
    if (!month || storeKey !== config.storePartitionKey) continue

    const closed = await loadStoreClosedDatesForMonth(
      supabase,
      config.storePartitionKey,
      month,
    )
    datesByMonth[month] = closed
    const rowNum = i + 2
    batch.push({
      range: formatSheetA1Range(tabName, `H${rowNum}`),
      values: [[formatClosedDatesForSheetCell(closed, month)]],
    })
    rowsUpdated += 1
  }

  if (batch.length > 0) {
    await batchUpdateSpreadsheetValues(config.spreadsheetId, batch)
  }

  return {
    rows_updated: rowsUpdated,
    dates_by_month: datesByMonth,
    pilot_store_key: config.storePartitionKey,
  }
}

function normalizePilotStoreKey(raw: unknown): string {
  return String(raw ?? "").trim().toLowerCase()
}

/** シート表示用: 同月内は M/D、連休は 5/3〜5/6、長いときは改行 */
function formatClosedDatesForSheetCell(dates: string[], month?: string): string {
  if (dates.length === 0) return ""
  const sorted = [...dates].sort()
  const targetMonth = month ?? sorted[0]?.slice(0, 7) ?? ""
  if (!/^\d{4}-\d{2}$/.test(targetMonth)) {
    return sorted.join("、")
  }
  const monthNum = Number(targetMonth.slice(5, 7))
  const dayNums: number[] = []
  for (const iso of sorted) {
    if (!iso.startsWith(`${targetMonth}-`)) continue
    const day = Number(iso.slice(8, 10))
    if (day >= 1 && day <= 31) dayNums.push(day)
  }
  if (dayNums.length === 0) return sorted.join("、")

  const segments = compressClosedDaysToSegments(dayNums, monthNum)
  if (segments.length <= 3 && segments.join("、").length <= 28) {
    return segments.join("、")
  }
  const lines: string[] = []
  for (let i = 0; i < segments.length; i += 2) {
    lines.push(segments.slice(i, i + 2).join("、"))
  }
  return lines.join("\n")
}

function compressClosedDaysToSegments(days: number[], monthNum: number): string[] {
  const unique = [...new Set(days)].sort((a, b) => a - b)
  const segments: string[] = []
  let start = unique[0]
  let end = unique[0]
  const fmt = (day: number) => `${monthNum}/${day}`
  for (let i = 1; i <= unique.length; i += 1) {
    const d = unique[i]
    if (i < unique.length && d === end + 1) {
      end = d
      continue
    }
    segments.push(start === end ? fmt(start) : `${fmt(start)}〜${fmt(end)}`)
    if (i < unique.length) {
      start = d
      end = d
    }
  }
  return segments
}

async function appendSyncLog(spreadsheetId: string, row: string[]): Promise<void> {
  let hasHeader = false
  for (const tab of TAB_ALIASES_LOG) {
    try {
      const existing = await getSpreadsheetValues(spreadsheetId, formatSheetA1Range(tab, "A1:A1"))
      hasHeader = existing.length > 0
      break
    } catch (e) {
      if (!isSheetNotFoundError(e)) throw e
    }
  }
  if (!hasHeader) {
    await updateSheetValuesForTab(spreadsheetId, TAB_ALIASES_LOG, "A1", [
      ["同期日時", "方向", "店舗キー", "取込結果", "書出結果", "メモ"],
    ])
  }
  await appendSheetValuesForTab(spreadsheetId, TAB_ALIASES_LOG, "A:F", [row])
}

type DailySeriesRow = {
  date: string
  gross_sales_yen: number
  party_count: number
  guest_count: number
  receipt_count: number
}

async function buildDailySeriesForStoreMonth(
  supabase: ReturnType<typeof createClient>,
  storePartitionKey: string,
  month: string,
): Promise<DailySeriesRow[]> {
  const range = buildJstMonthRange(month)
  const dayKeys = buildJstDateKeysForMonth(month)
  const dayKeySet = new Set(dayKeys)

  const { data, error } = await supabase
    .from("line_receipt_entries")
    .select(
      "receipt_date, gross_sales_yen, party_count, guest_count",
    )
    .eq("store_partition_key", storePartitionKey)
    .gte("created_at", range.startIso)
    .lt("created_at", range.endIso)
    .order("created_at", { ascending: true })
    .limit(20000)

  if (error) {
    throw new Error(`Failed to fetch receipt entries: ${error.message}`)
  }

  const dailyMap = new Map<string, DailySeriesRow>()
  for (const row of Array.isArray(data) ? data : []) {
    const r = row as Record<string, unknown>
    const dayKey = resolveReceiptEntryDateKeyForMonth(r.receipt_date, month)
    if (!dayKey || !dayKeySet.has(dayKey)) continue
    const gross = parseNonNegativeInt(r.gross_sales_yen)
    const party = parseNonNegativeInt(r.party_count)
    const guest = parseNonNegativeInt(r.guest_count)
    const existing = dailyMap.get(dayKey)
    if (!existing) {
      dailyMap.set(dayKey, {
        date: dayKey,
        gross_sales_yen: gross,
        party_count: party,
        guest_count: guest,
        receipt_count: 1,
      })
    } else {
      existing.gross_sales_yen += gross
      existing.party_count += party
      existing.guest_count += guest
      existing.receipt_count += 1
    }
  }

  return dayKeys.map((date) => dailyMap.get(date) ?? {
    date,
    gross_sales_yen: 0,
    party_count: 0,
    guest_count: 0,
    receipt_count: 0,
  })
}

type BudgetRow = {
  budget_yen: number
  weekday_weight: number
  pre_holiday_weight: number
  holiday_weight: number
  store_closed_dates: string[]
}

async function fetchBudgetRow(
  supabase: ReturnType<typeof createClient>,
  storePartitionKey: string,
  month: string,
): Promise<BudgetRow | null> {
  const { data, error } = await supabase
    .from("line_sales_month_budgets")
    .select("budget_yen, weekday_weight, pre_holiday_weight, holiday_weight, store_closed_dates")
    .eq("store_partition_key", storePartitionKey)
    .eq("target_month", month)
    .maybeSingle()

  if (error) {
    throw new Error(`Failed to fetch budget: ${error.message}`)
  }
  if (!data) return null
  const row = data as Record<string, unknown>
  const budgetYen = parseNonNegativeInt(row.budget_yen)
  if (budgetYen <= 0) return null

  const { data: closedRows } = await supabase
    .from("line_sales_month_store_closed_days")
    .select("closed_on")
    .eq("store_partition_key", storePartitionKey)
    .eq("target_month", month)

  const closedMerged = await loadStoreClosedDatesForMonth(
    supabase,
    storePartitionKey,
    month,
    row.store_closed_dates,
  )

  return {
    budget_yen: budgetYen,
    weekday_weight: parsePositiveWeight(row.weekday_weight, 1),
    pre_holiday_weight: parsePositiveWeight(row.pre_holiday_weight, 1.5),
    holiday_weight: parsePositiveWeight(row.holiday_weight, 2),
    store_closed_dates: closedMerged,
  }
}

/** 休業日テーブル＋予算 jsonb を統合（予算行が無くてもテーブルだけ読む） */
async function loadStoreClosedDatesForMonth(
  supabase: ReturnType<typeof createClient>,
  storePartitionKey: string,
  month: string,
  budgetJsonbRaw?: unknown,
): Promise<string[]> {
  const { data: closedRows, error: closedErr } = await supabase
    .from("line_sales_month_store_closed_days")
    .select("closed_on")
    .eq("store_partition_key", storePartitionKey)
    .eq("target_month", month)

  if (closedErr) {
    throw new Error(`Failed to fetch store closed days: ${closedErr.message}`)
  }

  const closedFromTable: string[] = []
  for (const cr of Array.isArray(closedRows) ? closedRows : []) {
    const iso = closedOnToMonthDateIso((cr as Record<string, unknown>).closed_on, month)
    if (iso) closedFromTable.push(iso)
  }

  let jsonbRaw = budgetJsonbRaw
  if (jsonbRaw === undefined) {
    const { data: budgetData } = await supabase
      .from("line_sales_month_budgets")
      .select("store_closed_dates")
      .eq("store_partition_key", storePartitionKey)
      .eq("target_month", month)
      .maybeSingle()
    jsonbRaw = (budgetData as Record<string, unknown> | null)?.store_closed_dates
  }

  return [...new Set([
    ...closedFromTable,
    ...parseStoreClosedDatesForMonth(jsonbRaw, month),
  ])].sort()
}

function closedOnToMonthDateIso(value: unknown, month: string): string | null {
  if (value == null) return null
  const s = String(value).trim()
  const matched = s.match(/^(\d{4}-\d{2}-\d{2})/)
  if (!matched) return null
  const iso = matched[1]
  if (!iso.startsWith(`${month}-`)) return null
  return iso
}

async function upsertBudgetRow(
  supabase: ReturnType<typeof createClient>,
  input: {
    store_partition_key: string
    month: string
    budget_yen: number
    weekday_weight: number
    pre_holiday_weight: number
    holiday_weight: number
    store_closed_dates: string[]
  },
): Promise<void> {
  const updatedAt = new Date().toISOString()
  const { error } = await supabase
    .from("line_sales_month_budgets")
    .upsert(
      {
        store_partition_key: input.store_partition_key,
        target_month: input.month,
        budget_yen: input.budget_yen,
        weekday_weight: input.weekday_weight,
        pre_holiday_weight: input.pre_holiday_weight,
        holiday_weight: input.holiday_weight,
        store_closed_dates: input.store_closed_dates,
        updated_at: updatedAt,
      },
      { onConflict: "store_partition_key,target_month" },
    )
  if (error) {
    throw new Error(error.message)
  }

  const { error: delErr } = await supabase
    .from("line_sales_month_store_closed_days")
    .delete()
    .eq("store_partition_key", input.store_partition_key)
    .eq("target_month", input.month)
  if (delErr) {
    throw new Error(delErr.message)
  }
  if (input.store_closed_dates.length > 0) {
    const rows = input.store_closed_dates.map((closed_on) => ({
      store_partition_key: input.store_partition_key,
      target_month: input.month,
      closed_on,
    }))
    const { error: insErr } = await supabase.from("line_sales_month_store_closed_days").insert(rows)
    if (insErr) {
      throw new Error(insErr.message)
    }
  }
}

async function upsertManualMonthEntries(
  supabase: ReturnType<typeof createClient>,
  storePartitionKey: string,
  entries: Array<{ sales_month: string; gross_sales_yen: number | null }>,
): Promise<void> {
  const updatedAt = new Date().toISOString()
  for (const entry of entries) {
    if (entry.gross_sales_yen === null) {
      const { error } = await supabase
        .from("line_sales_manual_month_gross")
        .delete()
        .eq("store_partition_key", storePartitionKey)
        .eq("sales_month", entry.sales_month)
      if (error) throw new Error(error.message)
    } else {
      const { error } = await supabase
        .from("line_sales_manual_month_gross")
        .upsert(
          {
            store_partition_key: storePartitionKey,
            sales_month: entry.sales_month,
            gross_sales_yen: entry.gross_sales_yen,
            updated_at: updatedAt,
          },
          { onConflict: "store_partition_key,sales_month" },
        )
      if (error) throw new Error(error.message)
    }
  }
}

function listPilotSyncMonthsJst(): string[] {
  const now = new Date()
  const parts = new Intl.DateTimeFormat("ja-JP", {
    timeZone: "Asia/Tokyo",
    year: "numeric",
    month: "2-digit",
  }).formatToParts(now)
  const y = Number(parts.find((p) => p.type === "year")?.value ?? now.getUTCFullYear())
  const m = Number(parts.find((p) => p.type === "month")?.value ?? 1)
  const current = `${String(y).padStart(4, "0")}-${String(m).padStart(2, "0")}`
  const prevTotal = y * 12 + (m - 1) - 1
  const py = Math.floor(prevTotal / 12)
  const pm = (prevTotal % 12) + 1
  const previous = `${String(py).padStart(4, "0")}-${String(pm).padStart(2, "0")}`
  return [previous, current]
}

function normalizeMonthCell(raw: unknown): string | null {
  if (raw instanceof Date && !Number.isNaN(raw.getTime())) {
    return formatYearMonthJst(raw)
  }
  if (typeof raw === "number" && Number.isFinite(raw) && raw > 20000 && raw < 120000) {
    const ms = Math.round((raw - 25569) * 86400 * 1000)
    const d = new Date(ms)
    if (!Number.isNaN(d.getTime())) return formatYearMonthJst(d)
  }
  const s = String(raw ?? "").trim()
  if (/^\d{4}-(0[1-9]|1[0-2])$/.test(s)) return s
  const isoDay = /^(\d{4})-(0[1-9]|1[0-2])-\d{2}/.exec(s)
  if (isoDay) {
    return `${isoDay[1]}-${isoDay[2]}`
  }
  const loose = /^(\d{4})[/-](\d{1,2})$/.exec(s)
  if (loose) {
    const y = Number(loose[1])
    const mo = Math.min(12, Math.max(1, Number(loose[2])))
    return `${String(y).padStart(4, "0")}-${String(mo).padStart(2, "0")}`
  }
  return null
}

function formatYearMonthJst(d: Date): string {
  const parts = new Intl.DateTimeFormat("ja-JP", {
    timeZone: "Asia/Tokyo",
    year: "numeric",
    month: "2-digit",
  }).formatToParts(d)
  const y = parts.find((p) => p.type === "year")?.value ?? "1970"
  const m = parts.find((p) => p.type === "month")?.value ?? "01"
  return `${y}-${m}`
}

function parseEnabledCell(raw: unknown): boolean {
  const s = String(raw ?? "").trim().toLowerCase()
  if (!s || s === "false" || s === "0" || s === "no" || s === "いいえ") return false
  return true
}

function parseClosedDatesCell(raw: unknown, month: string): string[] {
  const s = String(raw ?? "").trim()
  if (!s) return []
  const allowed = new Set(buildJstDateKeysForMonth(month))
  const monthNum = Number(month.slice(5, 7))
  const tokens = s
    .split(/[\n\r]+/)
    .flatMap((line) => line.split(/[,、]+/))
    .map((p) => p.trim())
    .filter(Boolean)
  const out: string[] = []
  for (const token of tokens) {
    out.push(...expandClosedDateToken(token, month, monthNum, allowed))
  }
  return [...new Set(out)].filter((d) => allowed.has(d)).sort()
}

function expandClosedDateToken(
  token: string,
  month: string,
  monthNum: number,
  allowed: Set<string>,
): string[] {
  const rangeFull = /^(\d{1,2})\/(\d{1,2})[〜~～\-－](\d{1,2})\/(\d{1,2})$/.exec(token)
  if (rangeFull) {
    const m1 = Number(rangeFull[1])
    const d1 = Number(rangeFull[2])
    const d2 = Number(rangeFull[4])
    if (m1 === monthNum) return daysInRange(month, d1, d2, allowed)
    return []
  }
  const rangeShort = /^(\d{1,2})\/(\d{1,2})[〜~～\-－](\d{1,2})$/.exec(token)
  if (rangeShort) {
    const m1 = Number(rangeShort[1])
    const d1 = Number(rangeShort[2])
    const d2 = Number(rangeShort[3])
    if (m1 === monthNum) return daysInRange(month, d1, d2, allowed)
    return []
  }
  const dayOnlyRange = /^(\d{1,2})[〜~～\-－](\d{1,2})$/.exec(token)
  if (dayOnlyRange) {
    return daysInRange(month, Number(dayOnlyRange[1]), Number(dayOnlyRange[2]), allowed)
  }

  let key = token
  if (/^\d{1,2}\/\d{1,2}$/.test(token)) {
    const [mRaw, dRaw] = token.split("/")
    const m = Number(mRaw)
    const d = Number(dRaw)
    if (m === monthNum) {
      key = `${month}-${String(d).padStart(2, "0")}`
    } else {
      key = `${month}-${String(m).padStart(2, "0")}-${String(d).padStart(2, "0")}`
    }
  } else if (/^\d{1,2}-\d{1,2}$/.test(token)) {
    const dayOnly = token.split("-")[1]
    key = `${month}-${String(dayOnly).padStart(2, "0")}`
  }
  if (/^\d{4}-\d{2}-\d{2}$/.test(key) && allowed.has(key)) {
    return [key]
  }
  return []
}

function daysInRange(
  month: string,
  startDay: number,
  endDay: number,
  allowed: Set<string>,
): string[] {
  const from = Math.min(startDay, endDay)
  const to = Math.max(startDay, endDay)
  const out: string[] = []
  for (let d = from; d <= to; d += 1) {
    const iso = `${month}-${String(d).padStart(2, "0")}`
    if (allowed.has(iso)) out.push(iso)
  }
  return out
}

function parseNonNegativeInt(raw: unknown): number {
  const n = Number(String(raw ?? "").replace(/,/g, "").trim())
  if (!Number.isFinite(n) || n < 0) return 0
  return Math.round(n)
}

function parsePositiveWeight(raw: unknown, fallback: number): number {
  const n = Number(String(raw ?? "").replace(/,/g, "").trim())
  if (!Number.isFinite(n) || n <= 0) return fallback
  return n
}

function buildJstMonthRange(month: string): { startIso: string; endIso: string } {
  const matched = month.match(/^(\d{4})-(\d{2})$/)
  if (!matched) {
    const fallbackStart = new Date()
    const fallbackEnd = new Date(fallbackStart.getTime() + 31 * 24 * 60 * 60 * 1000)
    return { startIso: fallbackStart.toISOString(), endIso: fallbackEnd.toISOString() }
  }
  const year = Number(matched[1])
  const monthNumber = Number(matched[2])
  const startUtc = Date.UTC(year, monthNumber - 1, 1, -9, 0, 0)
  const endUtc = Date.UTC(year, monthNumber, 1, -9, 0, 0)
  return { startIso: new Date(startUtc).toISOString(), endIso: new Date(endUtc).toISOString() }
}

function buildJstDateKeysForMonth(month: string): string[] {
  const matched = month.match(/^(\d{4})-(\d{2})$/)
  if (!matched) return []
  const year = Number(matched[1])
  const monthNum = Number(matched[2])
  const lastDay = new Date(Date.UTC(year, monthNum, 0)).getUTCDate()
  const keys: string[] = []
  for (let day = 1; day <= lastDay; day += 1) {
    keys.push(`${String(year).padStart(4, "0")}-${String(monthNum).padStart(2, "0")}-${String(day).padStart(2, "0")}`)
  }
  return keys
}

function resolveReceiptEntryDateKeyForMonth(receiptDateValue: unknown, month: string): string | null {
  const receiptDate = String(receiptDateValue ?? "").trim()
  if (/^\d{4}-(0[1-9]|1[0-2])-\d{2}$/.test(receiptDate) && receiptDate.startsWith(`${month}-`)) {
    return receiptDate
  }
  return null
}
