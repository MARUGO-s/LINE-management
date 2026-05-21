import "jsr:@supabase/functions-js/edge-runtime.d.ts"
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.44.0"
import { loadReceiptReportAggregateForRoom } from "../_shared/receipt_report_aggregate.ts"
import { buildReceiptReportFlexMessages } from "../_shared/receipt_report_flex.ts"

type ReceiptReportKind = "mid_month" | "month_end"
type ReceiptReportTriggerType = "day15_fallback" | "month_end_fallback"

type ReceiptReportTestParse = {
  roomId: string
  reportKind: ReceiptReportKind
  year: number
  month: number
  storePartitionKey: string | null
  keyFromQuery: string
  keyFromHeader: string
}

type ReceiptReportSchedule = {
  reportKind: ReceiptReportKind
  reportTitle: string
  triggerType: ReceiptReportTriggerType
  reportMonth: string
  periodStartDate: string
  periodEndDate: string
  rangeStartIso: string
  rangeEndIso: string
}

const JST_OFFSET_MS = 9 * 60 * 60 * 1000
const RECEIPT_MID_REPORT_TITLE = "中間報告"
const RECEIPT_MONTH_END_REPORT_TITLE = "月間報告"
/** 集計締めは営業日5時切替後（16日／翌月1日）だが、LINE送信は店舗向けに10時 */
const REPORT_RUN_HOUR_JST = 10
const REPORT_RUN_MINUTE_JST = 0

Deno.serve(async (req) => {
  const supabaseUrl = Deno.env.get("SUPABASE_URL") ?? ""
  const serviceRoleKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") ?? ""
  const lineAccessToken = Deno.env.get("LINE_CHANNEL_ACCESS_TOKEN") ?? ""

  if (!supabaseUrl || !serviceRoleKey) {
    return json({
      ok: false,
      error: "SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY is missing.",
    }, 500)
  }

  const testEarly = parseReceiptReportTestRequest(req)
  if (testEarly) {
    return await handleReceiptReportTestSend(testEarly, {
      supabaseUrl,
      serviceRoleKey,
      lineAccessToken,
    })
  }

  if (!lineAccessToken) {
    return json({
      ok: true,
      skipped: true,
      reason: "missing_line_channel_access_token",
    }, 200)
  }

  const now = new Date()
  const jst = toJstDateParts(now)
  const schedule = resolveReceiptReportSchedule(jst)
  if (!schedule) {
    return json({
      ok: true,
      skipped: true,
      reason: "not_receipt_report_time",
      now_jst:
        `${toJstDateString(jst.year, jst.month, jst.day)} ${String(jst.hour).padStart(2, "0")}:${String(jst.minute).padStart(2, "0")}`,
    }, 200)
  }

  const supabase = createClient(supabaseUrl, serviceRoleKey)

  const settingColumn = schedule.reportKind === "mid_month"
    ? "receipt_midreport_enabled"
    : "receipt_monthend_report_enabled"
  const { data: roomSettings, error: settingsError } = await supabase
    .from("room_summary_settings")
    .select(`room_id,${settingColumn}`)

  if (settingsError) {
    return json({
      ok: false,
      error: `Failed to load room_summary_settings: ${settingsError.message}`,
    }, 500)
  }

  const targetRoomIds = (Array.isArray(roomSettings) ? roomSettings : [])
    .filter((row) => (row as Record<string, unknown>)[settingColumn] !== false)
    .map((row) => String((row as Record<string, unknown>).room_id ?? "").trim())
    .filter((roomId) => roomId.length > 0)

  if (targetRoomIds.length === 0) {
    return json({
      ok: true,
      skipped: true,
      reason: "no_enabled_rooms_for_report",
      report_kind: schedule.reportKind,
      report_month: schedule.reportMonth,
    }, 200)
  }

  const { data: existingRows, error: existingError } = await supabase
    .from("line_receipt_mid_reports")
    .select("room_id")
    .eq("report_month", schedule.reportMonth)
    .eq("report_kind", schedule.reportKind)
    .in("room_id", targetRoomIds)

  if (existingError) {
    return json({
      ok: false,
      error: `Failed to load existing line_receipt_mid_reports: ${existingError.message}`,
    }, 500)
  }

  const existingSet = new Set(
    (Array.isArray(existingRows) ? existingRows : [])
      .map((row) => String((row as Record<string, unknown>)?.room_id ?? "").trim())
      .filter((roomId) => roomId.length > 0),
  )

  const sentRoomIds: string[] = []
  const skippedRoomIds: string[] = []
  const errors: string[] = []

  for (const roomId of targetRoomIds) {
    if (existingSet.has(roomId)) {
      skippedRoomIds.push(roomId)
      continue
    }

    const { aggregate, storePartitionKey } = await loadReceiptReportAggregateForRoom(
      supabase,
      roomId,
      schedule.periodStartDate,
      schedule.periodEndDate,
    )
    if (!aggregate || aggregate.receiptCount === 0) {
      skippedRoomIds.push(roomId)
      continue
    }

    const reportMessages = await buildReceiptReportFlexMessages(supabase, aggregate, {
      reportTitle: schedule.reportTitle,
      periodStartDate: schedule.periodStartDate,
      periodEndDate: schedule.periodEndDate,
      storePartitionKey,
      reportKind: schedule.reportKind,
    })
    const sendResult = await sendLinePushMessages(roomId, reportMessages, lineAccessToken)
    if (!sendResult.ok) {
      errors.push(`${roomId}: ${sendResult.error}`)
      continue
    }

    const { error: insertError } = await supabase
      .from("line_receipt_mid_reports")
      .insert({
        report_month: schedule.reportMonth,
        report_kind: schedule.reportKind,
        room_id: roomId,
        period_start_jst: schedule.periodStartDate,
        period_end_jst: schedule.periodEndDate,
        trigger_type: schedule.triggerType,
        trigger_line_message_id: null,
        receipt_count: aggregate.receiptCount,
        total_gross_sales_yen: aggregate.totalGrossSalesYen,
        total_party_count: aggregate.totalPartyCount,
        total_guest_count: aggregate.totalGuestCount,
        avg_gross_sales_yen: aggregate.avgDailyGrossSalesYen == null
          ? (aggregate.avgGrossSalesYen == null ? null : Math.round(aggregate.avgGrossSalesYen))
          : aggregate.avgDailyGrossSalesYen,
        avg_party_count: aggregate.avgPartyCount,
        avg_guest_count: aggregate.avgGuestCount,
        sent_at: now.toISOString(),
      })

    if (insertError) {
      const code = String((insertError as Record<string, unknown>)?.code ?? "")
      if (code === "23505") {
        skippedRoomIds.push(roomId)
      } else {
        errors.push(`${roomId}: failed to insert report log (${insertError.message})`)
      }
      continue
    }

    sentRoomIds.push(roomId)
  }

  return json({
    ok: true,
    report_kind: schedule.reportKind,
    report_title: schedule.reportTitle,
    report_month: schedule.reportMonth,
    period: { start: schedule.periodStartDate, end: schedule.periodEndDate },
    source_room_count: targetRoomIds.length,
    sent_room_count: sentRoomIds.length,
    skipped_room_count: skippedRoomIds.length,
    error_count: errors.length,
    sent_room_ids: sentRoomIds,
    skipped_room_ids: skippedRoomIds,
    errors,
  }, 200)
})

/** One-off test push (no DB log). Guard: Edge secret RECEIPT_MIDREPORT_CRON_TEST_KEY via query `key` or header `X-Receipt-Midreport-Test-Key`. */
function parseReceiptReportTestRequest(req: Request): ReceiptReportTestParse | null {
  const url = new URL(req.url)
  const flag = (url.searchParams.get("test_receipt_report") ?? url.searchParams.get("test_receipt_midreport") ?? "")
    .trim()
    .toLowerCase()
  if (flag !== "1" && flag !== "true" && flag !== "yes" && flag !== "on") {
    return null
  }
  const roomId = (url.searchParams.get("room_id") ?? "").trim()
  if (!roomId) return null

  const kindRaw = (url.searchParams.get("report_kind") ?? "mid_month").trim().toLowerCase()
  const reportKind: ReceiptReportKind = kindRaw === "month_end" ? "month_end" : "mid_month"

  const now = new Date()
  const jst = toJstDateParts(now)
  let year = Number(url.searchParams.get("year"))
  let month = Number(url.searchParams.get("month"))
  if (!Number.isInteger(year) || year < 2000 || year > 2100) year = jst.year
  if (!Number.isInteger(month) || month < 1 || month > 12) month = jst.month

  const keyFromQuery = (url.searchParams.get("key") ?? "").trim()
  const keyFromHeader = (req.headers.get("x-receipt-midreport-test-key") ?? "").trim()
  const storeKeyRaw = (url.searchParams.get("store_partition_key") ?? "").trim().toLowerCase()
  const storePartitionKey = /^[a-z0-9]{2,120}$/.test(storeKeyRaw) ? storeKeyRaw : null

  return {
    roomId,
    reportKind,
    year,
    month,
    storePartitionKey,
    keyFromQuery,
    keyFromHeader,
  }
}

type ReceiptReportTestDeps = {
  supabaseUrl: string
  serviceRoleKey: string
  lineAccessToken: string
}

async function handleReceiptReportTestSend(
  spec: ReceiptReportTestParse,
  deps: ReceiptReportTestDeps,
): Promise<Response> {
  const testKey = (Deno.env.get("RECEIPT_MIDREPORT_CRON_TEST_KEY") ?? "").trim()
  if (!testKey) {
    return json({
      ok: false,
      error: "Test send is disabled. Set Edge secret RECEIPT_MIDREPORT_CRON_TEST_KEY.",
    }, 503)
  }
  const provided = spec.keyFromHeader || spec.keyFromQuery
  if (!provided || provided !== testKey) {
    return json({ ok: false, error: "Forbidden" }, 403)
  }
  if (!deps.lineAccessToken) {
    return json({ ok: false, error: "LINE_CHANNEL_ACCESS_TOKEN is missing." }, 500)
  }

  const slice = buildReceiptReportTestSchedule(spec.reportKind, spec.year, spec.month)
  const supabase = createClient(deps.supabaseUrl, deps.serviceRoleKey)

  const { aggregate, storePartitionKey } = await loadReceiptReportAggregateForRoom(
    supabase,
    spec.roomId,
    slice.periodStartDate,
    slice.periodEndDate,
    spec.storePartitionKey,
  )

  if (!storePartitionKey) {
    return json({
      ok: true,
      skipped: true,
      mode: "test_receipt_report",
      reason: "store_not_resolved_for_room",
      report_kind: slice.reportKind,
      period: { start: slice.periodStartDate, end: slice.periodEndDate },
      room_id: spec.roomId,
    }, 200)
  }

  if (!aggregate || aggregate.receiptCount === 0) {
    return json({
      ok: true,
      skipped: true,
      mode: "test_receipt_report",
      reason: "no_receipt_entries_in_period_for_store",
      report_kind: slice.reportKind,
      report_month: slice.reportMonth,
      store_partition_key: storePartitionKey,
      period: { start: slice.periodStartDate, end: slice.periodEndDate },
      room_id: spec.roomId,
    }, 200)
  }

  const reportMessages = await buildReceiptReportFlexMessages(supabase, aggregate, {
    reportTitle: slice.reportTitle,
    periodStartDate: slice.periodStartDate,
    periodEndDate: slice.periodEndDate,
    storePartitionKey,
    reportKind: slice.reportKind,
  })
  const sendResult = await sendLinePushMessages(spec.roomId, reportMessages, deps.lineAccessToken)
  if (!sendResult.ok) {
    return json({
      ok: false,
      error: sendResult.error,
      mode: "test_receipt_report",
    }, 502)
  }

  return json({
    ok: true,
    mode: "test_receipt_report",
    note: "Preview send only. line_receipt_mid_reports was NOT updated.",
    report_kind: slice.reportKind,
    report_title: slice.reportTitle,
    report_month: slice.reportMonth,
    store_partition_key: storePartitionKey,
    period: { start: slice.periodStartDate, end: slice.periodEndDate },
    room_id: spec.roomId,
    receipt_count: aggregate.receiptCount,
    total_gross_sales_yen: aggregate.totalGrossSalesYen,
  }, 200)
}

function buildReceiptReportTestSchedule(
  reportKind: ReceiptReportKind,
  year: number,
  month: number,
): {
  reportKind: ReceiptReportKind
  reportTitle: string
  reportMonth: string
  periodStartDate: string
  periodEndDate: string
  rangeStartIso: string
  rangeEndIso: string
} {
  const reportMonth = toJstDateString(year, month, 1)
  const rangeStartIso = buildJstDateStartUtcIso(year, month, 1)
  if (reportKind === "mid_month") {
    return {
      reportKind: "mid_month",
      reportTitle: RECEIPT_MID_REPORT_TITLE,
      reportMonth,
      periodStartDate: reportMonth,
      periodEndDate: toJstDateString(year, month, 15),
      rangeStartIso,
      rangeEndIso: buildJstDateStartUtcIso(year, month, 16),
    }
  }
  const monthLastDay = getJstMonthLastDay(year, month)
  const nextMonth = shiftJstYearMonth(year, month, 1)
  return {
    reportKind: "month_end",
    reportTitle: RECEIPT_MONTH_END_REPORT_TITLE,
    reportMonth,
    periodStartDate: reportMonth,
    periodEndDate: toJstDateString(year, month, monthLastDay),
    rangeStartIso,
    rangeEndIso: buildJstDateStartUtcIso(nextMonth.year, nextMonth.month, 1),
  }
}

function json(payload: unknown, status = 200): Response {
  return new Response(JSON.stringify(payload), {
    status,
    headers: {
      "Content-Type": "application/json",
      "Cache-Control": "no-store",
    },
  })
}

function toJstDateParts(base = new Date()): { year: number; month: number; day: number; hour: number; minute: number } {
  const jst = new Date(base.getTime() + JST_OFFSET_MS)
  return {
    year: jst.getUTCFullYear(),
    month: jst.getUTCMonth() + 1,
    day: jst.getUTCDate(),
    hour: jst.getUTCHours(),
    minute: jst.getUTCMinutes(),
  }
}

function toJstDateString(year: number, month: number, day: number): string {
  return `${String(year).padStart(4, "0")}-${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")}`
}

function buildJstDateStartUtcIso(year: number, month: number, day: number): string {
  return new Date(Date.UTC(year, month - 1, day, -9, 0, 0, 0)).toISOString()
}

function shiftJstYearMonth(year: number, month: number, deltaMonths: number): { year: number; month: number } {
  const shifted = new Date(Date.UTC(year, month - 1 + deltaMonths, 1))
  return {
    year: shifted.getUTCFullYear(),
    month: shifted.getUTCMonth() + 1,
  }
}

function getJstMonthLastDay(year: number, month: number): number {
  return new Date(Date.UTC(year, month, 0)).getUTCDate()
}

function resolveReceiptReportSchedule(
  jst: { year: number; month: number; day: number; hour: number; minute: number },
): ReceiptReportSchedule | null {
  if (jst.hour !== REPORT_RUN_HOUR_JST || jst.minute !== REPORT_RUN_MINUTE_JST) {
    return null
  }

  // 中間: 毎月16日 10:00 JST（集計は当月1〜15日。15日深夜分は5時切替後に締め済み）
  if (jst.day === 16) {
    const reportMonth = toJstDateString(jst.year, jst.month, 1)
    return {
      reportKind: "mid_month",
      reportTitle: RECEIPT_MID_REPORT_TITLE,
      triggerType: "day15_fallback",
      reportMonth,
      periodStartDate: reportMonth,
      periodEndDate: toJstDateString(jst.year, jst.month, 15),
      rangeStartIso: buildJstDateStartUtcIso(jst.year, jst.month, 1),
      rangeEndIso: buildJstDateStartUtcIso(jst.year, jst.month, 16),
    }
  }

  // 月末: 翌月1日 10:00 JST（前月分。末日深夜分は5時切替後に締め済み）
  if (jst.day === 1) {
    const prev = shiftJstYearMonth(jst.year, jst.month, -1)
    const reportMonth = toJstDateString(prev.year, prev.month, 1)
    const monthLastDay = getJstMonthLastDay(prev.year, prev.month)
    const nextMonth = shiftJstYearMonth(prev.year, prev.month, 1)
    return {
      reportKind: "month_end",
      reportTitle: RECEIPT_MONTH_END_REPORT_TITLE,
      triggerType: "month_end_fallback",
      reportMonth,
      periodStartDate: reportMonth,
      periodEndDate: toJstDateString(prev.year, prev.month, monthLastDay),
      rangeStartIso: buildJstDateStartUtcIso(prev.year, prev.month, 1),
      rangeEndIso: buildJstDateStartUtcIso(nextMonth.year, nextMonth.month, 1),
    }
  }

  return null
}

async function sendLinePushMessages(
  to: string,
  messages: Array<Record<string, unknown>>,
  token: string,
): Promise<{ ok: true } | { ok: false; error: string }> {
  const response = await fetch("https://api.line.me/v2/bot/message/push", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Authorization": `Bearer ${token}`,
    },
    body: JSON.stringify({ to, messages: messages.slice(0, 5) }),
  })
  if (!response.ok) {
    const err = await response.text()
    return { ok: false, error: `LINE push API error (${response.status}): ${err}` }
  }
  return { ok: true }
}
