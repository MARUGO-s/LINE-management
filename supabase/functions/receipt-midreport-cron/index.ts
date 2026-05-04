import "jsr:@supabase/functions-js/edge-runtime.d.ts"
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.44.0"

type ReceiptAggregate = {
  receiptCount: number
  totalGrossSalesYen: number
  totalPartyCount: number
  totalGuestCount: number
  avgGrossSalesYen: number | null
  avgPartyCount: number | null
  avgGuestCount: number | null
}

type ReceiptReportKind = "mid_month" | "month_end"
type ReceiptReportTriggerType = "day15_fallback" | "month_end_fallback"

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
const REPORT_RUN_HOUR_JST = 23
const REPORT_RUN_MINUTE_JST = 59

Deno.serve(async () => {
  const supabaseUrl = Deno.env.get("SUPABASE_URL") ?? ""
  const serviceRoleKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") ?? ""
  const lineAccessToken = Deno.env.get("LINE_CHANNEL_ACCESS_TOKEN") ?? ""

  if (!supabaseUrl || !serviceRoleKey) {
    return json({
      ok: false,
      error: "SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY is missing.",
    }, 500)
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

  const { data: rawRows, error: rowError } = await supabase
    .from("line_receipt_entries")
    .select("room_id, gross_sales_yen, party_count, guest_count")
    .gte("created_at", schedule.rangeStartIso)
    .lt("created_at", schedule.rangeEndIso)

  if (rowError) {
    return json({
      ok: false,
      error: `Failed to load line_receipt_entries: ${rowError.message}`,
    }, 500)
  }

  const rows = Array.isArray(rawRows) ? rawRows as Array<Record<string, unknown>> : []
  if (rows.length === 0) {
    return json({
      ok: true,
      skipped: true,
      reason: `no_receipt_entries_for_${schedule.reportKind}`,
      report_kind: schedule.reportKind,
      report_month: schedule.reportMonth,
    }, 200)
  }

  const roomAggregateMap = buildRoomReceiptAggregateMap(rows)
  if (roomAggregateMap.size === 0) {
    return json({
      ok: true,
      skipped: true,
      reason: "no_aggregatable_receipt_entries",
      report_kind: schedule.reportKind,
      report_month: schedule.reportMonth,
    }, 200)
  }

  const roomIds = [...roomAggregateMap.keys()]
  const { data: existingRows, error: existingError } = await supabase
    .from("line_receipt_mid_reports")
    .select("room_id")
    .eq("report_month", schedule.reportMonth)
    .eq("report_kind", schedule.reportKind)
    .in("room_id", roomIds)

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

  for (const [roomId, aggregate] of roomAggregateMap.entries()) {
    if (existingSet.has(roomId)) {
      skippedRoomIds.push(roomId)
      continue
    }

    const reportMessages = buildReceiptReportFlexMessages(aggregate, {
      reportTitle: schedule.reportTitle,
      periodStartDate: schedule.periodStartDate,
      periodEndDate: schedule.periodEndDate,
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
        avg_gross_sales_yen: aggregate.avgGrossSalesYen == null ? null : Math.round(aggregate.avgGrossSalesYen),
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
    source_room_count: roomIds.length,
    sent_room_count: sentRoomIds.length,
    skipped_room_count: skippedRoomIds.length,
    error_count: errors.length,
    sent_room_ids: sentRoomIds,
    skipped_room_ids: skippedRoomIds,
    errors,
  }, 200)
})

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

  const reportMonth = toJstDateString(jst.year, jst.month, 1)
  const periodStartDate = reportMonth
  const rangeStartIso = buildJstDateStartUtcIso(jst.year, jst.month, 1)

  if (jst.day === 15) {
    return {
      reportKind: "mid_month",
      reportTitle: RECEIPT_MID_REPORT_TITLE,
      triggerType: "day15_fallback",
      reportMonth,
      periodStartDate,
      periodEndDate: toJstDateString(jst.year, jst.month, 15),
      rangeStartIso,
      rangeEndIso: buildJstDateStartUtcIso(jst.year, jst.month, 16),
    }
  }

  const monthLastDay = getJstMonthLastDay(jst.year, jst.month)
  if (jst.day !== monthLastDay) {
    return null
  }

  const nextMonth = shiftJstYearMonth(jst.year, jst.month, 1)
  return {
    reportKind: "month_end",
    reportTitle: RECEIPT_MONTH_END_REPORT_TITLE,
    triggerType: "month_end_fallback",
    reportMonth,
    periodStartDate,
    periodEndDate: toJstDateString(jst.year, jst.month, monthLastDay),
    rangeStartIso,
    rangeEndIso: buildJstDateStartUtcIso(nextMonth.year, nextMonth.month, 1),
  }
}

function buildRoomReceiptAggregateMap(rows: Array<Record<string, unknown>>): Map<string, ReceiptAggregate> {
  const byRoom = new Map<string, {
    receiptCount: number
    totalGrossSalesYen: number
    totalPartyCount: number
    totalGuestCount: number
    grossCount: number
    partyCountRows: number
    guestCountRows: number
  }>()

  for (const row of rows) {
    const roomId = String(row.room_id ?? "").trim()
    if (!roomId) continue
    if (!byRoom.has(roomId)) {
      byRoom.set(roomId, {
        receiptCount: 0,
        totalGrossSalesYen: 0,
        totalPartyCount: 0,
        totalGuestCount: 0,
        grossCount: 0,
        partyCountRows: 0,
        guestCountRows: 0,
      })
    }
    const target = byRoom.get(roomId)
    if (!target) continue
    target.receiptCount += 1

    const gross = Number(row.gross_sales_yen)
    if (Number.isFinite(gross) && gross >= 0) {
      target.totalGrossSalesYen += Math.round(gross)
      target.grossCount += 1
    }
    const party = Number(row.party_count)
    if (Number.isFinite(party) && party >= 0) {
      target.totalPartyCount += Math.round(party)
      target.partyCountRows += 1
    }
    const guest = Number(row.guest_count)
    if (Number.isFinite(guest) && guest >= 0) {
      target.totalGuestCount += Math.round(guest)
      target.guestCountRows += 1
    }
  }

  const result = new Map<string, ReceiptAggregate>()
  for (const [roomId, row] of byRoom.entries()) {
    if (row.receiptCount <= 0) continue
    result.set(roomId, {
      receiptCount: row.receiptCount,
      totalGrossSalesYen: row.totalGrossSalesYen,
      totalPartyCount: row.totalPartyCount,
      totalGuestCount: row.totalGuestCount,
      avgGrossSalesYen: row.grossCount > 0 ? row.totalGrossSalesYen / row.grossCount : null,
      avgPartyCount: row.partyCountRows > 0 ? row.totalPartyCount / row.partyCountRows : null,
      avgGuestCount: row.guestCountRows > 0 ? row.totalGuestCount / row.guestCountRows : null,
    })
  }
  return result
}

function formatYenAmount(value: number): string {
  return `¥${Math.round(value).toLocaleString("ja-JP")}`
}

function formatAverageCount(value: number | null): string {
  if (value == null || !Number.isFinite(value)) return "-"
  if (Math.abs(value - Math.round(value)) < 0.0001) return String(Math.round(value))
  return value.toFixed(2).replace(/\.?0+$/, "")
}

function buildReceiptReportFlexMessages(
  aggregate: ReceiptAggregate,
  opts: { reportTitle: string; periodStartDate: string; periodEndDate: string },
): Array<Record<string, unknown>> {
  const adminToken = Deno.env.get("ADMIN_DASHBOARD_TOKEN") ?? ""
  const dashboardUri = `https://marugo-s.github.io/LINE-management/analytics.html${adminToken ? `?t=${encodeURIComponent(adminToken)}` : ""}`

  const row = (label: string, value: string): Record<string, unknown> => ({
    type: "box", layout: "baseline", spacing: "sm",
    contents: [
      { type: "text", text: label, size: "sm", color: "#888888", flex: 4, wrap: false },
      { type: "text", text: value, size: "sm", color: "#1F1F1F", flex: 6, wrap: true, weight: "bold" },
    ],
  })

  const avgUnit = aggregate.avgGrossSalesYen == null ? null
    : (aggregate.totalGuestCount > 0 ? Math.round(aggregate.totalGrossSalesYen / aggregate.totalGuestCount) : null)

  const altText = `【${opts.reportTitle}】${opts.periodStartDate}〜${opts.periodEndDate} 総売上: ${formatYenAmount(aggregate.totalGrossSalesYen)}`

  return [{
    type: "flex",
    altText: altText.slice(0, 400),
    contents: {
      type: "bubble",
      header: {
        type: "box", layout: "vertical", paddingAll: "16dp",
        backgroundColor: "#006c3a",
        contents: [
          { type: "text", text: `📊 ${opts.reportTitle}`, size: "lg", weight: "bold", color: "#FFFFFF" },
          { type: "text", text: `${opts.periodStartDate}〜${opts.periodEndDate}`, size: "xs", color: "#CCFFDD", margin: "sm" },
        ],
      },
      body: {
        type: "box", layout: "vertical", spacing: "sm", paddingAll: "14dp",
        contents: [
          row("総売上", formatYenAmount(aggregate.totalGrossSalesYen)),
          row("組数合計", `${aggregate.totalPartyCount.toLocaleString("ja-JP")} 組`),
          row("客数合計", `${aggregate.totalGuestCount.toLocaleString("ja-JP")} 名`),
          ...(avgUnit != null ? [row("客単価", formatYenAmount(avgUnit))] : []),
          row("1日平均売上", aggregate.avgGrossSalesYen == null ? "-" : formatYenAmount(aggregate.avgGrossSalesYen)),
          row("組数平均", `${formatAverageCount(aggregate.avgPartyCount)} 組/日`),
          row("レシート", `${aggregate.receiptCount.toLocaleString("ja-JP")} 件`),
        ],
      },
      footer: {
        type: "box", layout: "vertical", spacing: "sm", paddingAll: "12dp",
        contents: [{
          type: "button", style: "secondary", height: "sm",
          action: { type: "uri", label: "📈 売上推移を見る", uri: dashboardUri },
        }],
      },
    },
  }]
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
