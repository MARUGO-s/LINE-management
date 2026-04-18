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

const JST_OFFSET_MS = 9 * 60 * 60 * 1000
const RECEIPT_MID_REPORT_TITLE = "中間報告"

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
  if (!(jst.day === 15 && jst.hour === 23 && jst.minute === 59)) {
    return json({
      ok: true,
      skipped: true,
      reason: "not_mid_month_report_time",
      now_jst: `${toJstDateString(jst.year, jst.month, jst.day)} ${String(jst.hour).padStart(2, "0")}:${String(jst.minute).padStart(2, "0")}`,
    }, 200)
  }

  const reportMonth = toJstDateString(jst.year, jst.month, 1)
  const periodStartDate = reportMonth
  const periodEndDate = toJstDateString(jst.year, jst.month, 15)
  const rangeStartIso = buildJstDateStartUtcIso(jst.year, jst.month, 1)
  const rangeEndIso = buildJstDateStartUtcIso(jst.year, jst.month, 16)

  const supabase = createClient(supabaseUrl, serviceRoleKey)

  const { data: rawRows, error: rowError } = await supabase
    .from("line_receipt_entries")
    .select("room_id, gross_sales_yen, party_count, guest_count")
    .gte("created_at", rangeStartIso)
    .lt("created_at", rangeEndIso)

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
      reason: "no_receipt_entries_for_mid_month",
      report_month: reportMonth,
    }, 200)
  }

  const roomAggregateMap = buildRoomReceiptAggregateMap(rows)
  if (roomAggregateMap.size === 0) {
    return json({
      ok: true,
      skipped: true,
      reason: "no_aggregatable_receipt_entries",
      report_month: reportMonth,
    }, 200)
  }

  const roomIds = [...roomAggregateMap.keys()]
  const { data: existingRows, error: existingError } = await supabase
    .from("line_receipt_mid_reports")
    .select("room_id")
    .eq("report_month", reportMonth)
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

    const reportText = buildMidMonthReceiptReportMessage(aggregate, {
      periodStartDate,
      periodEndDate,
    })
    const sendResult = await sendLinePushTextMessage(roomId, reportText, lineAccessToken)
    if (!sendResult.ok) {
      errors.push(`${roomId}: ${sendResult.error}`)
      continue
    }

    const { error: insertError } = await supabase
      .from("line_receipt_mid_reports")
      .insert({
        report_month: reportMonth,
        room_id: roomId,
        period_start_jst: periodStartDate,
        period_end_jst: periodEndDate,
        trigger_type: "day15_fallback",
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
    report_month: reportMonth,
    period: { start: periodStartDate, end: periodEndDate },
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

function buildMidMonthReceiptReportMessage(
  aggregate: ReceiptAggregate,
  opts: { periodStartDate: string; periodEndDate: string },
): string {
  return [
    `【${RECEIPT_MID_REPORT_TITLE}】`,
    `対象期間: ${opts.periodStartDate}〜${opts.periodEndDate}`,
    `総売上合計: ${formatYenAmount(aggregate.totalGrossSalesYen)}`,
    `組数合計: ${aggregate.totalPartyCount.toLocaleString("ja-JP")}`,
    `客数合計: ${aggregate.totalGuestCount.toLocaleString("ja-JP")}`,
    `総売上平均: ${aggregate.avgGrossSalesYen == null ? "-" : formatYenAmount(aggregate.avgGrossSalesYen)}`,
    `組数平均: ${formatAverageCount(aggregate.avgPartyCount)}`,
    `客数平均: ${formatAverageCount(aggregate.avgGuestCount)}`,
    `レシート件数: ${aggregate.receiptCount.toLocaleString("ja-JP")}`,
  ].join("\n")
}

async function sendLinePushTextMessage(
  to: string,
  text: string,
  token: string,
): Promise<{ ok: true } | { ok: false; error: string }> {
  const response = await fetch("https://api.line.me/v2/bot/message/push", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Authorization": `Bearer ${token}`,
    },
    body: JSON.stringify({
      to,
      messages: [{ type: "text", text: text.slice(0, 4900) }],
    }),
  })
  if (!response.ok) {
    const err = await response.text()
    return { ok: false, error: `LINE push API error (${response.status}): ${err}` }
  }
  return { ok: true }
}
