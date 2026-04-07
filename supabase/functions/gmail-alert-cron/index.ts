import "jsr:@supabase/functions-js/edge-runtime.d.ts"
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.44.0"

type GmailAlertEnv = {
  enabled: boolean
  clientId: string
  clientSecret: string
  refreshToken: string
  query: string
  maxMessages: number
  fallbackTargetRoomId: string
}

type GmailAlertEnvState =
  | { ok: true; env: GmailAlertEnv }
  | { ok: false; missing: string[] }

type GmailMessageListItem = {
  id: string
  threadId: string | null
}

type GmailMessageAlert = {
  id: string
  threadId: string | null
  subject: string
  from: string
  snippet: string
  internalDateIso: string | null
}

const DEFAULT_GMAIL_ALERT_QUERY = "is:inbox is:unread newer_than:7d (予約 OR reservation OR booking)"
const DEFAULT_GMAIL_ALERT_MAX_MESSAGES = 5
const MAX_GMAIL_ALERT_MAX_MESSAGES = 20

Deno.serve(async () => {
  const supabaseUrl = Deno.env.get("SUPABASE_URL") ?? ""
  const supabaseKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") ?? ""
  const lineAccessToken = Deno.env.get("LINE_CHANNEL_ACCESS_TOKEN") ?? ""
  const fallbackOverallRoomId = Deno.env.get("LINE_OVERALL_ROOM_ID") ?? ""

  if (!supabaseUrl || !supabaseKey) {
    return json({
      ok: false,
      error: "SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY is missing.",
    }, 500)
  }

  const supabase = createClient(supabaseUrl, supabaseKey)
  const now = new Date()
  const jstHour = (now.getUTCHours() + 9) % 24

  try {
    const result = await maybeSendGmailReservationAlerts({
      supabase,
      now,
      jstHour,
      lineAccessToken,
      fallbackOverallRoomId,
    })

    return json({
      ok: true,
      ...result,
    }, 200)
  } catch (e) {
    console.error("Failed to process Gmail reservation alerts:", e)
    return json({
      ok: false,
      error: e instanceof Error ? e.message : String(e),
    }, 500)
  }
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

async function maybeSendGmailReservationAlerts(params: {
  supabase: ReturnType<typeof createClient>
  now: Date
  jstHour: number
  lineAccessToken: string
  fallbackOverallRoomId: string
}): Promise<Record<string, unknown>> {
  const {
    supabase,
    now,
    jstHour,
    lineAccessToken,
    fallbackOverallRoomId,
  } = params

  const envState = loadGmailAlertEnv(fallbackOverallRoomId)
  if (!envState.ok) {
    return {
      skipped: true,
      reason: `missing_env: ${envState.missing.join(", ")}`,
    }
  }
  const env = envState.env
  if (!env.enabled) {
    return { skipped: true, reason: "gmail_alert_disabled" }
  }
  if (!lineAccessToken) {
    return { skipped: true, reason: "missing_line_channel_access_token" }
  }

  const targetRoomIds = await resolveGmailAlertTargetRooms(supabase, env.fallbackTargetRoomId)
  if (targetRoomIds.length === 0) {
    return { skipped: true, reason: "no_target_rooms" }
  }

  const accessToken = await fetchGmailAccessTokenByRefreshToken(env)
  const listedMessages = await listGmailMessages(accessToken, env.query, env.maxMessages)
  if (listedMessages.length === 0) {
    return { skipped: true, reason: "no_matching_messages" }
  }

  const unnotifiedMessageIds = await filterUnnotifiedGmailMessageIds(
    supabase,
    listedMessages.map((message) => message.id),
  )
  if (unnotifiedMessageIds.length === 0) {
    return { skipped: true, reason: "already_notified" }
  }

  const unnotifiedSet = new Set(unnotifiedMessageIds)
  const messagesToFetch = listedMessages.filter((message) => unnotifiedSet.has(message.id))
  const alerts: GmailMessageAlert[] = []

  for (const message of messagesToFetch) {
    const detail = await fetchGmailMessageAlert(accessToken, message.id)
    if (!detail) continue
    alerts.push(detail)
  }

  if (alerts.length === 0) {
    return { skipped: true, reason: "no_alert_payload" }
  }

  const lineText = buildGmailReservationAlertMessage(alerts)
  const successfulTargetRoomIds: string[] = []

  for (const targetRoomId of targetRoomIds) {
    const sendResult = await sendLineMessage(targetRoomId, lineText, lineAccessToken)
    if (!sendResult.ok) {
      await writeDeliveryLog(supabase, {
        jst_hour: jstHour,
        status: "gmail_alert_send_failed",
        reason: sendResult.error,
        should_send_overall: false,
        rooms_targeted: 1,
        messages_in_queue: 0,
        messages_marked_processed: 0,
        line_send_attempted: true,
        line_send_success: false,
        line_http_status: sendResult.status ?? null,
        target_room_id: targetRoomId,
        details: {
          gmail_query: env.query,
          listed_count: listedMessages.length,
          unnotified_count: alerts.length,
          target_room_count: targetRoomIds.length,
          source: "gmail-alert-cron",
        },
      })
      continue
    }

    successfulTargetRoomIds.push(targetRoomId)
    await writeDeliveryLog(supabase, {
      jst_hour: jstHour,
      status: "gmail_alert_sent",
      reason: `Sent ${alerts.length} reservation email alerts.`,
      should_send_overall: false,
      rooms_targeted: 1,
      messages_in_queue: 0,
      messages_marked_processed: 0,
      line_send_attempted: true,
      line_send_success: true,
      line_http_status: sendResult.status ?? null,
      target_room_id: targetRoomId,
      details: {
        gmail_query: env.query,
        listed_count: listedMessages.length,
        unnotified_count: alerts.length,
        target_room_count: targetRoomIds.length,
        source: "gmail-alert-cron",
      },
    })
  }

  if (successfulTargetRoomIds.length === 0) {
    return {
      sent: false,
      alerts_count: alerts.length,
      target_rooms: targetRoomIds.length,
      success_rooms: 0,
    }
  }

  await saveGmailReservationAlertLogs(supabase, alerts, successfulTargetRoomIds[0], now)

  return {
    sent: true,
    alerts_count: alerts.length,
    target_rooms: targetRoomIds.length,
    success_rooms: successfulTargetRoomIds.length,
  }
}

async function resolveGmailAlertTargetRooms(
  supabase: ReturnType<typeof createClient>,
  fallbackTargetRoomId: string,
): Promise<string[]> {
  const { data, error } = await supabase
    .from("room_summary_settings")
    .select("room_id, is_enabled, gmail_reservation_alert_enabled")

  if (error) {
    console.error("Failed to fetch room settings for Gmail alert targets:", error.message)
    const fallback = String(fallbackTargetRoomId ?? "").trim()
    return fallback ? [fallback] : []
  }

  const rows = Array.isArray(data) ? data : []
  const enabledRoomIds = rows
    .filter((row: any) => row?.is_enabled !== false && row?.gmail_reservation_alert_enabled === true)
    .map((row: any) => String(row?.room_id ?? "").trim())
    .filter((roomId: string) => roomId.length > 0)

  if (enabledRoomIds.length > 0) {
    return Array.from(new Set(enabledRoomIds))
  }

  if (rows.length > 0) {
    return []
  }

  const fallback = String(fallbackTargetRoomId ?? "").trim()
  return fallback ? [fallback] : []
}

function loadGmailAlertEnv(fallbackOverallRoomId: string): GmailAlertEnvState {
  const clientId = String(Deno.env.get("GMAIL_CLIENT_ID") ?? "").trim()
  const clientSecret = String(Deno.env.get("GMAIL_CLIENT_SECRET") ?? "").trim()
  const refreshToken = String(Deno.env.get("GMAIL_REFRESH_TOKEN") ?? "").trim()
  const query = String(Deno.env.get("GMAIL_ALERT_QUERY") ?? "").trim() || DEFAULT_GMAIL_ALERT_QUERY
  const fallbackTargetRoomId = String(Deno.env.get("LINE_GMAIL_ALERT_ROOM_ID") ?? "").trim() ||
    String(fallbackOverallRoomId ?? "").trim()

  const hasAnyCredential = !!clientId || !!clientSecret || !!refreshToken
  const enabled = parseBooleanEnv(Deno.env.get("GMAIL_ALERT_ENABLED"), hasAnyCredential)
  const rawMaxMessages = Number(Deno.env.get("GMAIL_ALERT_MAX_MESSAGES") ?? DEFAULT_GMAIL_ALERT_MAX_MESSAGES)
  const maxMessages = Number.isInteger(rawMaxMessages) &&
      rawMaxMessages >= 1 &&
      rawMaxMessages <= MAX_GMAIL_ALERT_MAX_MESSAGES
    ? rawMaxMessages
    : DEFAULT_GMAIL_ALERT_MAX_MESSAGES

  if (!enabled) {
    return {
      ok: true,
      env: {
        enabled: false,
        clientId,
        clientSecret,
        refreshToken,
        query,
        maxMessages,
        fallbackTargetRoomId,
      },
    }
  }

  const missing: string[] = []
  if (!clientId) missing.push("GMAIL_CLIENT_ID")
  if (!clientSecret) missing.push("GMAIL_CLIENT_SECRET")
  if (!refreshToken) missing.push("GMAIL_REFRESH_TOKEN")
  if (missing.length > 0) {
    return { ok: false, missing }
  }

  return {
    ok: true,
    env: {
      enabled: true,
      clientId,
      clientSecret,
      refreshToken,
      query,
      maxMessages,
      fallbackTargetRoomId,
    },
  }
}

async function fetchGmailAccessTokenByRefreshToken(env: GmailAlertEnv): Promise<string> {
  const body = new URLSearchParams()
  body.set("client_id", env.clientId)
  body.set("client_secret", env.clientSecret)
  body.set("refresh_token", env.refreshToken)
  body.set("grant_type", "refresh_token")

  const response = await fetch("https://oauth2.googleapis.com/token", {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: body.toString(),
  })

  if (!response.ok) {
    const text = await response.text()
    throw new Error(`Gmail OAuth token request failed (${response.status}): ${text}`)
  }

  const json = await response.json()
  const accessToken = String(json?.access_token ?? "")
  if (!accessToken) {
    throw new Error("Gmail OAuth token response missing access_token.")
  }
  return accessToken
}

async function listGmailMessages(
  accessToken: string,
  query: string,
  maxMessages: number,
): Promise<GmailMessageListItem[]> {
  const url = new URL("https://gmail.googleapis.com/gmail/v1/users/me/messages")
  url.searchParams.set("maxResults", String(maxMessages))
  url.searchParams.set("includeSpamTrash", "false")
  if (query) {
    url.searchParams.set("q", query)
  }

  const response = await fetch(url.toString(), {
    method: "GET",
    headers: {
      "Authorization": `Bearer ${accessToken}`,
      "Content-Type": "application/json",
    },
  })

  if (!response.ok) {
    const text = await response.text()
    throw new Error(`Gmail messages.list failed (${response.status}): ${text}`)
  }

  const data = await response.json()
  const rows = Array.isArray(data?.messages) ? data.messages : []
  return rows
    .map((row: any) => ({
      id: String(row?.id ?? "").trim(),
      threadId: String(row?.threadId ?? "").trim() || null,
    }))
    .filter((row: GmailMessageListItem) => row.id.length > 0)
}

async function filterUnnotifiedGmailMessageIds(
  supabase: ReturnType<typeof createClient>,
  messageIds: string[],
): Promise<string[]> {
  if (messageIds.length === 0) {
    return []
  }

  const uniqueIds = Array.from(
    new Set(messageIds.map((value) => String(value ?? "").trim()).filter((value) => value.length > 0)),
  )
  if (uniqueIds.length === 0) {
    return []
  }

  const { data, error } = await supabase
    .from("gmail_reservation_alert_logs")
    .select("gmail_message_id")
    .in("gmail_message_id", uniqueIds)

  if (error) {
    throw new Error(`Failed to query Gmail alert log table: ${error.message}`)
  }

  const existing = new Set<string>(
    (Array.isArray(data) ? data : [])
      .map((row: any) => String(row?.gmail_message_id ?? "").trim())
      .filter((value: string) => value.length > 0),
  )
  return uniqueIds.filter((id) => !existing.has(id))
}

async function fetchGmailMessageAlert(
  accessToken: string,
  messageId: string,
): Promise<GmailMessageAlert | null> {
  const normalizedMessageId = String(messageId ?? "").trim()
  if (!normalizedMessageId) {
    return null
  }

  const url = new URL(`https://gmail.googleapis.com/gmail/v1/users/me/messages/${encodeURIComponent(normalizedMessageId)}`)
  url.searchParams.set("format", "metadata")
  url.searchParams.append("metadataHeaders", "Subject")
  url.searchParams.append("metadataHeaders", "From")
  url.searchParams.append("metadataHeaders", "Date")

  const response = await fetch(url.toString(), {
    method: "GET",
    headers: {
      "Authorization": `Bearer ${accessToken}`,
      "Content-Type": "application/json",
    },
  })

  if (!response.ok) {
    const text = await response.text()
    throw new Error(`Gmail messages.get failed (${response.status}): ${text}`)
  }

  const data = await response.json()
  const payloadHeaders = Array.isArray(data?.payload?.headers) ? data.payload.headers : []
  const subject = normalizeInlineText(extractGmailHeader(payloadHeaders, "subject")) || "(件名なし)"
  const from = normalizeInlineText(extractGmailHeader(payloadHeaders, "from")) || "(送信元不明)"
  const snippet = normalizeInlineText(String(data?.snippet ?? ""))
  const internalDateMs = Number(data?.internalDate)
  const internalDateIso = Number.isFinite(internalDateMs) && internalDateMs > 0
    ? new Date(internalDateMs).toISOString()
    : null

  return {
    id: String(data?.id ?? normalizedMessageId),
    threadId: String(data?.threadId ?? "").trim() || null,
    subject,
    from,
    snippet,
    internalDateIso,
  }
}

function extractGmailHeader(
  headers: Array<{ name?: string; value?: string }>,
  headerName: string,
): string {
  const target = headerName.toLowerCase()
  for (const header of headers) {
    const name = String(header?.name ?? "").trim().toLowerCase()
    if (name !== target) continue
    return String(header?.value ?? "")
  }
  return ""
}

function buildGmailReservationAlertMessage(alerts: GmailMessageAlert[]): string {
  const lines: string[] = [`【予約メール通知】新着${alerts.length}件`]
  for (let i = 0; i < alerts.length; i += 1) {
    const alert = alerts[i]
    lines.push(`${i + 1}.`)
    lines.push(`  受信: ${formatGmailAlertReceivedAt(alert.internalDateIso)}`)
    lines.push(`  件名: ${alert.subject || "(件名なし)"}`)
    lines.push(`  送信元: ${alert.from || "(送信元不明)"}`)
    lines.push(`  内容: ${formatGmailAlertSnippet(alert.snippet)}`)
    if (i < alerts.length - 1) {
      lines.push("")
    }
  }
  return lines.join("\n").slice(0, 4900)
}

function formatGmailAlertReceivedAt(iso: string | null): string {
  if (!iso) return "(受信時刻不明)"
  const date = new Date(iso)
  if (Number.isNaN(date.getTime())) return "(受信時刻不明)"
  return `${formatDateOnlyForLine(date, "Asia/Tokyo")} ${formatTimeOnlyForLine(date, "Asia/Tokyo")}`
}

function formatGmailAlertSnippet(snippet: string): string {
  const normalized = normalizeInlineText(snippet)
  if (!normalized) return "（本文プレビューなし）"
  if (normalized.length <= 120) return normalized
  return `${normalized.slice(0, 120)}...`
}

async function saveGmailReservationAlertLogs(
  supabase: ReturnType<typeof createClient>,
  alerts: GmailMessageAlert[],
  targetRoomId: string,
  now: Date,
): Promise<void> {
  if (alerts.length === 0) return
  const lineSentAt = now.toISOString()
  const rows = alerts.map((alert) => ({
    gmail_message_id: alert.id,
    gmail_thread_id: alert.threadId,
    gmail_subject: alert.subject,
    gmail_from: alert.from,
    gmail_internal_date: alert.internalDateIso,
    line_target_room_id: targetRoomId,
    line_message_sent_at: lineSentAt,
  }))
  const { error } = await supabase
    .from("gmail_reservation_alert_logs")
    .upsert(rows, { onConflict: "gmail_message_id" })
  if (error) {
    throw new Error(`Failed to save Gmail alert logs: ${error.message}`)
  }
}

async function writeDeliveryLog(
  supabase: ReturnType<typeof createClient>,
  payload: {
    jst_hour: number
    status: string
    reason?: string
    should_send_overall: boolean
    rooms_targeted: number
    messages_in_queue: number
    messages_marked_processed: number
    line_send_attempted: boolean
    line_send_success: boolean
    line_http_status?: number | null
    target_room_id: string | null
    details?: Record<string, unknown>
  },
) {
  try {
    const { error } = await supabase
      .from("summary_delivery_logs")
      .insert({
        ...payload,
        details: payload.details ?? {},
      })

    if (error) {
      console.error("Failed to insert summary_delivery_logs:", error.message)
      return
    }

    await pruneDeliveryLogs(supabase, 100)
  } catch (e) {
    console.error("Unexpected error while inserting summary_delivery_logs:", e)
  }
}

async function pruneDeliveryLogs(
  supabase: ReturnType<typeof createClient>,
  keepLatest: number,
) {
  if (!Number.isInteger(keepLatest) || keepLatest <= 0) return
  try {
    const cutoffIndex = keepLatest - 1
    const { data: cutoff, error: cutoffError } = await supabase
      .from("summary_delivery_logs")
      .select("id")
      .order("id", { ascending: false })
      .range(cutoffIndex, cutoffIndex)
      .maybeSingle()

    if (cutoffError || !cutoff?.id) {
      return
    }

    const { error: deleteError } = await supabase
      .from("summary_delivery_logs")
      .delete()
      .lt("id", cutoff.id)

    if (deleteError) {
      console.error("Failed to prune summary_delivery_logs:", deleteError.message)
    }
  } catch (e) {
    console.error("Unexpected error while pruning summary_delivery_logs:", e)
  }
}

async function sendLineMessage(to: string, text: string, token: string) {
  try {
    const response = await fetch("https://api.line.me/v2/bot/message/push", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${token}`,
      },
      body: JSON.stringify({
        to,
        messages: [{ type: "text", text }],
      }),
    })

    if (!response.ok) {
      const errorText = await response.text()
      console.error(`Failed to send LINE message to ${to}. Status: ${response.status} Error: ${errorText}`)
      return { ok: false as const, status: response.status, error: errorText || `HTTP ${response.status}` }
    }

    return { ok: true as const, status: response.status as number }
  } catch (error) {
    console.error(`Network or fetch error while sending to ${to}:`, error)
    return { ok: false as const, error: error instanceof Error ? error.message : String(error) }
  }
}

function normalizeInlineText(raw: string): string {
  return String(raw ?? "").replace(/\u3000/g, " ").replace(/\s+/g, " ").trim()
}

function formatDateOnlyForLine(date: Date, timezone: string): string {
  return new Intl.DateTimeFormat("ja-JP", {
    timeZone: timezone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).format(date).replace(/\./g, "/")
}

function formatTimeOnlyForLine(date: Date, timezone: string): string {
  return new Intl.DateTimeFormat("ja-JP", {
    timeZone: timezone,
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).format(date)
}

function parseBooleanEnv(value: string | undefined, fallback: boolean): boolean {
  if (value == null) return fallback
  const normalized = String(value).trim().toLowerCase()
  if (!normalized) return fallback
  if (normalized === "1" || normalized === "true" || normalized === "yes" || normalized === "on") return true
  if (normalized === "0" || normalized === "false" || normalized === "no" || normalized === "off") return false
  return fallback
}
