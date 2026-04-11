import "jsr:@supabase/functions-js/edge-runtime.d.ts"
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.44.0"

type CalendarEnv = {
  calendarId: string
  serviceAccountEmail: string
  serviceAccountPrivateKey: string
  timezone: string
}

type PendingRow = {
  id: number
  room_id: string
  user_id: string | null
  title: string
  date: string
  time: string
  duration_min: number
  location: string | null
  status: string
  expires_at: string
}

const CALENDAR_SCOPE = "https://www.googleapis.com/auth/calendar"
const CALENDAR_CREATE_TIMEZONE = "Asia/Tokyo"
const MAX_BATCH = 50

Deno.serve(async () => {
  const supabaseUrl = Deno.env.get("SUPABASE_URL") ?? ""
  const serviceRoleKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") ?? ""
  if (!supabaseUrl || !serviceRoleKey) {
    return json({
      ok: false,
      error: "SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY is missing.",
    }, 500)
  }

  const envState = loadCalendarEnv()
  if (!envState.ok) {
    return json({
      ok: false,
      error: `Missing calendar env: ${envState.missing.join(", ")}`,
    }, 500)
  }

  const supabase = createClient(supabaseUrl, serviceRoleKey)
  const nowIso = new Date().toISOString()

  const { data, error } = await supabase
    .from("calendar_pending_confirmations")
    .select("id, room_id, user_id, title, date, time, duration_min, location, status, expires_at")
    .eq("status", "pending")
    .lte("expires_at", nowIso)
    .order("expires_at", { ascending: true })
    .limit(MAX_BATCH)

  if (error) {
    return json({ ok: false, error: `Failed to fetch pending confirmations: ${error.message}` }, 500)
  }

  const rows: PendingRow[] = Array.isArray(data)
    ? data.map((row: any) => ({
      id: Number(row?.id ?? 0),
      room_id: String(row?.room_id ?? ""),
      user_id: row?.user_id == null ? null : String(row.user_id),
      title: String(row?.title ?? ""),
      date: String(row?.date ?? ""),
      time: String(row?.time ?? ""),
      duration_min: Number(row?.duration_min ?? 60),
      location: row?.location == null ? null : String(row.location),
      status: String(row?.status ?? ""),
      expires_at: String(row?.expires_at ?? ""),
    }))
    : []

  let processed = 0
  let registered = 0
  const errors: string[] = []

  const env = envState.env
  const accessToken = await fetchGoogleAccessToken(env)

  for (const row of rows) {
    processed += 1
    const claimed = await claimPendingRow(supabase, row.id)
    if (!claimed) continue

    try {
      const title = appendProvisionalSuffixToTitle(row.title)
      const createResult = await createCalendarEventFromPending({
        env,
        accessToken,
        roomId: row.room_id,
        userId: row.user_id,
        title,
        date: row.date,
        time: row.time,
        durationMin: row.duration_min,
        location: row.location,
      })
      if (!createResult.ok) {
        throw new Error(createResult.error)
      }

      const { error: resolveError } = await supabase
        .from("calendar_pending_confirmations")
        .update({
          status: "confirmed",
          resolved_at: new Date().toISOString(),
        })
        .eq("id", row.id)
        .eq("status", "superseded")
      if (resolveError) {
        throw new Error(`failed to resolve confirmed: ${resolveError.message}`)
      }
      registered += 1
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err)
      errors.push(`id=${row.id}: ${message}`)
      // Re-open as pending for next retry cycle.
      await supabase
        .from("calendar_pending_confirmations")
        .update({
          status: "pending",
          resolved_at: null,
        })
        .eq("id", row.id)
        .eq("status", "superseded")
    }
  }

  return json({
    ok: true,
    scanned: rows.length,
    processed,
    registered,
    error_count: errors.length,
    errors,
  })
})

async function claimPendingRow(
  supabase: ReturnType<typeof createClient>,
  id: number,
): Promise<boolean> {
  const { data, error } = await supabase
    .from("calendar_pending_confirmations")
    .update({
      status: "superseded",
      resolved_at: new Date().toISOString(),
    })
    .eq("id", id)
    .eq("status", "pending")
    .select("id")
    .maybeSingle()

  if (error) return false
  return !!data?.id
}

function appendProvisionalSuffixToTitle(title: string): string {
  const normalized = String(title ?? "").trim() || "予定"
  if (/[（(]仮[）)]$/.test(normalized)) return normalized
  return `${normalized}（仮）`
}

function loadCalendarEnv():
  | { ok: true; env: CalendarEnv }
  | { ok: false; missing: string[] } {
  const calendarId = (Deno.env.get("GOOGLE_CALENDAR_ID") ?? "").trim()
  const serviceAccountEmail = (Deno.env.get("GOOGLE_SERVICE_ACCOUNT_EMAIL") ?? "").trim()
  const serviceAccountPrivateKey = normalizePrivateKey((Deno.env.get("GOOGLE_SERVICE_ACCOUNT_PRIVATE_KEY") ?? "").trim())
  const timezone = (Deno.env.get("GOOGLE_CALENDAR_TIMEZONE") ?? "Asia/Tokyo").trim() || "Asia/Tokyo"

  const missing: string[] = []
  if (!calendarId) missing.push("GOOGLE_CALENDAR_ID")
  if (!serviceAccountEmail) missing.push("GOOGLE_SERVICE_ACCOUNT_EMAIL")
  if (!serviceAccountPrivateKey) missing.push("GOOGLE_SERVICE_ACCOUNT_PRIVATE_KEY")
  if (missing.length > 0) return { ok: false, missing }
  return {
    ok: true,
    env: { calendarId, serviceAccountEmail, serviceAccountPrivateKey, timezone },
  }
}

function normalizePrivateKey(raw: string): string {
  return raw.replace(/\\n/g, "\n")
}

async function fetchGoogleAccessToken(env: CalendarEnv): Promise<string> {
  const nowSec = Math.floor(Date.now() / 1000)
  const payload = {
    iss: env.serviceAccountEmail,
    scope: CALENDAR_SCOPE,
    aud: "https://oauth2.googleapis.com/token",
    iat: nowSec,
    exp: nowSec + 3600,
  }

  const encodedHeader = encodeBase64Url(JSON.stringify({ alg: "RS256", typ: "JWT" }))
  const encodedPayload = encodeBase64Url(JSON.stringify(payload))
  const signingInput = `${encodedHeader}.${encodedPayload}`
  const signature = await signJwt(signingInput, env.serviceAccountPrivateKey)
  const assertion = `${signingInput}.${signature}`

  const body = new URLSearchParams()
  body.set("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer")
  body.set("assertion", assertion)

  const response = await fetch("https://oauth2.googleapis.com/token", {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: body.toString(),
  })
  if (!response.ok) {
    const text = await response.text()
    throw new Error(`OAuth token request failed (${response.status}): ${text}`)
  }
  const jsonBody = await response.json()
  const accessToken = String(jsonBody?.access_token ?? "")
  if (!accessToken) throw new Error("OAuth token response missing access_token.")
  return accessToken
}

async function signJwt(input: string, privateKeyPem: string): Promise<string> {
  const keyData = pemToArrayBuffer(privateKeyPem)
  const cryptoKey = await crypto.subtle.importKey(
    "pkcs8",
    keyData,
    {
      name: "RSASSA-PKCS1-v1_5",
      hash: "SHA-256",
    },
    false,
    ["sign"],
  )
  const signatureBuffer = await crypto.subtle.sign(
    "RSASSA-PKCS1-v1_5",
    cryptoKey,
    new TextEncoder().encode(input),
  )
  return encodeBase64UrlBytes(new Uint8Array(signatureBuffer))
}

function pemToArrayBuffer(pem: string): ArrayBuffer {
  const b64 = pem
    .replace(/-----BEGIN PRIVATE KEY-----/g, "")
    .replace(/-----END PRIVATE KEY-----/g, "")
    .replace(/\s+/g, "")
  const binary = atob(b64)
  const bytes = new Uint8Array(binary.length)
  for (let i = 0; i < binary.length; i += 1) {
    bytes[i] = binary.charCodeAt(i)
  }
  return bytes.buffer
}

function encodeBase64Url(value: string): string {
  return encodeBase64UrlBytes(new TextEncoder().encode(value))
}

function encodeBase64UrlBytes(bytes: Uint8Array): string {
  let binary = ""
  for (let i = 0; i < bytes.length; i += 1) {
    binary += String.fromCharCode(bytes[i])
  }
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "")
}

async function createCalendarEventFromPending(params: {
  env: CalendarEnv
  accessToken: string
  roomId: string
  userId: string | null
  title: string
  date: string
  time: string
  durationMin: number
  location: string | null
}): Promise<{ ok: true; eventId: string } | { ok: false; error: string }> {
  const { env, accessToken, roomId, userId, title, date, time, durationMin, location } = params

  const normalizedTime = normalizeTimeToHhMm(time)
  if (!normalizedTime) return { ok: false, error: `Invalid time format: ${time}` }
  const end = addMinutesToLocalDateTime(date, normalizedTime, durationMin)
  if (!end) return { ok: false, error: "Failed to calculate end time." }

  const payload: Record<string, unknown> = {
    summary: title,
    start: {
      dateTime: `${date}T${normalizedTime}:00+09:00`,
      timeZone: CALENDAR_CREATE_TIMEZONE,
    },
    end: {
      dateTime: `${end.date}T${end.time}:00+09:00`,
      timeZone: CALENDAR_CREATE_TIMEZONE,
    },
    extendedProperties: {
      private: {
        line_source_room_id: roomId,
        ...(userId ? { line_source_user_id: userId } : {}),
      },
    },
  }
  if (location && location.trim()) {
    payload.location = location.trim()
  }

  const response = await fetch(
    `https://www.googleapis.com/calendar/v3/calendars/${encodeURIComponent(env.calendarId)}/events`,
    {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${accessToken}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify(payload),
    },
  )
  if (!response.ok) {
    const text = await response.text()
    return { ok: false, error: `Google Calendar API error (${response.status}): ${text}` }
  }
  const data = await response.json()
  const eventId = String(data?.id ?? "").trim()
  if (!eventId) return { ok: false, error: "Calendar event ID is missing in response." }
  return { ok: true, eventId }
}

function normalizeTimeToHhMm(raw: string): string | null {
  const value = String(raw ?? "").trim()
  if (!value) return null
  const m = value.match(/^(\d{1,2}):(\d{2})$/)
  if (!m || !m[1] || !m[2]) return null
  const hour = Number(m[1])
  const minute = Number(m[2])
  if (!Number.isInteger(hour) || !Number.isInteger(minute)) return null
  if (hour < 0 || hour > 23 || minute < 0 || minute > 59) return null
  return `${String(hour).padStart(2, "0")}:${String(minute).padStart(2, "0")}`
}

function addMinutesToLocalDateTime(date: string, time: string, minutes: number): { date: string; time: string } | null {
  const dateMatch = String(date ?? "").match(/^(\d{4})-(\d{2})-(\d{2})$/)
  const timeMatch = String(time ?? "").match(/^(\d{2}):(\d{2})$/)
  if (!dateMatch || !timeMatch) return null
  const year = Number(dateMatch[1])
  const month = Number(dateMatch[2])
  const day = Number(dateMatch[3])
  const hour = Number(timeMatch[1])
  const minute = Number(timeMatch[2])
  if (
    !Number.isInteger(year) || !Number.isInteger(month) || !Number.isInteger(day) ||
    !Number.isInteger(hour) || !Number.isInteger(minute)
  ) return null

  const base = new Date(Date.UTC(year, month - 1, day, hour - 9, minute, 0, 0))
  if (Number.isNaN(base.getTime())) return null
  const target = new Date(base.getTime() + minutes * 60 * 1000)
  if (Number.isNaN(target.getTime())) return null

  const jst = new Date(target.getTime() + 9 * 60 * 60 * 1000)
  const yyyy = String(jst.getUTCFullYear()).padStart(4, "0")
  const mm = String(jst.getUTCMonth() + 1).padStart(2, "0")
  const dd = String(jst.getUTCDate()).padStart(2, "0")
  const hh = String(jst.getUTCHours()).padStart(2, "0")
  const mi = String(jst.getUTCMinutes()).padStart(2, "0")
  return { date: `${yyyy}-${mm}-${dd}`, time: `${hh}:${mi}` }
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
