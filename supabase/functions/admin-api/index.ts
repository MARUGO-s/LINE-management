import "jsr:@supabase/functions-js/edge-runtime.d.ts"
import { isJobTitleLabel, JOB_TITLE_OPTIONS } from "../_shared/job_titles.ts"
import {
  isMarugoGroupStoreLabel,
  MARUGO_GROUP_STORE_OPTIONS,
} from "../_shared/marugo_group_stores.ts"
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.44.0"
import JSZip from "https://esm.sh/jszip@3.10.1"

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type, x-admin-token",
  "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
}

type AppError = {
  status: number
  message: string
}

type MessageCleanupTiming = "after_each_delivery" | "end_of_day"
type LastDeliverySummaryMode = "independent" | "daily_rollup"
type MessageRetentionDays = 0 | 60 | 120 | 180 | 365 | 730 | 1095
type StorageUsageTableStat = {
  table_name: string
  size_bytes: number
  size_pretty: string
}
type StorageUsageStats = {
  database_size_bytes: number
  database_size_pretty: string
  managed_tables_total_bytes: number
  managed_tables_total_pretty: string
  managed_tables: StorageUsageTableStat[]
}
type MediaType = "image" | "video" | "audio" | "file"
type MediaListRow = {
  id: number
  message_id: string
  line_message_id: string
  room_id: string
  room_name: string | null
  user_id: string | null
  media_type: MediaType
  storage_bucket: string
  storage_path: string
  original_file_name: string | null
  mime_type: string | null
  file_size_bytes: number
  created_at: string
}
type MediaMessageContext = {
  before_text: string | null
  before_at: string | null
  after_text: string | null
  after_at: string | null
}
type DocumentMimeType =
  | "text/plain"
  | "application/pdf"
  | "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
  | "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
type DocumentListRow = {
  id: number
  room_id: string | null
  room_name: string | null
  storage_bucket: string
  storage_path: string
  original_file_name: string
  mime_type: DocumentMimeType
  file_size_bytes: number
  extracted_text: string
  source: string
  created_at: string
  updated_at: string
}
type DocumentViewerPermissionState = {
  document_id: number
  mode: "public" | "restricted"
  allowed_user_ids: string[]
}
type LineUserPermissionRow = {
  line_user_id: string
  display_name: string | null
  is_active: boolean
  can_message_search: boolean
  can_library_search: boolean
  can_calendar_create: boolean
  can_calendar_update: boolean
  can_media_access: boolean
  excluded_message_search_room_ids: string[]
  assigned_store: string | null
  assigned_job_title: string | null
  updated_at: string
}

const MEDIA_SIGNED_URL_EXPIRES_SEC = 60 * 30
const MEDIA_LIST_DEFAULT_LIMIT = 24
const MEDIA_LIST_MAX_LIMIT = 100
const MEDIA_STORAGE_CAP_BYTES = 2 * 1024 * 1024 * 1024
const DEFAULT_MEDIA_UPLOAD_MAX_MB = 10
const MAX_MEDIA_UPLOAD_MAX_MB = 20
const LINE_DOCUMENT_BUCKET = "line-documents"
const DOCUMENT_LIST_DEFAULT_LIMIT = 20
const DOCUMENT_LIST_MAX_LIMIT = 100
const DOCUMENT_UPLOAD_MAX_BYTES = 20 * 1024 * 1024
const DOCUMENT_EXTRACT_MAX_CHARS = 250000
const DOCUMENT_PREVIEW_MAX_CHARS = 240
const DOCUMENT_PDF_EXTRACT_MAX_PAGES = 120
const DOCUMENT_TEXT_BINARY_RATIO_MAX = 0.08
const PDFJS_MODULE_URL = "https://esm.sh/pdfjs-dist@4.10.38/build/pdf.mjs"
const DOCX_MIME_TYPE = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
const XLSX_MIME_TYPE = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
const DOCUMENT_ARCHIVE_MAX_XML_ENTRIES = 120
const DOCUMENT_ARCHIVE_MAX_ENTRIES = 400
const DOCUMENT_ARCHIVE_TOTAL_UNCOMPRESSED_MAX_BYTES = 80 * 1024 * 1024
const DOCUMENT_ARCHIVE_SINGLE_ENTRY_MAX_BYTES = 24 * 1024 * 1024
const DOCUMENT_ARCHIVE_MAX_COMPRESSION_RATIO = 40
const DOCUMENT_ARCHIVE_ENTRY_MAX_BYTES = 8 * 1024 * 1024
const ADMIN_RATE_LIMIT_DEFAULT_WINDOW_MS = 60 * 1000
const ADMIN_RATE_LIMIT_DEFAULT_MAX_REQUESTS = 180
const ADMIN_RATE_LIMIT_UPLOAD_WINDOW_MS = 60 * 1000
const ADMIN_RATE_LIMIT_UPLOAD_MAX_REQUESTS = 12
const USER_PERMISSION_LIST_DEFAULT_LIMIT = 100
const USER_PERMISSION_LIST_MAX_LIMIT = 300
/** Max rows fetched before in-memory sort (pagination applied after sort). */
const USER_PERMISSION_SORT_FETCH_CAP = 10000

type LineUserPermissionSortable = {
  display_name?: string | null
  line_user_id?: string | null
}

function sortLineUserPermissionsForAdminDisplay<T extends LineUserPermissionSortable>(rows: T[]): T[] {
  const collator = new Intl.Collator("ja", { sensitivity: "base" })
  return [...rows].sort((a, b) => {
    const sa = String(a.display_name ?? "").trim() || String(a.line_user_id ?? "")
    const sb = String(b.display_name ?? "").trim() || String(b.line_user_id ?? "")
    return collator.compare(sa, sb)
  })
}

type MediaUsageStats = {
  total_files: number
  total_bytes: number
}
type GmailLinkedAccountState = {
  enabled: boolean
  configured: boolean
  email_address: string | null
  history_id: string | null
  checked_at: string
  error: string | null
}
type PdfJsTextItem = {
  str?: unknown
}
type PdfJsTextContent = {
  items?: unknown
}
type PdfJsPage = {
  getTextContent: () => Promise<PdfJsTextContent>
  cleanup?: () => void
}
type PdfJsDocument = {
  numPages: number
  getPage: (pageNumber: number) => Promise<PdfJsPage>
  cleanup?: () => void
  destroy?: () => void | Promise<void>
}
type PdfJsLoadingTask = {
  promise: Promise<PdfJsDocument>
  destroy?: () => void | Promise<void>
}
type PdfJsModule = {
  getDocument: (source: Record<string, unknown>) => PdfJsLoadingTask
}
type OfficeZipEntry = {
  name: string
  dir: boolean
  async: (type: "string") => Promise<string>
}

let cachedPdfJsModulePromise: Promise<PdfJsModule | null> | null = null

Deno.serve(async (req) => {
  if (req.method === "OPTIONS") {
    return new Response("ok", { headers: corsHeaders })
  }

  const url = new URL(req.url)
  const path = normalizePath(url.pathname)
  const clientIp = extractClientIp(req.headers)

  const supabaseUrl = Deno.env.get("SUPABASE_URL") ?? ""
  const serviceRoleKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") ?? ""
  if (!supabaseUrl || !serviceRoleKey) {
    return json({ error: "SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY is missing." }, 500)
  }

  const supabase = createClient(supabaseUrl, serviceRoleKey)
  const rateLimit = resolveAdminRateLimit(req.method, path)
  const rateLimitResult = await consumeRateLimitFromDb(
    supabase,
    `${clientIp}:${req.method}:${path}`,
    rateLimit.maxRequests,
    rateLimit.windowMs,
  )
  if (!rateLimitResult.allowed) {
    return json({
      error: "Too many requests. Please retry later.",
      code: "rate_limited",
      retry_after_ms: rateLimitResult.retryAfterMs,
    }, 429)
  }

  const fallbackAdminToken = Deno.env.get("ADMIN_DASHBOARD_TOKEN") ?? ""
  const authResult = await authenticate(req, supabase, fallbackAdminToken)
  if (!authResult.ok) {
    return json({ error: authResult.message }, authResult.status)
  }

  try {
    if (req.method === "GET" && path === "/state") {
      const state = await fetchState(supabase, url)
      return json(state, 200)
    }

    if (req.method === "GET" && path === "/permissions/users") {
      const users = await fetchLineUserPermissions(supabase, url)
      return json(users, 200)
    }

    if (req.method === "GET" && path === "/gmail/account") {
      const gmailAccount = await fetchGmailLinkedAccountState()
      return json({ gmail_account: gmailAccount }, 200)
    }

    if (req.method === "GET" && path === "/media") {
      const mediaState = await fetchMediaState(supabase, url)
      return json(mediaState, 200)
    }

    if (req.method === "GET" && path === "/documents") {
      const documentState = await fetchDocumentState(supabase, url)
      return json(documentState, 200)
    }

    if (req.method === "POST" && path === "/documents") {
      const created = await uploadDocumentFile(req, supabase)
      return json({ success: true, document: created }, 200)
    }

    const permissionPath = parseDocumentPermissionPath(path)
    if (permissionPath != null) {
      if (req.method === "GET") {
        const permissionState = await fetchDocumentPermissionStateById(supabase, permissionPath)
        return json({ success: true, permissions: permissionState }, 200)
      }
      if (req.method === "PUT") {
        const body = await parseJson(req)
        if (!isRecord(body)) {
          throw { status: 400, message: "Invalid JSON body." } satisfies AppError
        }
        const permissionState = await updateDocumentPermissionStateById(supabase, permissionPath, body)
        return json({ success: true, permissions: permissionState }, 200)
      }
    }

    if (req.method === "PUT" && path === "/settings/media-upload-limit") {
      const body = await parseJson(req)
      if (!isRecord(body)) {
        throw { status: 400, message: "Invalid JSON body." } satisfies AppError
      }
      const maxMb = normalizeMediaUploadMaxMb(body.media_upload_max_mb)
      await fetchGlobalSettings(supabase)
      const updatedAt = new Date().toISOString()
      const { data, error } = await supabase
        .from("summary_settings")
        .update({
          media_upload_max_mb: maxMb,
          updated_at: updatedAt,
        })
        .eq("id", 1)
        .select("id, media_upload_max_mb, updated_at")
        .single()
      if (error) {
        throw { status: 500, message: `Failed to update media upload limit: ${error.message}` } satisfies AppError
      }
      return json({
        success: true,
        media_upload_max_mb: Number(data?.media_upload_max_mb ?? maxMb),
        updated_at: data?.updated_at ?? updatedAt,
      }, 200)
    }

    if (req.method === "DELETE" && path.startsWith("/media/")) {
      const mediaIdRaw = path.replace("/media/", "")
      const mediaId = Number(mediaIdRaw)
      if (!Number.isInteger(mediaId) || mediaId <= 0) {
        throw { status: 400, message: "media_id must be a positive integer." } satisfies AppError
      }
      const deleted = await deleteMediaItemById(supabase, mediaId)
      return json({ success: true, deleted }, 200)
    }

    if (req.method === "DELETE" && path.startsWith("/documents/")) {
      const documentIdRaw = path.replace("/documents/", "")
      const documentId = Number(documentIdRaw)
      if (!Number.isInteger(documentId) || documentId <= 0) {
        throw { status: 400, message: "document_id must be a positive integer." } satisfies AppError
      }
      const deleted = await deleteDocumentById(supabase, documentId)
      return json({ success: true, deleted }, 200)
    }

    if (req.method === "PUT" && path === "/auth/token") {
      const body = await parseJson(req)
      if (!isRecord(body)) {
        throw { status: 400, message: "Invalid JSON body." } satisfies AppError
      }

      const newToken = String(body.new_token ?? "").trim()
      if (newToken.length < 8) {
        throw { status: 400, message: "new_token must be at least 8 characters." } satisfies AppError
      }

      await fetchGlobalSettings(supabase)
      const tokenHash = await hashToken(newToken)
      const updatedAt = new Date().toISOString()
      const { error } = await supabase
        .from("summary_settings")
        .update({
          admin_dashboard_token_hash: tokenHash,
          admin_dashboard_token_updated_at: updatedAt,
          updated_at: updatedAt,
        })
        .eq("id", 1)

      if (error) {
        throw { status: 500, message: `Failed to update admin token: ${error.message}` } satisfies AppError
      }

      return json({
        success: true,
        token_updated_at: updatedAt,
      }, 200)
    }

    if (req.method === "PUT" && path === "/settings/global") {
      const body = await parseJson(req)
      const payload = buildGlobalSettingsPayload(body)
      const { data, error } = await supabase
        .from("summary_settings")
        .upsert({
          id: 1,
          delivery_hours: payload.delivery_hours,
          is_enabled: payload.is_enabled,
          message_cleanup_timing: payload.message_cleanup_timing,
          last_delivery_summary_mode: payload.last_delivery_summary_mode,
          message_retention_days: payload.message_retention_days,
          calendar_tomorrow_reminder_enabled: payload.calendar_tomorrow_reminder_enabled,
          calendar_tomorrow_reminder_hours: payload.calendar_tomorrow_reminder_hours,
          calendar_tomorrow_reminder_only_if_events: payload.calendar_tomorrow_reminder_only_if_events,
          calendar_tomorrow_reminder_max_items: payload.calendar_tomorrow_reminder_max_items,
          ...(payload.media_upload_max_mb != null ? { media_upload_max_mb: payload.media_upload_max_mb } : {}),
          updated_at: new Date().toISOString(),
        }, { onConflict: "id" })
        .select("id, delivery_hours, is_enabled, message_cleanup_timing, last_delivery_summary_mode, message_retention_days, calendar_tomorrow_reminder_enabled, calendar_tomorrow_reminder_hours, calendar_tomorrow_reminder_only_if_events, calendar_tomorrow_reminder_max_items, media_upload_max_mb, updated_at")
        .single()

      if (error) {
        throw { status: 500, message: `Failed to update global settings: ${error.message}` } satisfies AppError
      }
      return json({ global_settings: data }, 200)
    }

    if (req.method === "PUT" && path === "/settings/rooms") {
      const body = await parseJson(req)
      const payload = buildRoomSettingsPayload(body)
      const { data, error } = await supabase
        .from("room_summary_settings")
        .upsert({
          room_id: payload.room_id,
          room_name: payload.room_name,
          is_enabled: payload.is_enabled,
          bot_reply_enabled: payload.bot_reply_enabled,
          send_room_summary: payload.send_room_summary,
          calendar_tomorrow_reminder_enabled: payload.calendar_tomorrow_reminder_enabled,
          calendar_ai_auto_create_enabled: payload.calendar_ai_auto_create_enabled,
          calendar_silent_auto_register_enabled: payload.calendar_silent_auto_register_enabled,
          message_search_enabled: payload.message_search_enabled,
          message_search_library_enabled: payload.message_search_library_enabled,
          media_file_access_enabled: payload.media_file_access_enabled,
          gmail_reservation_alert_enabled: payload.gmail_reservation_alert_enabled,
          room_sort_order: payload.room_sort_order,
          delivery_hours: payload.delivery_hours,
          message_cleanup_timing: payload.message_cleanup_timing,
          last_delivery_summary_mode: payload.last_delivery_summary_mode,
          updated_at: new Date().toISOString(),
        }, { onConflict: "room_id" })
        .select("room_id, room_name, delivery_hours, is_enabled, bot_reply_enabled, send_room_summary, calendar_tomorrow_reminder_enabled, calendar_ai_auto_create_enabled, calendar_silent_auto_register_enabled, message_search_enabled, message_search_library_enabled, media_file_access_enabled, gmail_reservation_alert_enabled, room_sort_order, message_cleanup_timing, last_delivery_summary_mode, updated_at")
        .single()

      if (error) {
        throw { status: 500, message: `Failed to update room settings: ${error.message}` } satisfies AppError
      }
      return json({ room_settings: data }, 200)
    }

    if (req.method === "PUT" && path === "/permissions/users") {
      const body = await parseJson(req)
      const payload = buildLineUserPermissionPayload(body)
      const { data, error } = await supabase
        .from("line_user_permissions")
        .upsert({
          line_user_id: payload.line_user_id,
          display_name: payload.display_name,
          is_active: payload.is_active,
          can_message_search: payload.can_message_search,
          can_library_search: payload.can_library_search,
          can_calendar_create: payload.can_calendar_create,
          can_calendar_update: payload.can_calendar_update,
          can_media_access: payload.can_media_access,
          excluded_message_search_room_ids: payload.excluded_message_search_room_ids,
          assigned_store: payload.assigned_store,
          assigned_job_title: payload.assigned_job_title,
          updated_at: new Date().toISOString(),
        }, { onConflict: "line_user_id" })
        .select("line_user_id, display_name, is_active, can_message_search, can_library_search, can_calendar_create, can_calendar_update, can_media_access, excluded_message_search_room_ids, assigned_store, assigned_job_title, updated_at")
        .single()
      if (error) {
        throw { status: 500, message: `Failed to upsert line user permission: ${error.message}` } satisfies AppError
      }
      return json({ user_permission: data }, 200)
    }

    if (req.method === "POST" && path === "/permissions/users/backfill") {
      const result = await backfillLineUserPermissionsFromMessages(supabase)
      return json({ success: true, backfill: result }, 200)
    }

    if (req.method === "POST" && path === "/rooms/refresh-names") {
      const body = await parseJson(req).catch(() => ({}))
      const roomId = isRecord(body) ? String(body.room_id ?? "").trim() : ""
      const result = await refreshRoomNamesFromLine(supabase, roomId || null)
      return json({ success: true, refresh: result }, 200)
    }

    if (req.method === "DELETE" && path.startsWith("/settings/rooms/")) {
      const roomId = decodeURIComponent(path.replace("/settings/rooms/", ""))
      if (!roomId) {
        throw { status: 400, message: "room_id is required." } satisfies AppError
      }

      const { error } = await supabase
        .from("room_summary_settings")
        .delete()
        .eq("room_id", roomId)

      if (error) {
        throw { status: 500, message: `Failed to delete room settings: ${error.message}` } satisfies AppError
      }
      return json({ success: true, room_id: roomId }, 200)
    }

    if (req.method === "DELETE" && path.startsWith("/permissions/users/")) {
      const lineUserId = decodeURIComponent(path.replace("/permissions/users/", "")).trim()
      if (!lineUserId) {
        throw { status: 400, message: "line_user_id is required." } satisfies AppError
      }
      const { error } = await supabase
        .from("line_user_permissions")
        .delete()
        .eq("line_user_id", lineUserId)
      if (error) {
        throw { status: 500, message: `Failed to delete line user permission: ${error.message}` } satisfies AppError
      }
      return json({ success: true, line_user_id: lineUserId }, 200)
    }

    if (req.method === "DELETE" && path.startsWith("/rooms/")) {
      const roomId = decodeURIComponent(path.replace("/rooms/", ""))
      if (!roomId) {
        throw { status: 400, message: "room_id is required." } satisfies AppError
      }

      const mediaCleanup = await removeRoomMediaObjects(supabase, roomId)
      if (!mediaCleanup.ok) {
        throw { status: 500, message: mediaCleanup.message } satisfies AppError
      }
      const documentCleanup = await removeRoomDocuments(supabase, roomId)
      if (!documentCleanup.ok) {
        throw { status: 500, message: documentCleanup.message } satisfies AppError
      }

      const { count: messageCount, error: messageCountError } = await supabase
        .from("line_messages")
        .select("id", { count: "exact", head: true })
        .eq("room_id", roomId)
      if (messageCountError) {
        throw { status: 500, message: `Failed to count room messages: ${messageCountError.message}` } satisfies AppError
      }

      const { error: messageDeleteError } = await supabase
        .from("line_messages")
        .delete()
        .eq("room_id", roomId)
      if (messageDeleteError) {
        throw { status: 500, message: `Failed to delete room messages: ${messageDeleteError.message}` } satisfies AppError
      }

      const { data: roomSettingsRow, error: roomSettingsCountError } = await supabase
        .from("room_summary_settings")
        .select("room_id")
        .eq("room_id", roomId)
        .maybeSingle()
      if (roomSettingsCountError) {
        throw { status: 500, message: `Failed to inspect room settings: ${roomSettingsCountError.message}` } satisfies AppError
      }

      const { error: roomSettingsDeleteError } = await supabase
        .from("room_summary_settings")
        .delete()
        .eq("room_id", roomId)
      if (roomSettingsDeleteError) {
        throw { status: 500, message: `Failed to delete room settings: ${roomSettingsDeleteError.message}` } satisfies AppError
      }

      return json({
        success: true,
        room_id: roomId,
        deleted: {
          messages: messageCount ?? 0,
          media_files: mediaCleanup.deletedFiles,
          media_metadata: mediaCleanup.deletedMetadataRows,
          document_files: documentCleanup.deletedFiles,
          document_metadata: documentCleanup.deletedMetadataRows,
          room_settings: roomSettingsRow ? 1 : 0,
        },
      }, 200)
    }

    if (req.method === "POST" && path === "/actions/run-summary") {
      const body = await parseJson(req)
      if (!isRecord(body)) {
        throw { status: 400, message: "Invalid JSON body." } satisfies AppError
      }
      if (body.force != null && typeof body.force !== "boolean") {
        throw { status: 400, message: "force must be boolean when provided." } satisfies AppError
      }
      const forceRun = body.force == null ? true : body.force

      const { data: beforeLog } = await supabase
        .from("summary_delivery_logs")
        .select("id")
        .order("id", { ascending: false })
        .limit(1)
        .maybeSingle()
      const beforeId = beforeLog?.id ?? 0

      const { error: invokeError } = await supabase.rpc("invoke_summary_cron", { force_run: forceRun })
      if (invokeError) {
        throw { status: 500, message: `Failed to invoke summary cron: ${invokeError.message}` } satisfies AppError
      }

      const latestLog = await waitForNewLog(supabase, beforeId)
      if (!latestLog) {
        return json({
          success: true,
          queued: true,
          forced: forceRun,
          before_log_id: beforeId,
          latest_log: null,
          warning: "手動実行を受け付けました。ログ反映まで時間がかかっています。",
        }, 200)
      }

      return json({
        success: true,
        queued: true,
        forced: forceRun,
        before_log_id: beforeId,
        latest_log: {
          id: latestLog.id,
          run_at: latestLog.run_at,
          status: latestLog.status,
          reason: latestLog.reason,
        },
      }, 200)
    }

    return json({ error: "Not found." }, 404)
  } catch (e) {
    const err = asAppError(e)
    return json({ error: err.message }, err.status)
  }
})

async function authenticate(
  req: Request,
  supabase: ReturnType<typeof createClient>,
  fallbackToken: string,
): Promise<{ ok: true } | { ok: false; status: number; message: string }> {
  const provided = req.headers.get("x-admin-token") ?? ""
  if (!provided) {
    return { ok: false, status: 401, message: "Unauthorized." }
  }

  const dbHashResult = await getStoredAdminTokenHash(supabase)
  if (!dbHashResult.ok) {
    return dbHashResult
  }

  if (dbHashResult.hash) {
    const providedHash = await hashToken(provided)
    if (secureEqual(providedHash, dbHashResult.hash)) {
      return { ok: true }
    }
    // Break-glass path: allow fallback secret token even when DB hash exists.
    // This keeps recovery possible if the hashed dashboard token is lost.
    if (fallbackToken && secureEqual(provided, fallbackToken)) {
      return { ok: true }
    }
    return { ok: false, status: 401, message: "Unauthorized." }
  }

  if (!fallbackToken) {
    return { ok: false, status: 500, message: "ADMIN_DASHBOARD_TOKEN is not configured." }
  }

  if (!secureEqual(provided, fallbackToken)) {
    return { ok: false, status: 401, message: "Unauthorized." }
  }

  return { ok: true }
}

async function getStoredAdminTokenHash(
  supabase: ReturnType<typeof createClient>,
): Promise<{ ok: true; hash: string | null } | { ok: false; status: number; message: string }> {
  const { data, error } = await supabase
    .from("summary_settings")
    .select("admin_dashboard_token_hash")
    .eq("id", 1)
    .maybeSingle()

  if (error) {
    return { ok: false, status: 500, message: `Failed to load admin token settings: ${error.message}` }
  }

  const hash = typeof data?.admin_dashboard_token_hash === "string"
    ? data.admin_dashboard_token_hash.trim()
    : ""
  return { ok: true, hash: hash || null }
}

async function hashToken(value: string): Promise<string> {
  const input = new TextEncoder().encode(value)
  const digest = await crypto.subtle.digest("SHA-256", input)
  return Array.from(new Uint8Array(digest))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("")
}

async function fetchState(
  supabase: ReturnType<typeof createClient>,
  url: URL,
) {
  const logsLimit = clampInt(url.searchParams.get("logs_limit"), 30, 10, 30)
  const logsFetchLimit = logsLimit * 8

  const globalSettings = await fetchGlobalSettings(supabase)
  const [roomSettingsRes, roomOverviewRes, logsRes, storageUsageState, userPermissionsRes] = await Promise.all([
    supabase
      .from("room_summary_settings")
      .select("room_id, room_name, delivery_hours, is_enabled, bot_reply_enabled, send_room_summary, calendar_tomorrow_reminder_enabled, calendar_ai_auto_create_enabled, calendar_silent_auto_register_enabled, message_search_enabled, message_search_library_enabled, media_file_access_enabled, gmail_reservation_alert_enabled, room_sort_order, message_cleanup_timing, last_delivery_summary_mode, updated_at")
      .order("updated_at", { ascending: false }),
    supabase.rpc("get_room_overview"),
    supabase
      .from("summary_delivery_logs")
      .select("id, run_at, jst_hour, status, reason, should_send_overall, rooms_targeted, messages_in_queue, messages_marked_processed, line_send_attempted, line_send_success, line_http_status, target_room_id, details")
      .order("id", { ascending: false })
      .limit(logsFetchLimit),
    fetchStorageUsageState(supabase),
    supabase
      .from("line_user_permissions")
      .select("line_user_id, display_name, is_active, can_message_search, can_library_search, can_calendar_create, can_calendar_update, can_media_access, excluded_message_search_room_ids, assigned_store, assigned_job_title, updated_at")
      .limit(USER_PERMISSION_SORT_FETCH_CAP),
  ])

  if (roomSettingsRes.error) {
    throw { status: 500, message: `Failed to fetch room settings: ${roomSettingsRes.error.message}` } satisfies AppError
  }
  if (roomOverviewRes.error) {
    throw { status: 500, message: `Failed to fetch room overview: ${roomOverviewRes.error.message}` } satisfies AppError
  }
  if (logsRes.error) {
    throw { status: 500, message: `Failed to fetch delivery logs: ${logsRes.error.message}` } satisfies AppError
  }
  if (userPermissionsRes.error) {
    throw { status: 500, message: `Failed to fetch user permissions: ${userPermissionsRes.error.message}` } satisfies AppError
  }

  const filteredLogs = (logsRes.data ?? [])
    .filter((row) => isActionableDeliveryLogStatus(row.status, row.details))
    .slice(0, logsLimit)

  return {
    global_settings: globalSettings,
    room_settings: roomSettingsRes.data ?? [],
    user_permissions: sortLineUserPermissionsForAdminDisplay(userPermissionsRes.data ?? []).slice(
      0,
      USER_PERMISSION_LIST_MAX_LIMIT,
    ),
    marugo_group_store_options: [...MARUGO_GROUP_STORE_OPTIONS],
    job_title_options: [...JOB_TITLE_OPTIONS],
    room_overview: roomOverviewRes.data ?? [],
    delivery_logs: filteredLogs,
    storage_usage: storageUsageState.stats,
    storage_usage_error: storageUsageState.error,
    generated_at: new Date().toISOString(),
  }
}

async function fetchLineUserPermissions(
  supabase: ReturnType<typeof createClient>,
  url: URL,
) {
  const limit = clampInt(url.searchParams.get("limit"), USER_PERMISSION_LIST_DEFAULT_LIMIT, 1, USER_PERMISSION_LIST_MAX_LIMIT)
  const offset = clampInt(url.searchParams.get("offset"), 0, 0, 1000000)
  const q = String(url.searchParams.get("q") ?? "").trim()

  let query = supabase
    .from("line_user_permissions")
    .select("line_user_id, display_name, is_active, can_message_search, can_library_search, can_calendar_create, can_calendar_update, can_media_access, excluded_message_search_room_ids, assigned_store, assigned_job_title, updated_at", { count: "exact" })
    .limit(USER_PERMISSION_SORT_FETCH_CAP)

  if (q) {
    const escaped = q.replaceAll("%", "\\%").replaceAll("_", "\\_")
    query = query.or(`line_user_id.ilike.%${escaped}%,display_name.ilike.%${escaped}%`)
  }

  const { data, error, count } = await query
  if (error) {
    throw { status: 500, message: `Failed to fetch user permissions: ${error.message}` } satisfies AppError
  }

  const rows = Array.isArray(data) ? data : []
  const sorted = sortLineUserPermissionsForAdminDisplay(rows)
  const paged = sorted.slice(offset, offset + limit)
  const safeTotal = Number.isFinite(Number(count)) ? Number(count) : sorted.length
  const nextOffset = offset + paged.length
  return {
    items: paged,
    total: safeTotal,
    limit,
    offset,
    has_more: nextOffset < safeTotal,
    next_offset: nextOffset < safeTotal ? nextOffset : null,
    generated_at: new Date().toISOString(),
  }
}

async function backfillLineUserPermissionsFromMessages(
  supabase: ReturnType<typeof createClient>,
) {
  const pageSize = 1000
  const userIds = new Set<string>()
  const latestRoomIdByUserId = new Map<string, string>()
  let offset = 0

  while (true) {
    const { data, error } = await supabase
      .from("line_messages")
      .select("user_id, room_id, id")
      .not("user_id", "is", null)
      .neq("user_id", "")
      .order("id", { ascending: false })
      .range(offset, offset + pageSize - 1)
    if (error) {
      throw { status: 500, message: `Failed to scan line_messages users: ${error.message}` } satisfies AppError
    }
    const rows = Array.isArray(data) ? data : []
    if (rows.length === 0) break
    for (const row of rows) {
      const userId = String((row as { user_id?: unknown })?.user_id ?? "").trim()
      if (!userId) continue
      userIds.add(userId)
      if (!latestRoomIdByUserId.has(userId)) {
        const roomId = String((row as { room_id?: unknown })?.room_id ?? "").trim()
        if (roomId) latestRoomIdByUserId.set(userId, roomId)
      }
    }
    if (rows.length < pageSize) break
    offset += pageSize
  }

  // Include users that already exist in permission table (for display-name recovery).
  offset = 0
  while (true) {
    const { data, error } = await supabase
      .from("line_user_permissions")
      .select("line_user_id")
      .range(offset, offset + pageSize - 1)
    if (error) {
      throw { status: 500, message: `Failed to scan line_user_permissions users: ${error.message}` } satisfies AppError
    }
    const rows = Array.isArray(data) ? data : []
    if (rows.length === 0) break
    for (const row of rows) {
      const lineUserId = String((row as { line_user_id?: unknown })?.line_user_id ?? "").trim()
      if (lineUserId) userIds.add(lineUserId)
    }
    if (rows.length < pageSize) break
    offset += pageSize
  }

  const allUserIds = Array.from(userIds)
  if (allUserIds.length === 0) {
    return {
      scanned_user_ids: 0,
      inserted: 0,
      already_existing: 0,
      display_name_updated: 0,
    }
  }

  const existingNameByUserId = new Map<string, string | null>()
  for (let i = 0; i < allUserIds.length; i += pageSize) {
    const chunk = allUserIds.slice(i, i + pageSize)
    const { data: existingRows, error: existingError } = await supabase
      .from("line_user_permissions")
      .select("line_user_id, display_name")
      .in("line_user_id", chunk)
    if (existingError) {
      throw { status: 500, message: `Failed to inspect existing user permissions: ${existingError.message}` } satisfies AppError
    }
    for (const row of existingRows ?? []) {
      const lineUserId = String((row as { line_user_id?: unknown })?.line_user_id ?? "").trim()
      if (!lineUserId) continue
      const displayName = String((row as { display_name?: unknown })?.display_name ?? "").trim()
      existingNameByUserId.set(lineUserId, displayName || null)
    }
  }

  const existingSet = new Set(Array.from(existingNameByUserId.keys()))
  const lineAccessToken = String(Deno.env.get("LINE_CHANNEL_ACCESS_TOKEN") ?? "").trim()
  const idsNeedingDisplayName = allUserIds.filter((lineUserId) => {
    const displayName = existingNameByUserId.get(lineUserId)
    return !displayName
  })
  const fetchedDisplayNameByUserId = lineAccessToken
    ? await fetchLineDisplayNamesByUserIds(idsNeedingDisplayName, latestRoomIdByUserId, lineAccessToken)
    : new Map<string, string>()

  const now = new Date().toISOString()
  const inserts = allUserIds
    .filter((id) => !existingSet.has(id))
    .map((lineUserId) => ({
      line_user_id: lineUserId,
      display_name: fetchedDisplayNameByUserId.get(lineUserId) ?? null,
      is_active: true,
      can_message_search: true,
      can_library_search: true,
      can_calendar_create: true,
      can_calendar_update: true,
      can_media_access: true,
      assigned_store: null,
      assigned_job_title: null,
      updated_at: now,
    }))

  if (inserts.length > 0) {
    const { error: insertError } = await supabase
      .from("line_user_permissions")
      .insert(inserts, { defaultToNull: false, ignoreDuplicates: true })
    if (insertError) {
      throw { status: 500, message: `Failed to backfill user permissions: ${insertError.message}` } satisfies AppError
    }
  }

  let displayNameUpdated = 0
  const existingNeedDisplayNameIds = allUserIds.filter((lineUserId) => {
    if (!existingSet.has(lineUserId)) return false
    const currentName = existingNameByUserId.get(lineUserId)
    return !currentName && !!fetchedDisplayNameByUserId.get(lineUserId)
  })
  for (const lineUserId of existingNeedDisplayNameIds) {
    const displayName = fetchedDisplayNameByUserId.get(lineUserId)
    if (!displayName) continue
    const { error: updateError } = await supabase
      .from("line_user_permissions")
      .update({
        display_name: displayName,
        updated_at: new Date().toISOString(),
      })
      .eq("line_user_id", lineUserId)
    if (updateError) {
      throw { status: 500, message: `Failed to update display_name for ${lineUserId}: ${updateError.message}` } satisfies AppError
    }
    displayNameUpdated += 1
  }

  return {
    scanned_user_ids: allUserIds.length,
    inserted: inserts.length,
    already_existing: allUserIds.length - inserts.length,
    display_name_updated: displayNameUpdated,
  }
}

async function fetchLineDisplayNamesByUserIds(
  lineUserIds: string[],
  latestRoomIdByUserId: Map<string, string>,
  lineAccessToken: string,
): Promise<Map<string, string>> {
  const result = new Map<string, string>()
  for (const lineUserId of lineUserIds) {
    const userId = String(lineUserId ?? "").trim()
    if (!userId) continue
    const roomId = String(latestRoomIdByUserId.get(userId) ?? "").trim()
    const displayName = await fetchLineDisplayNameByUserId(userId, roomId, lineAccessToken)
    if (displayName) {
      result.set(userId, displayName)
    }
  }
  return result
}

async function fetchLineDisplayNameByUserId(
  lineUserId: string,
  roomId: string,
  lineAccessToken: string,
): Promise<string | null> {
  if (roomId.startsWith("C")) {
    const byGroupMember = await fetchLineDisplayNameByUrl(
      `https://api.line.me/v2/bot/group/${encodeURIComponent(roomId)}/member/${encodeURIComponent(lineUserId)}`,
      lineAccessToken,
    )
    if (byGroupMember) return byGroupMember
  } else if (roomId.startsWith("R")) {
    const byRoomMember = await fetchLineDisplayNameByUrl(
      `https://api.line.me/v2/bot/room/${encodeURIComponent(roomId)}/member/${encodeURIComponent(lineUserId)}`,
      lineAccessToken,
    )
    if (byRoomMember) return byRoomMember
  }

  return await fetchLineDisplayNameByUrl(
    `https://api.line.me/v2/bot/profile/${encodeURIComponent(lineUserId)}`,
    lineAccessToken,
  )
}

async function fetchLineDisplayNameByUrl(
  url: string,
  lineAccessToken: string,
): Promise<string | null> {
  try {
    const response = await fetch(url, {
      method: "GET",
      headers: {
        Authorization: `Bearer ${lineAccessToken}`,
      },
    })
    if (!response.ok) {
      return null
    }
    const body = await response.json()
    const displayName = String(body?.displayName ?? "").trim()
    return displayName || null
  } catch {
    return null
  }
}

async function refreshRoomNamesFromLine(
  supabase: ReturnType<typeof createClient>,
  roomId: string | null,
) {
  const lineAccessToken = String(Deno.env.get("LINE_CHANNEL_ACCESS_TOKEN") ?? "").trim()
  if (!lineAccessToken) {
    throw { status: 500, message: "LINE_CHANNEL_ACCESS_TOKEN is not set." } satisfies AppError
  }

  const roomIds = new Set<string>()
  if (roomId) {
    roomIds.add(roomId)
  } else {
    const { data, error } = await supabase.rpc("get_room_overview")
    if (error) {
      throw { status: 500, message: `Failed to fetch room overview for name refresh: ${error.message}` } satisfies AppError
    }
    const rows = Array.isArray(data) ? data : []
    for (const row of rows) {
      const id = String((row as { room_id?: unknown })?.room_id ?? "").trim()
      if (id) roomIds.add(id)
    }
  }

  let refreshed = 0
  let attempted = 0
  let notFound = 0
  const now = new Date().toISOString()
  for (const id of roomIds) {
    attempted += 1
    const name = await fetchLineConversationNameByRoomId(id, lineAccessToken)
    if (!name) {
      notFound += 1
      continue
    }

    const { error: settingsError } = await supabase
      .from("room_summary_settings")
      .update({
        room_name: name,
        updated_at: now,
      })
      .eq("room_id", id)
    if (settingsError) {
      throw { status: 500, message: `Failed to refresh room name in settings (${id}): ${settingsError.message}` } satisfies AppError
    }

    const { error: cacheError } = await supabase
      .from("line_room_names")
      .upsert({
        room_id: id,
        room_name: name,
        updated_at: now,
      }, { onConflict: "room_id" })
    if (cacheError) {
      throw { status: 500, message: `Failed to refresh room name in cache (${id}): ${cacheError.message}` } satisfies AppError
    }

    refreshed += 1
  }

  return {
    attempted,
    refreshed,
    not_found: notFound,
    generated_at: now,
  }
}

async function fetchLineConversationNameByRoomId(
  roomId: string,
  lineAccessToken: string,
): Promise<string | null> {
  const normalizedRoomId = String(roomId ?? "").trim()
  if (!normalizedRoomId) return null

  if (normalizedRoomId.startsWith("C")) {
    return await fetchLineConversationNameByUrl(
      `https://api.line.me/v2/bot/group/${encodeURIComponent(normalizedRoomId)}/summary`,
      lineAccessToken,
      "groupName",
    )
  }
  if (normalizedRoomId.startsWith("R")) {
    return await fetchLineConversationNameByUrl(
      `https://api.line.me/v2/bot/room/${encodeURIComponent(normalizedRoomId)}/summary`,
      lineAccessToken,
      "roomName",
    )
  }
  if (normalizedRoomId.startsWith("U")) {
    return await fetchLineConversationNameByUrl(
      `https://api.line.me/v2/bot/profile/${encodeURIComponent(normalizedRoomId)}`,
      lineAccessToken,
      "displayName",
    )
  }
  return null
}

async function fetchLineConversationNameByUrl(
  url: string,
  lineAccessToken: string,
  key: "groupName" | "roomName" | "displayName",
): Promise<string | null> {
  try {
    const response = await fetch(url, {
      method: "GET",
      headers: {
        Authorization: `Bearer ${lineAccessToken}`,
      },
    })
    if (!response.ok) {
      return null
    }
    const body = await response.json()
    const value = String(body?.[key] ?? "").trim()
    return value || null
  } catch {
    return null
  }
}

async function fetchGmailLinkedAccountState(): Promise<GmailLinkedAccountState> {
  const checkedAt = new Date().toISOString()
  const clientId = String(Deno.env.get("GMAIL_CLIENT_ID") ?? "").trim()
  const clientSecret = String(Deno.env.get("GMAIL_CLIENT_SECRET") ?? "").trim()
  const refreshToken = String(Deno.env.get("GMAIL_REFRESH_TOKEN") ?? "").trim()

  const hasAnyCredential = !!clientId || !!clientSecret || !!refreshToken
  const enabled = parseBooleanEnv(Deno.env.get("GMAIL_ALERT_ENABLED"), hasAnyCredential)
  const configured = !!clientId && !!clientSecret && !!refreshToken

  if (!enabled) {
    return {
      enabled: false,
      configured,
      email_address: null,
      history_id: null,
      checked_at: checkedAt,
      error: null,
    }
  }

  if (!configured) {
    const missing: string[] = []
    if (!clientId) missing.push("GMAIL_CLIENT_ID")
    if (!clientSecret) missing.push("GMAIL_CLIENT_SECRET")
    if (!refreshToken) missing.push("GMAIL_REFRESH_TOKEN")
    return {
      enabled: true,
      configured: false,
      email_address: null,
      history_id: null,
      checked_at: checkedAt,
      error: `Missing Gmail secrets: ${missing.join(", ")}`,
    }
  }

  const tokenState = await fetchGmailAccessTokenByRefreshToken(clientId, clientSecret, refreshToken)
  if (!tokenState.ok) {
    return {
      enabled: true,
      configured: true,
      email_address: null,
      history_id: null,
      checked_at: checkedAt,
      error: tokenState.error,
    }
  }

  const profileState = await fetchGmailProfile(tokenState.accessToken)
  if (!profileState.ok) {
    return {
      enabled: true,
      configured: true,
      email_address: null,
      history_id: null,
      checked_at: checkedAt,
      error: profileState.error,
    }
  }

  return {
    enabled: true,
    configured: true,
    email_address: profileState.emailAddress,
    history_id: profileState.historyId,
    checked_at: checkedAt,
    error: null,
  }
}

async function fetchGmailAccessTokenByRefreshToken(
  clientId: string,
  clientSecret: string,
  refreshToken: string,
): Promise<{ ok: true; accessToken: string } | { ok: false; error: string }> {
  try {
    const body = new URLSearchParams()
    body.set("client_id", clientId)
    body.set("client_secret", clientSecret)
    body.set("refresh_token", refreshToken)
    body.set("grant_type", "refresh_token")

    const response = await fetch("https://oauth2.googleapis.com/token", {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: body.toString(),
    })
    const text = await response.text()
    if (!response.ok) {
      return {
        ok: false,
        error: `Gmail token取得エラー (${response.status}): ${extractGoogleApiErrorMessage(text)}`,
      }
    }

    const data = parseJsonObjectSafe(text)
    const accessToken = typeof data?.access_token === "string" ? data.access_token.trim() : ""
    if (!accessToken) {
      return { ok: false, error: "Gmail token取得エラー: access_token が空です。" }
    }
    return { ok: true, accessToken }
  } catch (error) {
    return {
      ok: false,
      error: `Gmail token取得エラー: ${sanitizeSingleLine(error instanceof Error ? error.message : String(error))}`,
    }
  }
}

async function fetchGmailProfile(
  accessToken: string,
): Promise<{ ok: true; emailAddress: string | null; historyId: string | null } | { ok: false; error: string }> {
  try {
    const response = await fetch("https://gmail.googleapis.com/gmail/v1/users/me/profile", {
      method: "GET",
      headers: {
        "Authorization": `Bearer ${accessToken}`,
        "Content-Type": "application/json",
      },
    })
    const text = await response.text()
    if (!response.ok) {
      return {
        ok: false,
        error: `Gmail profile取得エラー (${response.status}): ${extractGoogleApiErrorMessage(text)}`,
      }
    }

    const data = parseJsonObjectSafe(text)
    const emailAddress = typeof data?.emailAddress === "string" ? data.emailAddress.trim() : ""
    const historyId = data?.historyId == null ? "" : String(data.historyId).trim()
    return {
      ok: true,
      emailAddress: emailAddress || null,
      historyId: historyId || null,
    }
  } catch (error) {
    return {
      ok: false,
      error: `Gmail profile取得エラー: ${sanitizeSingleLine(error instanceof Error ? error.message : String(error))}`,
    }
  }
}

function extractGoogleApiErrorMessage(responseText: string): string {
  const raw = String(responseText ?? "").trim()
  if (!raw) return "unknown error"
  const parsed = parseJsonObjectSafe(raw)
  const nestedMessage = parsed?.error?.message
  if (typeof nestedMessage === "string" && nestedMessage.trim()) {
    return sanitizeSingleLine(nestedMessage)
  }
  const description = parsed?.error_description
  if (typeof description === "string" && description.trim()) {
    return sanitizeSingleLine(description)
  }
  return sanitizeSingleLine(raw)
}

function parseJsonObjectSafe(value: string): Record<string, any> | null {
  try {
    const parsed = JSON.parse(value)
    return isRecord(parsed) ? parsed : null
  } catch {
    return null
  }
}

function sanitizeSingleLine(value: string): string {
  return String(value ?? "")
    .replace(/\s+/g, " ")
    .trim()
}

async function fetchMediaState(
  supabase: ReturnType<typeof createClient>,
  url: URL,
) {
  const limit = clampInt(url.searchParams.get("limit"), MEDIA_LIST_DEFAULT_LIMIT, 1, MEDIA_LIST_MAX_LIMIT)
  const offset = clampInt(url.searchParams.get("offset"), 0, 0, 1000000)
  const roomId = String(url.searchParams.get("room_id") ?? "").trim()
  const mediaType = normalizeMediaType(url.searchParams.get("media_type"))

  let query = supabase
    .from("line_message_media")
    .select(
      "id, message_id, line_message_id, room_id, user_id, media_type, storage_bucket, storage_path, original_file_name, mime_type, file_size_bytes, created_at",
      { count: "exact" },
    )
    .order("created_at", { ascending: false })
    .range(offset, offset + limit - 1)

  if (roomId) {
    query = query.eq("room_id", roomId)
  }
  if (mediaType) {
    query = query.eq("media_type", mediaType)
  }

  const [listRes, filteredUsageRes, allUsageRes, mediaUploadMaxMb] = await Promise.all([
    query,
    fetchLineMediaUsageStats(supabase, roomId || null, mediaType),
    fetchLineMediaUsageStats(supabase, null, null),
    fetchMediaUploadMaxMb(supabase),
  ])

  const { data, error, count } = listRes
  if (error) {
    throw { status: 500, message: `Failed to fetch media list: ${error.message}` } satisfies AppError
  }
  if (!filteredUsageRes.ok) {
    throw { status: 500, message: filteredUsageRes.message } satisfies AppError
  }
  if (!allUsageRes.ok) {
    throw { status: 500, message: allUsageRes.message } satisfies AppError
  }

  const rows = Array.isArray(data) ? data.map((item) => normalizeMediaListRow(item)).filter((item): item is MediaListRow => item !== null) : []
  const roomNameMap = await fetchRoomNameMapForIds(supabase, rows.map((row) => row.room_id))
  const mediaContextMap = await fetchMediaContextMap(supabase, rows)
  const items = await Promise.all(rows.map(async (row) => {
    const signedUrl = await createSignedMediaUrl(supabase, row.storage_bucket, row.storage_path)
    const downloadUrl = await createSignedMediaDownloadUrl(
      supabase,
      row.storage_bucket,
      row.storage_path,
      row.original_file_name ?? `${row.line_message_id}`,
    )
    const context = mediaContextMap.get(row.id) ?? null
    return {
      ...row,
      room_name: roomNameMap.get(row.room_id) ?? row.room_name ?? null,
      context_before_text: context?.before_text ?? null,
      context_before_at: context?.before_at ?? null,
      context_after_text: context?.after_text ?? null,
      context_after_at: context?.after_at ?? null,
      signed_url: signedUrl,
      download_url: downloadUrl ?? signedUrl,
      line_message_tag: formatLineMediaTag(row.line_message_id),
    }
  }))
  const safeTotal = Number.isFinite(Number(count)) ? Number(count) : items.length
  const nextOffset = offset + items.length
  return {
    items,
    total: safeTotal,
    total_file_bytes: filteredUsageRes.stats.total_bytes,
    total_file_count: filteredUsageRes.stats.total_files,
    all_file_bytes: allUsageRes.stats.total_bytes,
    all_file_count: allUsageRes.stats.total_files,
    media_storage_cap_bytes: MEDIA_STORAGE_CAP_BYTES,
    media_storage_usage_ratio: MEDIA_STORAGE_CAP_BYTES > 0
      ? Math.min(1, allUsageRes.stats.total_bytes / MEDIA_STORAGE_CAP_BYTES)
      : 0,
    media_upload_max_mb: mediaUploadMaxMb,
    limit,
    offset,
    has_more: nextOffset < safeTotal,
    next_offset: nextOffset < safeTotal ? nextOffset : null,
    generated_at: new Date().toISOString(),
  }
}

async function fetchDocumentState(
  supabase: ReturnType<typeof createClient>,
  url: URL,
) {
  const limit = clampInt(url.searchParams.get("limit"), DOCUMENT_LIST_DEFAULT_LIMIT, 1, DOCUMENT_LIST_MAX_LIMIT)
  const offset = clampInt(url.searchParams.get("offset"), 0, 0, 1000000)
  const roomId = String(url.searchParams.get("room_id") ?? "").trim()

  let query = supabase
    .from("line_search_documents")
    .select(
      "id, room_id, room_name, storage_bucket, storage_path, original_file_name, mime_type, file_size_bytes, extracted_text, source, created_at, updated_at",
      { count: "exact" },
    )
    .order("created_at", { ascending: false })
    .range(offset, offset + limit - 1)

  if (roomId) {
    query = query.eq("room_id", roomId)
  }

  const [listRes, filteredUsageRes, allUsageRes] = await Promise.all([
    query,
    fetchLineDocumentUsageStats(supabase, roomId || null),
    fetchLineDocumentUsageStats(supabase, null),
  ])
  const { data, error, count } = listRes
  if (error) {
    throw { status: 500, message: `Failed to fetch document list: ${error.message}` } satisfies AppError
  }
  if (!filteredUsageRes.ok) {
    throw { status: 500, message: filteredUsageRes.message } satisfies AppError
  }
  if (!allUsageRes.ok) {
    throw { status: 500, message: allUsageRes.message } satisfies AppError
  }

  const rows = Array.isArray(data)
    ? data.map((item) => normalizeDocumentListRow(item)).filter((item): item is DocumentListRow => item !== null)
    : []
  const roomNameMap = await fetchRoomNameMapForIds(
    supabase,
    rows.map((row) => row.room_id || "").filter((value) => value.length > 0),
  )
  const items = await Promise.all(rows.map(async (row) => {
    const signedUrl = await createSignedMediaDownloadUrl(
      supabase,
      row.storage_bucket,
      row.storage_path,
      row.original_file_name,
    )
    const normalizedRoomId = row.room_id || null
    const latestRoomName = normalizedRoomId ? (roomNameMap.get(normalizedRoomId) ?? "") : ""
    const normalizedRoomName = latestRoomName
      || (row.room_name ? String(row.room_name) : (normalizedRoomId || null))
    const snippet = buildDocumentSnippet(row.extracted_text, DOCUMENT_PREVIEW_MAX_CHARS)
    return {
      ...row,
      room_id: normalizedRoomId,
      room_name: normalizedRoomName,
      snippet,
      signed_url: signedUrl,
      has_extracted_text: row.extracted_text.length > 0,
      extracted_char_count: row.extracted_text.length,
    }
  }))
  const permissionSummaries = await fetchDocumentPermissionSummaries(supabase, rows.map((row) => row.id))

  const safeTotal = Number.isFinite(Number(count)) ? Number(count) : items.length
  const nextOffset = offset + items.length
  return {
    items: items.map((item) => {
      const summary = permissionSummaries.get(item.id)
      return {
        ...item,
        permission_mode: summary?.mode ?? "public",
        allowed_user_count: summary?.allowed_user_count ?? 0,
      }
    }),
    total: safeTotal,
    total_file_bytes: filteredUsageRes.stats.total_bytes,
    total_file_count: filteredUsageRes.stats.total_files,
    all_file_bytes: allUsageRes.stats.total_bytes,
    all_file_count: allUsageRes.stats.total_files,
    limit,
    offset,
    has_more: nextOffset < safeTotal,
    next_offset: nextOffset < safeTotal ? nextOffset : null,
    generated_at: new Date().toISOString(),
  }
}

async function fetchLineDocumentUsageStats(
  supabase: ReturnType<typeof createClient>,
  roomId: string | null,
): Promise<{ ok: true; stats: MediaUsageStats } | { ok: false; message: string }> {
  const { data, error } = await supabase.rpc("get_line_document_usage_stats", {
    filter_room_id: roomId,
  })
  if (error) {
    return { ok: false, message: `Failed to fetch document usage stats: ${error.message}` }
  }
  const row = Array.isArray(data) ? data[0] : null
  const totalFiles = toNonNegativeInteger((row as any)?.total_files)
  const totalBytes = toNonNegativeInteger((row as any)?.total_bytes)
  return {
    ok: true,
    stats: {
      total_files: totalFiles,
      total_bytes: totalBytes,
    },
  }
}

function normalizeDocumentListRow(value: unknown): DocumentListRow | null {
  if (!isRecord(value)) return null
  const idNum = Number(value.id)
  if (!Number.isFinite(idNum) || idNum <= 0) return null

  const storageBucket = toSafeString(value.storage_bucket)
  const storagePath = toSafeString(value.storage_path)
  const originalFileName = toSafeString(value.original_file_name)
  if (!storageBucket || !storagePath || !originalFileName) return null

  const mimeType = normalizeDocumentMimeType(String(value.mime_type ?? ""), originalFileName)
  if (!mimeType) return null

  return {
    id: Math.floor(idNum),
    room_id: value.room_id == null ? null : toSafeString(value.room_id) || null,
    room_name: value.room_name == null ? null : String(value.room_name),
    storage_bucket: storageBucket,
    storage_path: storagePath,
    original_file_name: originalFileName,
    mime_type: mimeType,
    file_size_bytes: toNonNegativeInteger(value.file_size_bytes),
    extracted_text: normalizeExtractedText(String(value.extracted_text ?? "")),
    source: toSafeString(value.source) || "manual_upload",
    created_at: String(value.created_at ?? ""),
    updated_at: String(value.updated_at ?? ""),
  }
}

async function uploadDocumentFile(
  req: Request,
  supabase: ReturnType<typeof createClient>,
) {
  const contentLength = Number(req.headers.get("content-length") ?? "")
  if (Number.isFinite(contentLength) && contentLength > DOCUMENT_UPLOAD_MAX_BYTES + 1024 * 1024) {
    throw { status: 400, message: "payload is too large." } satisfies AppError
  }

  let formData: FormData
  try {
    formData = await req.formData()
  } catch {
    throw {
      status: 400,
      message: "Upload request must be multipart/form-data.",
    } satisfies AppError
  }

  const fileValue = formData.get("file")
  if (!(fileValue instanceof File)) {
    throw { status: 400, message: "file is required." } satisfies AppError
  }
  if (!Number.isFinite(fileValue.size) || fileValue.size <= 0) {
    throw { status: 400, message: "file must not be empty." } satisfies AppError
  }
  if (fileValue.size >= DOCUMENT_UPLOAD_MAX_BYTES) {
    throw {
      status: 400,
      message: `file must be smaller than ${Math.floor(DOCUMENT_UPLOAD_MAX_BYTES / (1024 * 1024))}MB.`,
    } satisfies AppError
  }

  const roomIdRaw = toSafeString(formData.get("room_id"))
  const roomId = roomIdRaw || null
  let roomName = toSafeString(formData.get("room_name")) || null

  const originalFileName = sanitizeUploadFileName(fileValue.name || "document")
  const mimeType = normalizeDocumentMimeType(fileValue.type || "", originalFileName)
  if (!mimeType) {
    throw { status: 400, message: "Only TXT/PDF/DOCX/XLSX files can be uploaded." } satisfies AppError
  }

  const buffer = await fileValue.arrayBuffer()
  const bytes = new Uint8Array(buffer)
  const validationError = await validateDocumentPayloadSafety(bytes, mimeType)
  if (validationError) {
    throw { status: 400, message: validationError } satisfies AppError
  }
  const fallbackExtractedText = mimeType === "text/plain"
    ? tryDecodeText(bytes)
    : mimeType === "application/pdf"
      ? await extractPdfText(bytes)
      : mimeType === DOCX_MIME_TYPE
        ? await extractDocxText(bytes)
        : mimeType === XLSX_MIME_TYPE
          ? await extractXlsxText(bytes)
      : ""
  const extractedText = normalizeExtractedText(fallbackExtractedText)
  const nowIso = new Date().toISOString()
  const storagePath = buildDocumentStoragePath(roomId, originalFileName)

  const uploadRes = await supabase
    .storage
    .from(LINE_DOCUMENT_BUCKET)
    .upload(storagePath, bytes, {
      contentType: mimeType,
      upsert: false,
    })
  if (uploadRes.error) {
    throw { status: 500, message: `Failed to upload document: ${uploadRes.error.message}` } satisfies AppError
  }

  if (roomId && !roomName) {
    const { data: roomRow } = await supabase
      .from("room_summary_settings")
      .select("room_name")
      .eq("room_id", roomId)
      .maybeSingle()
    roomName = toSafeString(roomRow?.room_name) || null
  }

  const insertPayload = {
    room_id: roomId,
    room_name: roomName,
    storage_bucket: LINE_DOCUMENT_BUCKET,
    storage_path: storagePath,
    original_file_name: originalFileName,
    mime_type: mimeType,
    file_size_bytes: bytes.byteLength,
    extracted_text: extractedText,
    source: "manual_upload",
    created_at: nowIso,
    updated_at: nowIso,
  }
  const { data: inserted, error: insertError } = await supabase
    .from("line_search_documents")
    .insert(insertPayload)
    .select("id, room_id, room_name, storage_bucket, storage_path, original_file_name, mime_type, file_size_bytes, extracted_text, source, created_at, updated_at")
    .single()

  if (insertError) {
    await supabase.storage.from(LINE_DOCUMENT_BUCKET).remove([storagePath])
    throw { status: 500, message: `Failed to save document metadata: ${insertError.message}` } satisfies AppError
  }

  const normalized = normalizeDocumentListRow(inserted)
  if (!normalized) {
    await supabase.storage.from(LINE_DOCUMENT_BUCKET).remove([storagePath])
    throw { status: 500, message: "Saved document row is invalid." } satisfies AppError
  }

  const signedUrl = await createSignedMediaDownloadUrl(
    supabase,
    normalized.storage_bucket,
    normalized.storage_path,
    normalized.original_file_name,
  )
  return {
    ...normalized,
    snippet: buildDocumentSnippet(normalized.extracted_text, DOCUMENT_PREVIEW_MAX_CHARS),
    signed_url: signedUrl,
    has_extracted_text: normalized.extracted_text.length > 0,
    extracted_char_count: normalized.extracted_text.length,
  }
}

function normalizeDocumentMimeType(rawMimeType: string, fileName: string): DocumentMimeType | null {
  const normalizedMimeType = String(rawMimeType ?? "").split(";")[0].trim().toLowerCase()
  if (normalizedMimeType === "text/plain") return "text/plain"
  if (normalizedMimeType === "application/pdf") return "application/pdf"
  if (normalizedMimeType === DOCX_MIME_TYPE) return DOCX_MIME_TYPE
  if (normalizedMimeType === XLSX_MIME_TYPE) return XLSX_MIME_TYPE

  const ext = extractFileExt(fileName)
  if (ext === "txt" || ext === "log" || ext === "md" || ext === "csv") {
    return "text/plain"
  }
  if (ext === "pdf") return "application/pdf"
  if (ext === "docx") return DOCX_MIME_TYPE
  if (ext === "xlsx") return XLSX_MIME_TYPE
  return null
}

function parseDocumentPermissionPath(path: string): number | null {
  const match = path.match(/^\/documents\/(\d+)\/permissions$/)
  if (!match || !match[1]) return null
  const documentId = Number(match[1])
  if (!Number.isInteger(documentId) || documentId <= 0) return null
  return documentId
}

async function ensureDocumentExists(
  supabase: ReturnType<typeof createClient>,
  documentId: number,
): Promise<void> {
  const { data, error } = await supabase
    .from("line_search_documents")
    .select("id")
    .eq("id", documentId)
    .maybeSingle()
  if (error) {
    throw { status: 500, message: `Failed to fetch document row: ${error.message}` } satisfies AppError
  }
  if (!data?.id) {
    throw { status: 404, message: "Document not found." } satisfies AppError
  }
}

async function fetchDocumentPermissionStateById(
  supabase: ReturnType<typeof createClient>,
  documentId: number,
): Promise<DocumentViewerPermissionState> {
  await ensureDocumentExists(supabase, documentId)
  const { data, error } = await supabase
    .from("line_search_document_viewers")
    .select("line_user_id")
    .eq("document_id", documentId)
    .order("line_user_id", { ascending: true })
  if (error) {
    throw { status: 500, message: `Failed to fetch document permissions: ${error.message}` } satisfies AppError
  }
  const ids = Array.isArray(data)
    ? data.map((row) => toSafeString(row?.line_user_id)).filter((value) => value.length > 0)
    : []
  return {
    document_id: documentId,
    mode: ids.length > 0 ? "restricted" : "public",
    allowed_user_ids: ids,
  }
}

function normalizeDocumentPermissionUserIds(raw: unknown): string[] {
  if (!Array.isArray(raw)) return []
  const out: string[] = []
  const seen = new Set<string>()
  for (const value of raw) {
    const normalized = String(value ?? "").trim()
    if (!normalized || normalized.length > 191) continue
    if (seen.has(normalized)) continue
    seen.add(normalized)
    out.push(normalized)
    if (out.length >= 300) break
  }
  return out
}

async function updateDocumentPermissionStateById(
  supabase: ReturnType<typeof createClient>,
  documentId: number,
  body: Record<string, unknown>,
): Promise<DocumentViewerPermissionState> {
  await ensureDocumentExists(supabase, documentId)

  const modeRaw = String(body.mode ?? "").trim().toLowerCase()
  const requestedIds = normalizeDocumentPermissionUserIds(body.line_user_ids)
  const shouldRestrict = modeRaw === "restricted" || (modeRaw !== "public" && requestedIds.length > 0)
  if (shouldRestrict && requestedIds.length === 0) {
    throw { status: 400, message: "restricted mode requires line_user_ids." } satisfies AppError
  }

  const { error: deleteError } = await supabase
    .from("line_search_document_viewers")
    .delete()
    .eq("document_id", documentId)
  if (deleteError) {
    throw { status: 500, message: `Failed to clear existing document permissions: ${deleteError.message}` } satisfies AppError
  }

  if (shouldRestrict) {
    const rows = requestedIds.map((lineUserId) => ({
      document_id: documentId,
      line_user_id: lineUserId,
    }))
    const { error: insertError } = await supabase
      .from("line_search_document_viewers")
      .insert(rows)
    if (insertError) {
      throw { status: 500, message: `Failed to save document permissions: ${insertError.message}` } satisfies AppError
    }
  }

  return await fetchDocumentPermissionStateById(supabase, documentId)
}

async function fetchDocumentPermissionSummaries(
  supabase: ReturnType<typeof createClient>,
  documentIds: number[],
): Promise<Map<number, { mode: "public" | "restricted"; allowed_user_count: number }>> {
  const out = new Map<number, { mode: "public" | "restricted"; allowed_user_count: number }>()
  const ids = Array.from(new Set(documentIds.filter((value) => Number.isInteger(value) && value > 0)))
  if (ids.length === 0) return out

  const { data, error } = await supabase
    .from("line_search_document_viewers")
    .select("document_id, line_user_id")
    .in("document_id", ids)
  if (error) {
    console.error("Failed to fetch document permission summaries:", error.message)
    return out
  }

  const counts = new Map<number, number>()
  for (const row of Array.isArray(data) ? data : []) {
    const documentId = Number((row as any)?.document_id)
    if (!Number.isInteger(documentId) || documentId <= 0) continue
    counts.set(documentId, (counts.get(documentId) || 0) + 1)
  }
  for (const documentId of ids) {
    const count = counts.get(documentId) || 0
    out.set(documentId, {
      mode: count > 0 ? "restricted" : "public",
      allowed_user_count: count,
    })
  }
  return out
}

function sanitizeUploadFileName(value: string): string {
  const safe = String(value ?? "")
    .replace(/[\\/:*?"<>|]+/g, "_")
    .replace(/\s+/g, " ")
    .trim()
  if (!safe) return "document.txt"
  return safe.length <= 180 ? safe : safe.slice(0, 180).trimEnd()
}

function extractFileExt(fileName: string): string {
  const safe = sanitizeUploadFileName(fileName)
  const idx = safe.lastIndexOf(".")
  if (idx < 0 || idx === safe.length - 1) return ""
  return safe
    .slice(idx + 1)
    .toLowerCase()
    .replace(/[^a-z0-9]/g, "")
}

function buildDocumentStoragePath(roomId: string | null, originalFileName: string): string {
  const now = new Date()
  const y = now.getUTCFullYear()
  const m = String(now.getUTCMonth() + 1).padStart(2, "0")
  const d = String(now.getUTCDate()).padStart(2, "0")
  const roomSegment = sanitizeStoragePathSegment(roomId || "shared")
  const ext = extractFileExt(originalFileName) || "bin"
  const baseName = sanitizeStoragePathSegment(originalFileName.replace(/\.[^.]+$/, "") || "document")
  const docId = crypto.randomUUID()
  return `${y}/${m}/${d}/${roomSegment}/${docId}-${baseName}.${ext}`
}

function sanitizeStoragePathSegment(value: string): string {
  const cleaned = String(value ?? "")
    .trim()
    .replace(/[^a-zA-Z0-9._-]/g, "_")
  if (!cleaned) return "unknown"
  return cleaned.length > 120 ? cleaned.slice(0, 120) : cleaned
}

async function loadPdfJsModule(): Promise<PdfJsModule | null> {
  if (!cachedPdfJsModulePromise) {
    cachedPdfJsModulePromise = import(PDFJS_MODULE_URL)
      .then((mod) => {
        if (isRecord(mod) && typeof mod.getDocument === "function") {
          return mod as unknown as PdfJsModule
        }
        console.error("pdfjs module is invalid: getDocument is missing.")
        return null
      })
      .catch((error) => {
        console.error("Failed to load pdfjs module:", error)
        return null
      })
  }
  return await cachedPdfJsModulePromise
}

async function extractPdfText(bytes: Uint8Array): Promise<string> {
  const pdfjs = await loadPdfJsModule()
  if (!pdfjs) return ""

  let loadingTask: PdfJsLoadingTask | null = null
  let pdfDocument: PdfJsDocument | null = null
  try {
    loadingTask = pdfjs.getDocument({
      data: bytes,
      disableWorker: true,
      useSystemFonts: false,
      isEvalSupported: false,
      stopAtErrors: false,
    })
    pdfDocument = await loadingTask.promise

    const numPages = Number(pdfDocument.numPages || 0)
    if (!Number.isFinite(numPages) || numPages <= 0) return ""

    const pagesToRead = Math.min(numPages, DOCUMENT_PDF_EXTRACT_MAX_PAGES)
    const chunks: string[] = []
    let extractedChars = 0

    for (let pageNumber = 1; pageNumber <= pagesToRead; pageNumber += 1) {
      let page: PdfJsPage | null = null
      try {
        page = await pdfDocument.getPage(pageNumber)
        const textContent = await page.getTextContent()
        const rawItems = Array.isArray(textContent?.items) ? textContent.items : []
        const pageText = rawItems
          .map((item) => {
            if (!isRecord(item)) return ""
            const strValue = (item as PdfJsTextItem).str
            return typeof strValue === "string" ? strValue : ""
          })
          .join(" ")
          .replace(/\s+/g, " ")
          .trim()
        if (!pageText) continue

        const remaining = DOCUMENT_EXTRACT_MAX_CHARS - extractedChars
        if (remaining <= 0) break
        const clipped = pageText.length > remaining
          ? pageText.slice(0, remaining)
          : pageText
        chunks.push(clipped)
        extractedChars += clipped.length + 1
        if (extractedChars >= DOCUMENT_EXTRACT_MAX_CHARS) break
      } catch (pageError) {
        console.error(`Failed to extract PDF page text (${pageNumber}):`, pageError)
      } finally {
        try {
          page?.cleanup?.()
        } catch {
          // no-op
        }
      }
    }

    return normalizeExtractedText(chunks.join("\n"))
  } catch (error) {
    console.error("Failed to extract PDF text:", error)
    return ""
  } finally {
    try {
      pdfDocument?.cleanup?.()
    } catch {
      // no-op
    }
    try {
      await pdfDocument?.destroy?.()
    } catch {
      // no-op
    }
    try {
      await loadingTask?.destroy?.()
    } catch {
      // no-op
    }
  }
}

async function loadOfficeZip(bytes: Uint8Array): Promise<JSZip | null> {
  try {
    return await JSZip.loadAsync(bytes, {
      checkCRC32: false,
      createFolders: false,
    })
  } catch (error) {
    console.error("Failed to load office archive:", error)
    return null
  }
}

async function validateDocumentPayloadSafety(
  bytes: Uint8Array,
  mimeType: DocumentMimeType,
): Promise<string | null> {
  if (mimeType === "application/pdf" && !hasPdfMagicHeader(bytes)) {
    return "Invalid PDF payload."
  }
  if (mimeType === DOCX_MIME_TYPE || mimeType === XLSX_MIME_TYPE) {
    if (!hasZipMagicHeader(bytes)) {
      return "Office file must be a valid ZIP container."
    }
    const inspection = await inspectOfficeArchiveSafety(bytes, mimeType)
    if (!inspection.ok) return inspection.message
    return null
  }
  if (mimeType === "text/plain" && looksLikeBinaryTextPayload(bytes)) {
    return "Text file appears to contain binary data."
  }
  return null
}

function hasPdfMagicHeader(bytes: Uint8Array): boolean {
  if (bytes.length < 5) return false
  return bytes[0] === 0x25
    && bytes[1] === 0x50
    && bytes[2] === 0x44
    && bytes[3] === 0x46
    && bytes[4] === 0x2d
}

function hasZipMagicHeader(bytes: Uint8Array): boolean {
  if (bytes.length < 4) return false
  if (bytes[0] !== 0x50 || bytes[1] !== 0x4b) return false
  const marker = bytes[2] * 256 + bytes[3]
  return marker === 0x0304 || marker === 0x0506 || marker === 0x0708
}

function looksLikeBinaryTextPayload(bytes: Uint8Array): boolean {
  if (bytes.length === 0) return false
  const sampleSize = Math.min(bytes.length, 4096)
  let binaryCount = 0
  for (let i = 0; i < sampleSize; i += 1) {
    const value = bytes[i]
    const isAllowedControl = value === 9 || value === 10 || value === 13
    const isTextByte = value >= 32 && value <= 126
    const isMultiByteLead = value >= 0x80
    if (!isAllowedControl && !isTextByte && !isMultiByteLead) {
      binaryCount += 1
    }
  }
  return (binaryCount / sampleSize) > DOCUMENT_TEXT_BINARY_RATIO_MAX
}

async function inspectOfficeArchiveSafety(
  bytes: Uint8Array,
  mimeType: DocumentMimeType,
): Promise<{ ok: true } | { ok: false; message: string }> {
  let zip: JSZip
  try {
    zip = await JSZip.loadAsync(bytes, {
      checkCRC32: false,
      createFolders: false,
    })
  } catch {
    return { ok: false, message: "Failed to parse Office archive." }
  }

  const entries = Object.values(zip.files).filter((entry) => !entry.dir)
  if (entries.length === 0) {
    return { ok: false, message: "Office archive has no files." }
  }
  if (entries.length > DOCUMENT_ARCHIVE_MAX_ENTRIES) {
    return { ok: false, message: "Office archive has too many entries." }
  }

  let totalUncompressed = 0
  let totalCompressed = 0
  for (const entry of entries) {
    const entryName = String(entry.name || "")
    if (!entryName || entryName.startsWith("/") || entryName.includes("../") || entryName.includes("..\\")) {
      return { ok: false, message: "Office archive contains unsafe entry paths." }
    }
    const uncompressedSize = Number((entry as any)?._data?.uncompressedSize ?? 0)
    const compressedSize = Number((entry as any)?._data?.compressedSize ?? 0)
    if (Number.isFinite(uncompressedSize) && uncompressedSize > DOCUMENT_ARCHIVE_SINGLE_ENTRY_MAX_BYTES) {
      return { ok: false, message: "Office archive entry exceeds allowed size." }
    }
    if (Number.isFinite(uncompressedSize) && uncompressedSize > 0) {
      totalUncompressed += uncompressedSize
      if (totalUncompressed > DOCUMENT_ARCHIVE_TOTAL_UNCOMPRESSED_MAX_BYTES) {
        return { ok: false, message: "Office archive exceeds uncompressed size limit." }
      }
    }
    if (Number.isFinite(compressedSize) && compressedSize > 0) {
      totalCompressed += compressedSize
    }
  }

  const compressionRatio = totalUncompressed / Math.max(totalCompressed, 1)
  if (compressionRatio > DOCUMENT_ARCHIVE_MAX_COMPRESSION_RATIO) {
    return { ok: false, message: "Office archive compression ratio is too high." }
  }

  const hasContentTypes = !!zip.file("[Content_Types].xml")
  if (!hasContentTypes) {
    return { ok: false, message: "Office archive is missing required metadata." }
  }
  if (mimeType === DOCX_MIME_TYPE && !zip.file("word/document.xml")) {
    return { ok: false, message: "DOCX payload is missing word/document.xml." }
  }
  if (mimeType === XLSX_MIME_TYPE && !zip.file("xl/workbook.xml")) {
    return { ok: false, message: "XLSX payload is missing xl/workbook.xml." }
  }
  return { ok: true }
}

function extractClientIp(headers: Headers): string {
  const candidates = [
    headers.get("cf-connecting-ip"),
    headers.get("x-real-ip"),
    headers.get("x-forwarded-for"),
  ]
  for (const raw of candidates) {
    const value = String(raw ?? "").trim()
    if (!value) continue
    const first = value.split(",")[0]?.trim()
    if (first) return first
  }
  return "unknown"
}

function resolveAdminRateLimit(method: string, path: string): { maxRequests: number; windowMs: number } {
  if (method === "POST" && path === "/documents") {
    return {
      maxRequests: ADMIN_RATE_LIMIT_UPLOAD_MAX_REQUESTS,
      windowMs: ADMIN_RATE_LIMIT_UPLOAD_WINDOW_MS,
    }
  }
  return {
    maxRequests: ADMIN_RATE_LIMIT_DEFAULT_MAX_REQUESTS,
    windowMs: ADMIN_RATE_LIMIT_DEFAULT_WINDOW_MS,
  }
}

async function consumeRateLimitFromDb(
  supabase: ReturnType<typeof createClient>,
  bucket: string,
  maxRequests: number,
  windowMs: number,
): Promise<{ allowed: boolean; retryAfterMs: number }> {
  const windowSeconds = Math.max(1, Math.floor(windowMs / 1000))
  try {
    const { data, error } = await supabase.rpc("consume_security_rate_limit", {
      rate_bucket: bucket,
      window_seconds: windowSeconds,
      max_hits: maxRequests,
    })
    if (error) {
      console.error("Rate limit RPC failed (admin-api):", error.message)
      return { allowed: true, retryAfterMs: windowMs }
    }
    const row = Array.isArray(data) ? data[0] : null
    const allowed = row?.allowed !== false
    const retryAfterSeconds = Number(row?.retry_after_seconds ?? windowSeconds)
    const retryAfterMs = Math.max(1000, retryAfterSeconds * 1000)
    return { allowed, retryAfterMs }
  } catch (error) {
    console.error("Unexpected rate limit error (admin-api):", error)
    return { allowed: true, retryAfterMs: windowMs }
  }
}

function getOfficeXmlEntries(
  zip: JSZip,
  predicate: (entryName: string) => boolean,
): OfficeZipEntry[] {
  const items = Object.values(zip.files)
    .filter((entry) => !entry.dir && predicate(entry.name))
    .slice(0, DOCUMENT_ARCHIVE_MAX_XML_ENTRIES) as unknown as OfficeZipEntry[]
  return items
}

async function readOfficeXmlEntry(entry: OfficeZipEntry): Promise<string> {
  try {
    const uncompressedSize = Number((entry as any)?._data?.uncompressedSize ?? 0)
    if (Number.isFinite(uncompressedSize) && uncompressedSize > DOCUMENT_ARCHIVE_ENTRY_MAX_BYTES) {
      console.error(`Skipped oversized office xml entry: ${entry.name}`)
      return ""
    }
    const raw = await entry.async("string")
    if (!raw) return ""
    if (raw.length > DOCUMENT_ARCHIVE_ENTRY_MAX_BYTES) {
      return raw.slice(0, DOCUMENT_ARCHIVE_ENTRY_MAX_BYTES)
    }
    return raw
  } catch (error) {
    console.error(`Failed to read office xml entry (${entry.name}):`, error)
    return ""
  }
}

function parseXmlDocument(xml: string): Document | null {
  if (!xml) return null
  try {
    const doc = new DOMParser().parseFromString(xml, "application/xml")
    const parserErrors = doc.getElementsByTagName("parsererror")
    if (parserErrors && parserErrors.length > 0) return null
    return doc
  } catch {
    return null
  }
}

function appendChunkWithinLimit(chunks: string[], chunk: string, remainingChars: number): number {
  if (!Number.isFinite(remainingChars) || remainingChars <= 0) return 0
  const text = String(chunk ?? "")
  if (!text || !/\S/.test(text)) return remainingChars
  const clipped = text.length > remainingChars
    ? text.slice(0, remainingChars)
    : text
  chunks.push(clipped)
  return remainingChars - clipped.length - 1
}

function compareWordXmlEntry(a: string, b: string): number {
  const rank = (entryName: string): number => {
    if (entryName === "word/document.xml") return 0
    if (entryName.startsWith("word/header")) return 1
    if (entryName.startsWith("word/footer")) return 2
    if (entryName.startsWith("word/footnotes")) return 3
    if (entryName.startsWith("word/endnotes")) return 4
    return 9
  }
  const diff = rank(a) - rank(b)
  return diff !== 0 ? diff : a.localeCompare(b)
}

function appendWordXmlNodeText(node: Node, out: string[]): void {
  if (node.nodeType === Node.TEXT_NODE) {
    const value = node.nodeValue
    if (value) out.push(value)
    return
  }
  if (node.nodeType !== Node.ELEMENT_NODE) return

  const el = node as Element
  const local = String(el.localName || "").toLowerCase()
  if (local === "t") {
    const text = el.textContent || ""
    if (text) out.push(text)
    return
  }
  if (local === "tab") {
    out.push("\t")
    return
  }
  if (local === "br" || local === "cr") {
    out.push("\n")
    return
  }

  const children = Array.from(el.childNodes)
  for (const child of children) {
    appendWordXmlNodeText(child, out)
  }
  if (local === "p" || local === "tr") {
    out.push("\n")
  } else if (local === "tc") {
    out.push("\t")
  }
}

function extractWordXmlText(xml: string): string {
  const doc = parseXmlDocument(xml)
  if (!doc || !doc.documentElement) return ""
  const out: string[] = []
  appendWordXmlNodeText(doc.documentElement, out)
  return out.join("")
}

async function extractDocxText(bytes: Uint8Array): Promise<string> {
  const zip = await loadOfficeZip(bytes)
  if (!zip) return ""

  const entries = getOfficeXmlEntries(zip, (entryName) =>
    entryName.startsWith("word/")
      && entryName.endsWith(".xml")
      && !entryName.includes("/_rels/"),
  ).sort((a, b) => compareWordXmlEntry(a.name, b.name))

  const chunks: string[] = []
  let remainingChars = DOCUMENT_EXTRACT_MAX_CHARS
  for (const entry of entries) {
    if (remainingChars <= 0) break
    const xml = await readOfficeXmlEntry(entry)
    if (!xml) continue
    const text = extractWordXmlText(xml)
    if (!text) continue
    remainingChars = appendChunkWithinLimit(chunks, text, remainingChars)
  }
  return normalizeExtractedText(chunks.join("\n"))
}

function collectTextNodes(root: Element, localName: string): string[] {
  const nodes = root.getElementsByTagNameNS("*", localName)
  const out: string[] = []
  for (let i = 0; i < nodes.length; i += 1) {
    const value = nodes.item(i)?.textContent ?? ""
    if (value) out.push(value)
  }
  return out
}

function parseXlsxSharedStrings(xml: string): string[] {
  const doc = parseXmlDocument(xml)
  if (!doc || !doc.documentElement) return []
  const siNodes = doc.getElementsByTagNameNS("*", "si")
  const out: string[] = []
  for (let i = 0; i < siNodes.length; i += 1) {
    const si = siNodes.item(i)
    if (!si) {
      out.push("")
      continue
    }
    const joined = collectTextNodes(si, "t").join("")
    out.push(joined)
  }
  return out
}

function getDirectChildElementsByLocalName(root: Element, localName: string): Element[] {
  const out: Element[] = []
  for (let i = 0; i < root.children.length; i += 1) {
    const child = root.children.item(i)
    if (!child) continue
    if ((child.localName || "").toLowerCase() === localName) {
      out.push(child)
    }
  }
  return out
}

function getFirstDescendantElementByLocalName(root: Element, localName: string): Element | null {
  const nodes = root.getElementsByTagNameNS("*", localName)
  return nodes.length > 0 ? nodes.item(0) : null
}

function columnNameToIndex(columnName: string): number | null {
  const normalized = String(columnName || "").trim().toUpperCase()
  if (!normalized || !/^[A-Z]+$/.test(normalized)) return null
  let value = 0
  for (let i = 0; i < normalized.length; i += 1) {
    value = value * 26 + (normalized.charCodeAt(i) - 64)
  }
  return value - 1
}

function getColumnIndexFromCellRef(cellRef: string): number | null {
  const match = String(cellRef || "").toUpperCase().match(/^([A-Z]+)\d+$/)
  if (!match) return null
  return columnNameToIndex(match[1])
}

function extractXlsxCellText(cell: Element, sharedStrings: string[]): string {
  const cellType = String(cell.getAttribute("t") || "").trim().toLowerCase()
  const valueEl = getFirstDescendantElementByLocalName(cell, "v")
  const rawValue = String(valueEl?.textContent ?? "").trim()

  if (cellType === "s") {
    const idx = Number(rawValue)
    if (Number.isInteger(idx) && idx >= 0 && idx < sharedStrings.length) {
      return String(sharedStrings[idx] || "")
    }
    return ""
  }
  if (cellType === "inlineStr") {
    const inlineEl = getFirstDescendantElementByLocalName(cell, "is")
    if (!inlineEl) return ""
    return collectTextNodes(inlineEl, "t").join("")
  }
  if (cellType === "b") {
    if (rawValue === "1") return "TRUE"
    if (rawValue === "0") return "FALSE"
  }
  if (rawValue) return rawValue
  const formulaEl = getFirstDescendantElementByLocalName(cell, "f")
  const formula = String(formulaEl?.textContent ?? "").trim()
  return formula ? `=${formula}` : ""
}

function extractXlsxSheetText(xml: string, sharedStrings: string[]): string {
  const doc = parseXmlDocument(xml)
  if (!doc || !doc.documentElement) return ""

  const rowNodes = doc.getElementsByTagNameNS("*", "row")
  const lines: string[] = []
  for (let i = 0; i < rowNodes.length; i += 1) {
    const row = rowNodes.item(i)
    if (!row) continue
    const cellNodes = getDirectChildElementsByLocalName(row, "c")
    if (cellNodes.length === 0) continue

    const cols: string[] = []
    let nextCol = 0
    for (const cell of cellNodes) {
      const ref = String(cell.getAttribute("r") || "")
      const indexedCol = getColumnIndexFromCellRef(ref)
      const col = indexedCol == null ? nextCol : indexedCol
      while (nextCol < col) {
        cols.push("")
        nextCol += 1
      }
      const text = extractXlsxCellText(cell, sharedStrings)
      cols.push(text)
      nextCol = col + 1
    }

    while (cols.length > 0 && !String(cols[cols.length - 1] || "").trim()) {
      cols.pop()
    }
    if (cols.length === 0) continue
    lines.push(cols.join("\t"))
  }
  return lines.join("\n")
}

function compareXlsxWorksheetEntry(a: string, b: string): number {
  const parse = (name: string): number => {
    const match = name.match(/sheet(\d+)\.xml$/)
    if (!match) return Number.POSITIVE_INFINITY
    return Number(match[1])
  }
  const diff = parse(a) - parse(b)
  return Number.isFinite(diff) && diff !== 0 ? diff : a.localeCompare(b)
}

async function extractXlsxText(bytes: Uint8Array): Promise<string> {
  const zip = await loadOfficeZip(bytes)
  if (!zip) return ""

  const sharedEntry = zip.file("xl/sharedStrings.xml") as unknown as OfficeZipEntry | null
  const sharedStrings = sharedEntry
    ? parseXlsxSharedStrings(await readOfficeXmlEntry(sharedEntry))
    : []

  const worksheetEntries = getOfficeXmlEntries(zip, (entryName) =>
    entryName.startsWith("xl/worksheets/")
      && entryName.endsWith(".xml")
      && !entryName.includes("/_rels/"),
  ).sort((a, b) => compareXlsxWorksheetEntry(a.name, b.name))

  const chunks: string[] = []
  let remainingChars = DOCUMENT_EXTRACT_MAX_CHARS
  for (const entry of worksheetEntries) {
    if (remainingChars <= 0) break
    const xml = await readOfficeXmlEntry(entry)
    if (!xml) continue
    const text = extractXlsxSheetText(xml, sharedStrings)
    if (!text) continue
    remainingChars = appendChunkWithinLimit(chunks, text, remainingChars)
  }
  return normalizeExtractedText(chunks.join("\n"))
}

function tryDecodeText(bytes: Uint8Array): string {
  try {
    return new TextDecoder("utf-8", { fatal: false }).decode(bytes)
  } catch {
    return ""
  }
}

function normalizeExtractedText(value: string): string {
  const normalized = String(value ?? "")
    .replace(/\u0000/g, " ")
    .replace(/\r\n?/g, "\n")
    .trim()
  if (!normalized) return ""
  if (normalized.length <= DOCUMENT_EXTRACT_MAX_CHARS) return normalized
  return normalized.slice(0, DOCUMENT_EXTRACT_MAX_CHARS).trimEnd()
}

function buildDocumentSnippet(text: string, maxChars: number): string {
  const normalized = String(text ?? "")
    .replace(/\s+/g, " ")
    .trim()
  if (!normalized) return ""
  if (normalized.length <= maxChars) return normalized
  return `${normalized.slice(0, maxChars).trimEnd()}…`
}

async function fetchMediaUploadMaxMb(
  supabase: ReturnType<typeof createClient>,
): Promise<number> {
  try {
    const { data, error } = await supabase
      .from("summary_settings")
      .select("media_upload_max_mb")
      .eq("id", 1)
      .maybeSingle()
    if (error) {
      console.error("Failed to fetch media_upload_max_mb:", error.message)
      return DEFAULT_MEDIA_UPLOAD_MAX_MB
    }
    return normalizeMediaUploadMaxMb(data?.media_upload_max_mb)
  } catch (error) {
    console.error("Unexpected error while fetching media_upload_max_mb:", error)
    return DEFAULT_MEDIA_UPLOAD_MAX_MB
  }
}

async function fetchLineMediaUsageStats(
  supabase: ReturnType<typeof createClient>,
  roomId: string | null,
  mediaType: MediaType | null,
): Promise<{ ok: true; stats: MediaUsageStats } | { ok: false; message: string }> {
  const { data, error } = await supabase.rpc("get_line_media_usage_stats", {
    filter_room_id: roomId,
    filter_media_type: mediaType,
  })
  if (error) {
    return { ok: false, message: `Failed to fetch media usage stats: ${error.message}` }
  }

  const row = Array.isArray(data) ? data[0] : null
  const totalFiles = toNonNegativeInteger((row as any)?.total_files)
  const totalBytes = toNonNegativeInteger((row as any)?.total_bytes)
  return {
    ok: true,
    stats: {
      total_files: totalFiles,
      total_bytes: totalBytes,
    },
  }
}

function normalizeMediaType(value: string | null): MediaType | null {
  const normalized = String(value ?? "").trim().toLowerCase()
  if (normalized === "image" || normalized === "video" || normalized === "audio" || normalized === "file") {
    return normalized
  }
  return null
}

function normalizeMediaListRow(value: unknown): MediaListRow | null {
  if (!isRecord(value)) return null
  const idNum = Number(value.id)
  if (!Number.isFinite(idNum) || idNum <= 0) return null
  const mediaType = normalizeMediaType(String(value.media_type ?? ""))
  if (!mediaType) return null

  const messageId = toSafeString(value.message_id)
  const lineMessageId = toSafeString(value.line_message_id)
  const roomId = toSafeString(value.room_id)
  const storageBucket = toSafeString(value.storage_bucket)
  const storagePath = toSafeString(value.storage_path)
  if (!messageId || !lineMessageId || !roomId || !storageBucket || !storagePath) return null

  return {
    id: Math.floor(idNum),
    message_id: messageId,
    line_message_id: lineMessageId,
    room_id: roomId,
    room_name: value.room_name == null ? null : String(value.room_name),
    user_id: value.user_id == null ? null : String(value.user_id),
    media_type: mediaType,
    storage_bucket: storageBucket,
    storage_path: storagePath,
    original_file_name: value.original_file_name == null ? null : String(value.original_file_name),
    mime_type: value.mime_type == null ? null : String(value.mime_type),
    file_size_bytes: toNonNegativeInteger(value.file_size_bytes),
    created_at: String(value.created_at ?? ""),
  }
}

async function fetchRoomNameMapForIds(
  supabase: ReturnType<typeof createClient>,
  roomIds: string[],
): Promise<Map<string, string>> {
  const normalizedIds = Array.from(
    new Set(
      roomIds
        .map((value) => String(value ?? "").trim())
        .filter((value) => value.length > 0),
    ),
  )
  if (normalizedIds.length === 0) return new Map<string, string>()

  const { data, error } = await supabase
    .from("room_summary_settings")
    .select("room_id, room_name")
    .in("room_id", normalizedIds)

  if (error) {
    console.error("Failed to fetch room names for media list:", error.message)
    return new Map<string, string>()
  }

  const map = new Map<string, string>()
  for (const row of Array.isArray(data) ? data : []) {
    const id = toSafeString((row as any)?.room_id)
    const name = toSafeString((row as any)?.room_name)
    if (!id || !name) continue
    map.set(id, name)
  }
  return map
}

async function fetchMediaContextMap(
  supabase: ReturnType<typeof createClient>,
  rows: MediaListRow[],
): Promise<Map<number, MediaMessageContext>> {
  const contextMap = new Map<number, MediaMessageContext>()
  if (rows.length === 0) return contextMap

  const anchorMessageIds = Array.from(
    new Set(rows.map((row) => row.message_id).filter((id) => id.length > 0)),
  )
  const anchorMap = new Map<string, { room_id: string; created_at: string }>()
  if (anchorMessageIds.length > 0) {
    const { data: anchorRows, error: anchorError } = await supabase
      .from("line_messages")
      .select("id, room_id, created_at")
      .in("id", anchorMessageIds)
    if (anchorError) {
      console.error("Failed to fetch anchor line_messages for media context:", anchorError.message)
    } else {
      for (const row of Array.isArray(anchorRows) ? anchorRows : []) {
        const id = toSafeString((row as any)?.id)
        const roomId = toSafeString((row as any)?.room_id)
        const createdAt = toSafeString((row as any)?.created_at)
        if (!id || !roomId || !createdAt) continue
        anchorMap.set(id, { room_id: roomId, created_at: createdAt })
      }
    }
  }

  const contextEntries = await Promise.all(rows.map(async (row) => {
    const anchor = anchorMap.get(row.message_id)
    const roomId = anchor?.room_id || row.room_id
    const createdAt = anchor?.created_at || row.created_at
    if (!roomId || !createdAt) {
      return {
        mediaId: row.id,
        context: {
          before_text: null,
          before_at: null,
          after_text: null,
          after_at: null,
        } satisfies MediaMessageContext,
      }
    }

    const [beforeRes, afterRes] = await Promise.all([
      supabase
        .from("line_messages")
        .select("content, created_at")
        .eq("room_id", roomId)
        .lt("created_at", createdAt)
        .order("created_at", { ascending: false })
        .limit(8),
      supabase
        .from("line_messages")
        .select("content, created_at")
        .eq("room_id", roomId)
        .gt("created_at", createdAt)
        .order("created_at", { ascending: true })
        .limit(8),
    ])

    if (beforeRes.error) {
      console.error(`Failed to fetch before-context for media ${row.id}:`, beforeRes.error.message)
    }
    if (afterRes.error) {
      console.error(`Failed to fetch after-context for media ${row.id}:`, afterRes.error.message)
    }

    const before = pickMediaContextCandidate(beforeRes.data)
    const after = pickMediaContextCandidate(afterRes.data)
    return {
      mediaId: row.id,
      context: {
        before_text: before?.text ?? null,
        before_at: before?.created_at ?? null,
        after_text: after?.text ?? null,
        after_at: after?.created_at ?? null,
      } satisfies MediaMessageContext,
    }
  }))

  for (const entry of contextEntries) {
    contextMap.set(entry.mediaId, entry.context)
  }
  return contextMap
}

function pickMediaContextCandidate(
  rows: unknown,
): { text: string; created_at: string | null } | null {
  const list = Array.isArray(rows) ? rows : []
  for (const row of list) {
    const rawText = typeof (row as any)?.content === "string" ? String((row as any).content) : ""
    const text = normalizeMediaContextText(rawText)
    if (!text) continue
    const createdAt = toSafeString((row as any)?.created_at) || null
    return { text, created_at: createdAt }
  }
  return null
}

function normalizeMediaContextText(raw: string): string {
  if (!raw) return ""
  const withoutMetaLines = raw
    .split(/\r?\n/)
    .map((line) => String(line ?? "").trim())
    .filter((line) => line.length > 0)
    .filter((line) => !isInternalLineMessageMetaLine(line))
    .join(" ")
    .replace(/\[\[MEDIA:[^\]]+\]\]/gi, " ")
    .replace(/\s+/g, " ")
    .trim()
  if (!withoutMetaLines) return ""
  return truncateForContext(withoutMetaLines, 120)
}

function isInternalLineMessageMetaLine(line: string): boolean {
  return /^(LINE room_id:|LINE user_id:|source:\s*line-webhook)/i.test(line)
}

function truncateForContext(text: string, maxChars: number): string {
  if (text.length <= maxChars) return text
  return `${text.slice(0, maxChars).trimEnd()}…`
}

async function createSignedMediaUrl(
  supabase: ReturnType<typeof createClient>,
  storageBucket: string,
  storagePath: string,
): Promise<string | null> {
  try {
    const { data, error } = await supabase
      .storage
      .from(storageBucket)
      .createSignedUrl(storagePath, MEDIA_SIGNED_URL_EXPIRES_SEC)
    if (error) {
      console.error(`Failed to create signed URL for ${storageBucket}/${storagePath}:`, error.message)
      return null
    }
    const signedUrl = typeof data?.signedUrl === "string" ? data.signedUrl.trim() : ""
    return signedUrl || null
  } catch (error) {
    console.error(`Unexpected error while signing media URL for ${storageBucket}/${storagePath}:`, error)
    return null
  }
}

async function createSignedMediaDownloadUrl(
  supabase: ReturnType<typeof createClient>,
  storageBucket: string,
  storagePath: string,
  fileName: string,
): Promise<string | null> {
  const safeName = sanitizeDownloadFileName(fileName)
  const downloadOption: string | boolean = safeName || true
  try {
    const { data, error } = await supabase
      .storage
      .from(storageBucket)
      .createSignedUrl(storagePath, MEDIA_SIGNED_URL_EXPIRES_SEC, {
        download: downloadOption,
      } as any)
    if (error) {
      console.error(`Failed to create signed download URL for ${storageBucket}/${storagePath}:`, error.message)
      return null
    }
    const signedUrl = typeof data?.signedUrl === "string" ? data.signedUrl.trim() : ""
    return signedUrl || null
  } catch (error) {
    console.error(`Unexpected error while signing media download URL for ${storageBucket}/${storagePath}:`, error)
    return null
  }
}

function sanitizeDownloadFileName(value: string): string {
  const sanitized = String(value ?? "")
    .replace(/[\\/:*?"<>|]+/g, "_")
    .replace(/\s+/g, " ")
    .trim()
  if (!sanitized) return ""
  if (sanitized.length <= 120) return sanitized
  return sanitized.slice(0, 120).trimEnd()
}

function formatLineMediaTag(lineMessageId: string): string {
  return `[[MEDIA:${lineMessageId}]]`
}

async function deleteMediaItemById(
  supabase: ReturnType<typeof createClient>,
  mediaId: number,
): Promise<{ media_id: number; room_id: string; line_message_id: string; storage_deleted: boolean }> {
  const { data: row, error: fetchError } = await supabase
    .from("line_message_media")
    .select("id, room_id, line_message_id, storage_bucket, storage_path")
    .eq("id", mediaId)
    .maybeSingle()
  if (fetchError) {
    throw { status: 500, message: `Failed to fetch media row: ${fetchError.message}` } satisfies AppError
  }
  if (!row) {
    throw { status: 404, message: "Media not found." } satisfies AppError
  }

  const storageBucket = toSafeString(row.storage_bucket)
  const storagePath = toSafeString(row.storage_path)
  let storageDeleted = false
  if (storageBucket && storagePath) {
    const { data: removed, error: removeError } = await supabase
      .storage
      .from(storageBucket)
      .remove([storagePath])
    if (removeError) {
      throw { status: 500, message: `Failed to delete storage object: ${removeError.message}` } satisfies AppError
    }
    storageDeleted = Array.isArray(removed) && removed.length > 0
  }

  const { error: deleteError } = await supabase
    .from("line_message_media")
    .delete()
    .eq("id", mediaId)
  if (deleteError) {
    throw { status: 500, message: `Failed to delete media metadata: ${deleteError.message}` } satisfies AppError
  }

  return {
    media_id: mediaId,
    room_id: String(row.room_id ?? ""),
    line_message_id: String(row.line_message_id ?? ""),
    storage_deleted: storageDeleted,
  }
}

async function deleteDocumentById(
  supabase: ReturnType<typeof createClient>,
  documentId: number,
): Promise<{ document_id: number; room_id: string | null; storage_deleted: boolean }> {
  const { data: row, error: fetchError } = await supabase
    .from("line_search_documents")
    .select("id, room_id, storage_bucket, storage_path")
    .eq("id", documentId)
    .maybeSingle()
  if (fetchError) {
    throw { status: 500, message: `Failed to fetch document row: ${fetchError.message}` } satisfies AppError
  }
  if (!row) {
    throw { status: 404, message: "Document not found." } satisfies AppError
  }

  const storageBucket = toSafeString(row.storage_bucket)
  const storagePath = toSafeString(row.storage_path)
  let storageDeleted = false
  if (storageBucket && storagePath) {
    const { data: removed, error: removeError } = await supabase
      .storage
      .from(storageBucket)
      .remove([storagePath])
    if (removeError) {
      throw { status: 500, message: `Failed to delete storage object: ${removeError.message}` } satisfies AppError
    }
    storageDeleted = Array.isArray(removed) && removed.length > 0
  }

  const { error: deleteError } = await supabase
    .from("line_search_documents")
    .delete()
    .eq("id", documentId)
  if (deleteError) {
    throw { status: 500, message: `Failed to delete document metadata: ${deleteError.message}` } satisfies AppError
  }

  return {
    document_id: documentId,
    room_id: row.room_id == null ? null : String(row.room_id),
    storage_deleted: storageDeleted,
  }
}

async function removeRoomMediaObjects(
  supabase: ReturnType<typeof createClient>,
  roomId: string,
): Promise<{ ok: true; deletedFiles: number; deletedMetadataRows: number } | { ok: false; message: string }> {
  const { data: mediaRows, error: mediaError } = await supabase
    .from("line_message_media")
    .select("id, storage_bucket, storage_path")
    .eq("room_id", roomId)

  if (mediaError) {
    return { ok: false, message: `Failed to fetch room media metadata: ${mediaError.message}` }
  }

  const rows = Array.isArray(mediaRows)
    ? mediaRows
        .map((item) => ({
          id: Number(item?.id),
          storage_bucket: toSafeString(item?.storage_bucket),
          storage_path: toSafeString(item?.storage_path),
        }))
        .filter((item) => Number.isFinite(item.id) && item.id > 0 && item.storage_bucket && item.storage_path)
    : []

  if (rows.length === 0) {
    return { ok: true, deletedFiles: 0, deletedMetadataRows: 0 }
  }

  const bucketMap = new Map<string, string[]>()
  for (const row of rows) {
    const list = bucketMap.get(row.storage_bucket) ?? []
    list.push(row.storage_path)
    bucketMap.set(row.storage_bucket, list)
  }

  let deletedFiles = 0
  for (const [bucket, paths] of bucketMap.entries()) {
    const chunks = chunkArray(paths, 100)
    for (const chunk of chunks) {
      const { data: removed, error: removeError } = await supabase
        .storage
        .from(bucket)
        .remove(chunk)
      if (removeError) {
        return { ok: false, message: `Failed to delete storage files in bucket ${bucket}: ${removeError.message}` }
      }
      deletedFiles += Array.isArray(removed) ? removed.length : 0
    }
  }

  const { count: deletedMetadataRows, error: deleteMetaError } = await supabase
    .from("line_message_media")
    .delete({ count: "exact" })
    .eq("room_id", roomId)
  if (deleteMetaError) {
    return { ok: false, message: `Failed to delete media metadata: ${deleteMetaError.message}` }
  }

  return { ok: true, deletedFiles, deletedMetadataRows: deletedMetadataRows ?? 0 }
}

async function removeRoomDocuments(
  supabase: ReturnType<typeof createClient>,
  roomId: string,
): Promise<{ ok: true; deletedFiles: number; deletedMetadataRows: number } | { ok: false; message: string }> {
  const { data: rowsRaw, error: fetchError } = await supabase
    .from("line_search_documents")
    .select("id, storage_bucket, storage_path")
    .eq("room_id", roomId)
  if (fetchError) {
    return { ok: false, message: `Failed to fetch room documents: ${fetchError.message}` }
  }

  const rows = Array.isArray(rowsRaw)
    ? rowsRaw
        .map((item) => ({
          id: Number(item?.id),
          storage_bucket: toSafeString(item?.storage_bucket),
          storage_path: toSafeString(item?.storage_path),
        }))
        .filter((item) => Number.isFinite(item.id) && item.id > 0 && item.storage_bucket && item.storage_path)
    : []

  if (rows.length === 0) {
    return { ok: true, deletedFiles: 0, deletedMetadataRows: 0 }
  }

  const bucketMap = new Map<string, string[]>()
  for (const row of rows) {
    const list = bucketMap.get(row.storage_bucket) ?? []
    list.push(row.storage_path)
    bucketMap.set(row.storage_bucket, list)
  }

  let deletedFiles = 0
  for (const [bucket, paths] of bucketMap.entries()) {
    const chunks = chunkArray(paths, 100)
    for (const chunk of chunks) {
      const { data: removed, error: removeError } = await supabase
        .storage
        .from(bucket)
        .remove(chunk)
      if (removeError) {
        return { ok: false, message: `Failed to delete document files in bucket ${bucket}: ${removeError.message}` }
      }
      deletedFiles += Array.isArray(removed) ? removed.length : 0
    }
  }

  const { count: deletedMetadataRows, error: deleteMetaError } = await supabase
    .from("line_search_documents")
    .delete({ count: "exact" })
    .eq("room_id", roomId)
  if (deleteMetaError) {
    return { ok: false, message: `Failed to delete document metadata: ${deleteMetaError.message}` }
  }
  return { ok: true, deletedFiles, deletedMetadataRows: deletedMetadataRows ?? 0 }
}

function chunkArray<T>(items: T[], size: number): T[][] {
  if (size <= 0) return [items]
  const chunks: T[][] = []
  for (let i = 0; i < items.length; i += size) {
    chunks.push(items.slice(i, i + size))
  }
  return chunks
}

async function fetchStorageUsageState(
  supabase: ReturnType<typeof createClient>,
): Promise<{ stats: StorageUsageStats | null; error: string | null }> {
  const { data, error } = await supabase.rpc("get_storage_usage_stats")
  if (error) {
    return { stats: null, error: `容量取得エラー: ${error.message}` }
  }
  return { stats: normalizeStorageUsageStats(data), error: null }
}

function normalizeStorageUsageStats(value: unknown): StorageUsageStats | null {
  if (!isRecord(value)) return null

  const managedTablesRaw = Array.isArray(value.managed_tables) ? value.managed_tables : []
  const managedTables = managedTablesRaw
    .map((item) => normalizeStorageUsageTableStat(item))
    .filter((item): item is StorageUsageTableStat => item !== null)
    .sort((a, b) => b.size_bytes - a.size_bytes)

  return {
    database_size_bytes: toNonNegativeInteger(value.database_size_bytes),
    database_size_pretty: toSafeString(value.database_size_pretty) || "0 bytes",
    managed_tables_total_bytes: toNonNegativeInteger(value.managed_tables_total_bytes),
    managed_tables_total_pretty: toSafeString(value.managed_tables_total_pretty) || "0 bytes",
    managed_tables: managedTables,
  }
}

function normalizeStorageUsageTableStat(value: unknown): StorageUsageTableStat | null {
  if (!isRecord(value)) return null
  const tableName = toSafeString(value.table_name)
  if (!tableName) return null
  return {
    table_name: tableName,
    size_bytes: toNonNegativeInteger(value.size_bytes),
    size_pretty: toSafeString(value.size_pretty) || "0 bytes",
  }
}

function isActionableDeliveryLogStatus(status: unknown, details?: unknown): boolean {
  const normalized = String(status ?? "").trim().toLowerCase()
  if (!normalized) return true
  if (isForceRunLogDetails(details)) return true
  return !nonActionableDeliveryLogStatuses.has(normalized)
}

const nonActionableDeliveryLogStatuses = new Set([
  "no_messages",
  "not_scheduled",
  "no_room_summary",
  "overall_schedule_skip",
])

function isForceRunLogDetails(details: unknown): boolean {
  if (!isRecord(details)) return false
  return details.force_run === true
}

async function waitForNewLog(
  supabase: ReturnType<typeof createClient>,
  previousId: number,
): Promise<{ id: number; run_at: string; status: string; reason: string | null } | null> {
  const maxAttempts = 20
  const intervalMs = 1000
  for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
    if (attempt > 0) {
      await new Promise((resolve) => setTimeout(resolve, intervalMs))
    }

    const { data, error } = await supabase
      .from("summary_delivery_logs")
      .select("id, run_at, status, reason")
      .order("id", { ascending: false })
      .limit(1)
      .maybeSingle()

    if (error) {
      continue
    }
    if (data && data.id > previousId) {
      return data
    }
  }
  return null
}

async function fetchGlobalSettings(supabase: ReturnType<typeof createClient>) {
  const { data, error } = await supabase
    .from("summary_settings")
    .select("id, delivery_hours, is_enabled, message_cleanup_timing, last_delivery_summary_mode, message_retention_days, calendar_tomorrow_reminder_enabled, calendar_tomorrow_reminder_hours, calendar_tomorrow_reminder_only_if_events, calendar_tomorrow_reminder_max_items, media_upload_max_mb, updated_at")
    .eq("id", 1)
    .maybeSingle()

  if (error) {
    throw { status: 500, message: `Failed to fetch global settings: ${error.message}` } satisfies AppError
  }

  if (data) {
    return data
  }

  const fallback = {
    id: 1,
    delivery_hours: [12, 17, 23],
    is_enabled: true,
    message_cleanup_timing: "after_each_delivery" as MessageCleanupTiming,
    last_delivery_summary_mode: "independent" as LastDeliverySummaryMode,
    message_retention_days: 365 as MessageRetentionDays,
    calendar_tomorrow_reminder_enabled: true,
    calendar_tomorrow_reminder_hours: [19],
    calendar_tomorrow_reminder_only_if_events: false,
    calendar_tomorrow_reminder_max_items: 20,
    media_upload_max_mb: DEFAULT_MEDIA_UPLOAD_MAX_MB,
    updated_at: new Date().toISOString(),
  }

  const { data: inserted, error: insertError } = await supabase
    .from("summary_settings")
    .upsert(fallback, { onConflict: "id" })
    .select("id, delivery_hours, is_enabled, message_cleanup_timing, last_delivery_summary_mode, message_retention_days, calendar_tomorrow_reminder_enabled, calendar_tomorrow_reminder_hours, calendar_tomorrow_reminder_only_if_events, calendar_tomorrow_reminder_max_items, media_upload_max_mb, updated_at")
    .single()

  if (insertError) {
    throw { status: 500, message: `Failed to initialize global settings: ${insertError.message}` } satisfies AppError
  }

  return inserted
}

function buildGlobalSettingsPayload(body: unknown): {
  delivery_hours: number[]
  is_enabled: boolean
  message_cleanup_timing: MessageCleanupTiming
  last_delivery_summary_mode: LastDeliverySummaryMode
  message_retention_days: MessageRetentionDays
  calendar_tomorrow_reminder_enabled: boolean
  calendar_tomorrow_reminder_hours: number[]
  calendar_tomorrow_reminder_only_if_events: boolean
  calendar_tomorrow_reminder_max_items: number
  media_upload_max_mb: number | null
} {
  if (!isRecord(body)) {
    throw { status: 400, message: "Invalid JSON body." } satisfies AppError
  }

  const isEnabled = body.is_enabled
  if (typeof isEnabled !== "boolean") {
    throw { status: 400, message: "is_enabled must be boolean." } satisfies AppError
  }

  const deliveryHours = normalizeHours(body.delivery_hours, false)
  if (deliveryHours.length === 0) {
    throw { status: 400, message: "delivery_hours must include at least one hour." } satisfies AppError
  }

  const messageCleanupTiming = normalizeMessageCleanupTiming(body.message_cleanup_timing)
  const lastDeliverySummaryMode = normalizeLastDeliverySummaryMode(body.last_delivery_summary_mode)
  if (lastDeliverySummaryMode === "daily_rollup" && messageCleanupTiming !== "end_of_day") {
    throw {
      status: 400,
      message: "last_delivery_summary_mode=daily_rollup requires message_cleanup_timing=end_of_day.",
    } satisfies AppError
  }
  const messageRetentionDays = normalizeMessageRetentionDays(body.message_retention_days)
  const mediaUploadMaxMb = body.media_upload_max_mb == null || body.media_upload_max_mb === ""
    ? null
    : normalizeMediaUploadMaxMb(body.media_upload_max_mb)

  const reminderEnabled = body.calendar_tomorrow_reminder_enabled
  if (typeof reminderEnabled !== "boolean") {
    throw { status: 400, message: "calendar_tomorrow_reminder_enabled must be boolean." } satisfies AppError
  }

  const reminderHours = normalizeHours(body.calendar_tomorrow_reminder_hours, false)
  if (reminderHours.length === 0) {
    throw { status: 400, message: "calendar_tomorrow_reminder_hours must include at least one hour." } satisfies AppError
  }

  const onlyIfEvents = body.calendar_tomorrow_reminder_only_if_events
  if (typeof onlyIfEvents !== "boolean") {
    throw { status: 400, message: "calendar_tomorrow_reminder_only_if_events must be boolean." } satisfies AppError
  }

  const maxItems = Number(body.calendar_tomorrow_reminder_max_items)
  if (!Number.isInteger(maxItems) || maxItems < 1 || maxItems > 50) {
    throw { status: 400, message: "calendar_tomorrow_reminder_max_items must be an integer between 1 and 50." } satisfies AppError
  }

  return {
    is_enabled: isEnabled,
    delivery_hours: deliveryHours,
    message_cleanup_timing: messageCleanupTiming,
    last_delivery_summary_mode: lastDeliverySummaryMode,
    message_retention_days: messageRetentionDays,
    calendar_tomorrow_reminder_enabled: reminderEnabled,
    calendar_tomorrow_reminder_hours: reminderHours,
    calendar_tomorrow_reminder_only_if_events: onlyIfEvents,
    calendar_tomorrow_reminder_max_items: maxItems,
    media_upload_max_mb: mediaUploadMaxMb,
  }
}

function buildRoomSettingsPayload(body: unknown): {
  room_id: string
  room_name: string | null
  is_enabled: boolean
  bot_reply_enabled: boolean
  send_room_summary: boolean
  calendar_tomorrow_reminder_enabled: boolean
  calendar_ai_auto_create_enabled: boolean
  calendar_silent_auto_register_enabled: boolean
  message_search_enabled: boolean
  message_search_library_enabled: boolean
  media_file_access_enabled: boolean
  gmail_reservation_alert_enabled: boolean
  room_sort_order: number | null
  delivery_hours: number[] | null
  message_cleanup_timing: MessageCleanupTiming | null
  last_delivery_summary_mode: LastDeliverySummaryMode | null
} {
  if (!isRecord(body)) {
    throw { status: 400, message: "Invalid JSON body." } satisfies AppError
  }

  const roomIdRaw = String(body.room_id ?? "").trim()
  if (!roomIdRaw) {
    throw { status: 400, message: "room_id is required." } satisfies AppError
  }

  const isEnabled = body.is_enabled
  if (typeof isEnabled !== "boolean") {
    throw { status: 400, message: "is_enabled must be boolean." } satisfies AppError
  }

  const botReplyEnabledRaw = body.bot_reply_enabled
  if (botReplyEnabledRaw != null && typeof botReplyEnabledRaw !== "boolean") {
    throw { status: 400, message: "bot_reply_enabled must be boolean when provided." } satisfies AppError
  }
  const botReplyEnabled = botReplyEnabledRaw !== false

  const sendRoomSummary = body.send_room_summary
  if (typeof sendRoomSummary !== "boolean") {
    throw { status: 400, message: "send_room_summary must be boolean." } satisfies AppError
  }

  const roomTomorrowReminderEnabledRaw = body.calendar_tomorrow_reminder_enabled
  if (roomTomorrowReminderEnabledRaw != null && typeof roomTomorrowReminderEnabledRaw !== "boolean") {
    throw { status: 400, message: "calendar_tomorrow_reminder_enabled must be boolean when provided." } satisfies AppError
  }
  const roomTomorrowReminderEnabled = roomTomorrowReminderEnabledRaw === true

  const roomAiAutoCreateEnabledRaw = body.calendar_ai_auto_create_enabled
  if (roomAiAutoCreateEnabledRaw != null && typeof roomAiAutoCreateEnabledRaw !== "boolean") {
    throw { status: 400, message: "calendar_ai_auto_create_enabled must be boolean when provided." } satisfies AppError
  }
  const roomAiAutoCreateEnabled = roomAiAutoCreateEnabledRaw !== false

  const roomSilentAutoRegisterEnabledRaw = body.calendar_silent_auto_register_enabled
  if (roomSilentAutoRegisterEnabledRaw != null && typeof roomSilentAutoRegisterEnabledRaw !== "boolean") {
    throw { status: 400, message: "calendar_silent_auto_register_enabled must be boolean when provided." } satisfies AppError
  }
  const roomSilentAutoRegisterEnabled = roomSilentAutoRegisterEnabledRaw === true

  const messageSearchEnabledRaw = body.message_search_enabled
  if (messageSearchEnabledRaw != null && typeof messageSearchEnabledRaw !== "boolean") {
    throw { status: 400, message: "message_search_enabled must be boolean when provided." } satisfies AppError
  }
  const messageSearchEnabled = messageSearchEnabledRaw !== false

  const messageSearchLibraryEnabledRaw = body.message_search_library_enabled
  if (messageSearchLibraryEnabledRaw != null && typeof messageSearchLibraryEnabledRaw !== "boolean") {
    throw { status: 400, message: "message_search_library_enabled must be boolean when provided." } satisfies AppError
  }
  const messageSearchLibraryEnabled = messageSearchLibraryEnabledRaw !== false

  const mediaFileAccessEnabledRaw = body.media_file_access_enabled
  if (mediaFileAccessEnabledRaw != null && typeof mediaFileAccessEnabledRaw !== "boolean") {
    throw { status: 400, message: "media_file_access_enabled must be boolean when provided." } satisfies AppError
  }
  const mediaFileAccessEnabled = mediaFileAccessEnabledRaw !== false

  const gmailReservationAlertEnabledRaw = body.gmail_reservation_alert_enabled
  if (gmailReservationAlertEnabledRaw != null && typeof gmailReservationAlertEnabledRaw !== "boolean") {
    throw { status: 400, message: "gmail_reservation_alert_enabled must be boolean when provided." } satisfies AppError
  }
  const gmailReservationAlertEnabled = gmailReservationAlertEnabledRaw === true

  const roomNameRaw = typeof body.room_name === "string" ? body.room_name.trim() : ""
  const roomSortOrderRaw = body.room_sort_order
  let roomSortOrder: number | null = null
  if (roomSortOrderRaw != null && roomSortOrderRaw !== "") {
    const num = Number(roomSortOrderRaw)
    if (!Number.isInteger(num) || num < 0 || num > 1000000) {
      throw { status: 400, message: "room_sort_order must be an integer between 0 and 1000000 or null." } satisfies AppError
    }
    roomSortOrder = num
  }
  const deliveryHours = body.delivery_hours == null ? null : normalizeHours(body.delivery_hours, false)
  if (Array.isArray(deliveryHours) && deliveryHours.length === 0) {
    throw { status: 400, message: "delivery_hours must contain at least one hour or null." } satisfies AppError
  }

  const roomCleanupTiming = normalizeOptionalMessageCleanupTiming(body.message_cleanup_timing)
  const roomSummaryMode = normalizeOptionalLastDeliverySummaryMode(body.last_delivery_summary_mode)
  if (roomSummaryMode === "daily_rollup" && roomCleanupTiming === "after_each_delivery") {
    throw {
      status: 400,
      message: "last_delivery_summary_mode=daily_rollup requires message_cleanup_timing=end_of_day or null (inherit).",
    } satisfies AppError
  }

  return {
    room_id: roomIdRaw,
    room_name: roomNameRaw || null,
    is_enabled: isEnabled,
    bot_reply_enabled: botReplyEnabled,
    send_room_summary: sendRoomSummary,
    calendar_tomorrow_reminder_enabled: roomTomorrowReminderEnabled,
    calendar_ai_auto_create_enabled: roomAiAutoCreateEnabled,
    calendar_silent_auto_register_enabled: roomSilentAutoRegisterEnabled,
    message_search_enabled: messageSearchEnabled,
    message_search_library_enabled: messageSearchLibraryEnabled,
    media_file_access_enabled: mediaFileAccessEnabled,
    gmail_reservation_alert_enabled: gmailReservationAlertEnabled,
    room_sort_order: roomSortOrder,
    delivery_hours: deliveryHours,
    message_cleanup_timing: roomCleanupTiming,
    last_delivery_summary_mode: roomSummaryMode,
  }
}

function buildLineUserPermissionPayload(body: unknown): LineUserPermissionRow {
  if (!isRecord(body)) {
    throw { status: 400, message: "Invalid JSON body." } satisfies AppError
  }
  const lineUserId = String(body.line_user_id ?? "").trim()
  if (!lineUserId) {
    throw { status: 400, message: "line_user_id is required." } satisfies AppError
  }
  const ensureBoolean = (value: unknown, key: string, fallback: boolean): boolean => {
    if (value == null) return fallback
    if (typeof value !== "boolean") {
      throw { status: 400, message: `${key} must be boolean when provided.` } satisfies AppError
    }
    return value
  }
  const displayNameRaw = typeof body.display_name === "string" ? body.display_name.trim() : ""
  let assignedStore: string | null = null
  if (body.assigned_store != null) {
    const raw = typeof body.assigned_store === "string" ? body.assigned_store.trim() : ""
    if (raw) {
      if (!isMarugoGroupStoreLabel(raw)) {
        throw { status: 400, message: "assigned_store must be one of the predefined store labels." } satisfies AppError
      }
      assignedStore = raw
    }
  }
  let assignedJobTitle: string | null = null
  if (body.assigned_job_title != null) {
    const raw = typeof body.assigned_job_title === "string" ? body.assigned_job_title.trim() : ""
    if (raw) {
      if (!isJobTitleLabel(raw)) {
        throw { status: 400, message: "assigned_job_title must be one of the predefined job titles." } satisfies AppError
      }
      assignedJobTitle = raw
    }
  }
  const excludedRoomIdsRaw = body.excluded_message_search_room_ids
  let excludedRoomIds: string[] = []
  if (excludedRoomIdsRaw != null) {
    if (!Array.isArray(excludedRoomIdsRaw)) {
      throw { status: 400, message: "excluded_message_search_room_ids must be string[] when provided." } satisfies AppError
    }
    excludedRoomIds = Array.from(new Set(excludedRoomIdsRaw
      .map((v) => String(v ?? "").trim())
      .filter((v) => v.length > 0)))
  }
  return {
    line_user_id: lineUserId,
    display_name: displayNameRaw || null,
    is_active: ensureBoolean(body.is_active, "is_active", true),
    can_message_search: ensureBoolean(body.can_message_search, "can_message_search", true),
    can_library_search: ensureBoolean(body.can_library_search, "can_library_search", true),
    can_calendar_create: ensureBoolean(body.can_calendar_create, "can_calendar_create", true),
    can_calendar_update: ensureBoolean(body.can_calendar_update, "can_calendar_update", true),
    can_media_access: ensureBoolean(body.can_media_access, "can_media_access", true),
    excluded_message_search_room_ids: excludedRoomIds,
    assigned_store: assignedStore,
    assigned_job_title: assignedJobTitle,
    updated_at: new Date().toISOString(),
  }
}

function normalizeHours(value: unknown, allowNull: boolean): number[] | null {
  if (value == null) {
    return allowNull ? null : []
  }

  if (!Array.isArray(value)) {
    throw { status: 400, message: "delivery_hours must be an array of integers 0-23." } satisfies AppError
  }

  const hours: number[] = []
  for (const item of value) {
    const num = Number(item)
    if (!Number.isInteger(num) || num < 0 || num > 23) {
      throw { status: 400, message: "delivery_hours must be an array of integers 0-23." } satisfies AppError
    }
    if (!hours.includes(num)) {
      hours.push(num)
    }
  }
  hours.sort((a, b) => a - b)
  return hours
}

function normalizeMessageCleanupTiming(value: unknown): MessageCleanupTiming {
  if (value == null) return "after_each_delivery"
  if (value === "after_each_delivery" || value === "end_of_day") return value
  throw {
    status: 400,
    message: "message_cleanup_timing must be either after_each_delivery or end_of_day.",
  } satisfies AppError
}

function normalizeLastDeliverySummaryMode(value: unknown): LastDeliverySummaryMode {
  if (value == null) return "independent"
  if (value === "independent" || value === "daily_rollup") return value
  throw {
    status: 400,
    message: "last_delivery_summary_mode must be either independent or daily_rollup.",
  } satisfies AppError
}

function normalizeMessageRetentionDays(value: unknown): MessageRetentionDays {
  if (value == null || value === "") return 365
  const days = Number(value)
  if (days === 0 || days === 60 || days === 120 || days === 180 || days === 365 || days === 730 || days === 1095) {
    return days
  }
  throw {
    status: 400,
    message: "message_retention_days must be one of 0, 60, 120, 180, 365, 730, or 1095.",
  } satisfies AppError
}

function normalizeMediaUploadMaxMb(value: unknown): number {
  if (value == null || value === "") return DEFAULT_MEDIA_UPLOAD_MAX_MB
  const mb = Number(value)
  if (!Number.isInteger(mb) || mb < 1 || mb > MAX_MEDIA_UPLOAD_MAX_MB) {
    throw {
      status: 400,
      message: `media_upload_max_mb must be an integer between 1 and ${MAX_MEDIA_UPLOAD_MAX_MB}.`,
    } satisfies AppError
  }
  return mb
}

function normalizeOptionalMessageCleanupTiming(value: unknown): MessageCleanupTiming | null {
  if (value == null || value === "") return null
  return normalizeMessageCleanupTiming(value)
}

function normalizeOptionalLastDeliverySummaryMode(value: unknown): LastDeliverySummaryMode | null {
  if (value == null || value === "") return null
  return normalizeLastDeliverySummaryMode(value)
}

function secureEqual(a: string, b: string): boolean {
  const encoder = new TextEncoder()
  const aBytes = encoder.encode(a)
  const bBytes = encoder.encode(b)
  if (aBytes.length !== bBytes.length) return false

  let result = 0
  for (let i = 0; i < aBytes.length; i += 1) {
    result |= aBytes[i] ^ bBytes[i]
  }
  return result === 0
}

async function parseJson(req: Request): Promise<unknown> {
  try {
    return await req.json()
  } catch {
    throw { status: 400, message: "Request body must be valid JSON." } satisfies AppError
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value)
}

function toSafeString(value: unknown): string {
  return typeof value === "string" ? value.trim() : ""
}

function toNonNegativeInteger(value: unknown): number {
  const parsed = Number(value)
  if (!Number.isFinite(parsed) || parsed < 0) return 0
  return Math.floor(parsed)
}

function clampInt(value: string | null, fallback: number, min: number, max: number): number {
  if (!value) return fallback
  const parsed = Number(value)
  if (!Number.isInteger(parsed)) return fallback
  return Math.min(max, Math.max(min, parsed))
}

function parseBooleanEnv(value: string | undefined, fallback: boolean): boolean {
  if (value == null) return fallback
  const normalized = String(value).trim().toLowerCase()
  if (!normalized) return fallback
  if (normalized === "1" || normalized === "true" || normalized === "yes" || normalized === "on") return true
  if (normalized === "0" || normalized === "false" || normalized === "no" || normalized === "off") return false
  return fallback
}

function normalizePath(pathname: string): string {
  const stripped = pathname
    .replace(/^\/functions\/v1\/admin-api/, "")
    .replace(/^\/admin-api/, "")
  return stripped || "/"
}

function asAppError(error: unknown): AppError {
  if (isRecord(error) && typeof error.status === "number" && typeof error.message === "string") {
    return { status: error.status, message: error.message }
  }
  return { status: 500, message: error instanceof Error ? error.message : "Internal Server Error" }
}

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      ...corsHeaders,
      "Content-Type": "application/json; charset=utf-8",
      "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
      "Pragma": "no-cache",
      "Expires": "0",
    },
  })
}
