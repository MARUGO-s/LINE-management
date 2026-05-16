import "jsr:@supabase/functions-js/edge-runtime.d.ts"
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.44.0"
import {
  readReceiptSheetsPilotConfig,
  runReceiptSheetsPilotSync,
  runReceiptSheetsPilotSyncViaGas,
  type ReceiptSheetsGasPullInput,
  type ReceiptSheetsSyncDirection,
} from "../_shared/receipt_sheets_pilot_sync.ts"

Deno.serve(async (req) => {
  if (req.method === "OPTIONS") {
    return json({}, 204)
  }

  if (!isAuthorized(req)) {
    return json({ ok: false, error: "Unauthorized." }, 401)
  }

  const supabaseUrl = Deno.env.get("SUPABASE_URL") ?? ""
  const serviceRoleKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") ?? ""
  if (!supabaseUrl || !serviceRoleKey) {
    return json({ ok: false, error: "Supabase env is missing." }, 500)
  }

  const supabase = createClient(supabaseUrl, serviceRoleKey)
  const url = new URL(req.url)
  let direction: ReceiptSheetsSyncDirection = "both"
  let viaGas = url.searchParams.get("via_gas") === "1"
  let gasPull: Partial<ReceiptSheetsGasPullInput> = {}

  if (req.method === "POST") {
    try {
      const body = await req.json()
      const raw = String(body?.direction ?? "").trim().toLowerCase()
      if (raw === "pull" || raw === "push" || raw === "both") {
        direction = raw
      }
      if (body?.via_gas === true) viaGas = true
      if (Array.isArray(body?.monthly_budget_rows)) {
        gasPull.monthly_budget_rows = body.monthly_budget_rows
      }
      if (Array.isArray(body?.past_sales_rows)) {
        gasPull.past_sales_rows = body.past_sales_rows
      }
    } catch {
      // empty body → both
    }
  } else if (url.searchParams.get("direction") === "pull" || url.searchParams.get("direction") === "push") {
    direction = url.searchParams.get("direction") as ReceiptSheetsSyncDirection
  }

  if (viaGas) {
    try {
      const result = await runReceiptSheetsPilotSyncViaGas(supabase, direction, gasPull)
      return json(result, 200)
    } catch (e) {
      console.error("receipt-sheets-sync-cron (via_gas) failed:", e)
      return json({ ok: false, error: String(e), via_gas: true }, 500)
    }
  }

  const config = readReceiptSheetsPilotConfig()
  if (!config) {
    return json({
      ok: true,
      skipped: true,
      reason: "receipt_sheets_pilot_not_configured",
      hint: "Set RECEIPT_SHEETS_PILOT_SPREADSHEET_ID (and share the sheet with the service account).",
    }, 200)
  }

  try {
    const result = await runReceiptSheetsPilotSync(supabase, direction)
    return json(result, 200)
  } catch (e) {
    console.error("receipt-sheets-sync-cron failed:", e)
    return json({
      ok: false,
      error: String(e),
      store_partition_key: config.storePartitionKey,
      spreadsheet_id: config.spreadsheetId,
    }, 500)
  }
})

function isAuthorized(req: Request): boolean {
  const syncSecret = (Deno.env.get("RECEIPT_SHEETS_SYNC_SECRET") ?? "").trim()
  const cronToken = (Deno.env.get("CRON_AUTH_TOKEN") ?? "").trim()
  const authHeader = (req.headers.get("Authorization") ?? "").trim()
  const bearer = authHeader.startsWith("Bearer ") ? authHeader.slice(7).trim() : ""
  const headerKey = (req.headers.get("x-receipt-sheets-sync-key") ?? "").trim()
  if (syncSecret && (bearer === syncSecret || headerKey === syncSecret)) return true
  if (cronToken && bearer === cronToken) return true
  return false
}

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  })
}
