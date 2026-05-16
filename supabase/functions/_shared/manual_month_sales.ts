import { createClient } from "https://esm.sh/@supabase/supabase-js@2.44.0"

export type ManualMonthSalesRecord = {
  gross_sales_yen: number
  party_count: number | null
  guest_count: number | null
}

export type ManualMonthSalesUpsertEntry = {
  sales_month: string
  gross_sales_yen: number | null
  party_count?: number | null
  guest_count?: number | null
}

function parseOptionalNonNegativeInt(value: unknown): number | null {
  if (value === null || value === undefined || value === "") return null
  const n = Number(value)
  if (!Number.isFinite(n) || n < 0) return null
  return Math.round(n)
}

export function parseManualMonthPartyGuestFromUnknown(
  partyRaw: unknown,
  guestRaw: unknown,
): { party_count: number | null; guest_count: number | null } {
  const partyRawStr = String(partyRaw ?? "").trim()
  const guestRawStr = String(guestRaw ?? "").trim()
  return {
    party_count: partyRawStr === "" ? null : parseOptionalNonNegativeInt(partyRawStr),
    guest_count: guestRawStr === "" ? null : parseOptionalNonNegativeInt(guestRawStr),
  }
}

export function manualMonthSalesFromRow(
  row: Record<string, unknown> | null | undefined,
): ManualMonthSalesRecord | null {
  if (!row) return null
  const gross = Number(row.gross_sales_yen)
  if (!Number.isFinite(gross) || gross < 0) return null
  return {
    gross_sales_yen: Math.round(gross),
    party_count: parseOptionalNonNegativeInt(row.party_count),
    guest_count: parseOptionalNonNegativeInt(row.guest_count),
  }
}

export async function fetchManualMonthSales(
  supabase: ReturnType<typeof createClient>,
  storePartitionKey: string,
  salesMonthYyyyMm: string,
): Promise<ManualMonthSalesRecord | null> {
  const key = String(storePartitionKey ?? "").trim().toLowerCase()
  const month = String(salesMonthYyyyMm ?? "").trim().slice(0, 7)
  if (!key || !/^\d{4}-\d{2}$/.test(month)) return null

  const { data, error } = await supabase
    .from("line_sales_manual_month_gross")
    .select("gross_sales_yen, party_count, guest_count")
    .eq("store_partition_key", key)
    .eq("sales_month", month)
    .maybeSingle()

  if (error) {
    console.error(`fetchManualMonthSales failed (store=${key}, month=${month}):`, error.message)
    return null
  }
  return manualMonthSalesFromRow(data as Record<string, unknown> | null)
}

export async function fetchManualMonthSalesMapForStore(
  supabase: ReturnType<typeof createClient>,
  storePartitionKey: string,
  salesMonths: string[],
): Promise<Map<string, ManualMonthSalesRecord>> {
  const key = String(storePartitionKey ?? "").trim().toLowerCase()
  const months = [...new Set(
    salesMonths.map((m) => String(m ?? "").trim().slice(0, 7)).filter((m) => /^\d{4}-\d{2}$/.test(m)),
  )]
  const out = new Map<string, ManualMonthSalesRecord>()
  if (!key || months.length === 0) return out

  const { data, error } = await supabase
    .from("line_sales_manual_month_gross")
    .select("sales_month, gross_sales_yen, party_count, guest_count")
    .eq("store_partition_key", key)
    .in("sales_month", months)

  if (error) {
    console.error(`fetchManualMonthSalesMapForStore failed (store=${key}):`, error.message)
    return out
  }

  for (const row of Array.isArray(data) ? data : []) {
    const r = row as Record<string, unknown>
    const sm = String(r.sales_month ?? "").trim().slice(0, 7)
    const parsed = manualMonthSalesFromRow(r)
    if (parsed) out.set(sm, parsed)
  }
  return out
}

export async function upsertManualMonthSalesEntries(
  supabase: ReturnType<typeof createClient>,
  storePartitionKey: string,
  entries: ManualMonthSalesUpsertEntry[],
): Promise<void> {
  const key = String(storePartitionKey ?? "").trim().toLowerCase()
  const updatedAt = new Date().toISOString()

  for (const entry of entries) {
    const salesMonth = String(entry.sales_month ?? "").trim().slice(0, 7)
    if (!/^\d{4}-\d{2}$/.test(salesMonth)) continue

    if (entry.gross_sales_yen === null) {
      const { error } = await supabase
        .from("line_sales_manual_month_gross")
        .delete()
        .eq("store_partition_key", key)
        .eq("sales_month", salesMonth)
      if (error) throw new Error(error.message)
      continue
    }

    const party = entry.party_count === undefined
      ? null
      : parseOptionalNonNegativeInt(entry.party_count)
    const guest = entry.guest_count === undefined
      ? null
      : parseOptionalNonNegativeInt(entry.guest_count)

    const { error } = await supabase
      .from("line_sales_manual_month_gross")
      .upsert(
        {
          store_partition_key: key,
          sales_month: salesMonth,
          gross_sales_yen: Math.round(entry.gross_sales_yen),
          party_count: party,
          guest_count: guest,
          updated_at: updatedAt,
        },
        { onConflict: "store_partition_key,sales_month" },
      )
    if (error) throw new Error(error.message)
  }
}
