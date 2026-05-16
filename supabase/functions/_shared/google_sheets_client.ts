import { fetchGoogleServiceAccountAccessToken } from "./google_service_account_auth.ts"

const SHEETS_SCOPE = "https://www.googleapis.com/auth/spreadsheets"
const SHEETS_API = "https://sheets.googleapis.com/v4/spreadsheets"

export type SheetValues = string[][]

/** 日本語タブ名などは 'シート名'!A1 形式が必須 */
export function formatSheetA1Range(sheetTabName: string, a1Suffix: string): string {
  const escaped = sheetTabName.replace(/'/g, "''")
  return `'${escaped}'!${a1Suffix}`
}

export async function getSpreadsheetValues(
  spreadsheetId: string,
  rangeA1: string,
): Promise<SheetValues> {
  const accessToken = await fetchGoogleServiceAccountAccessToken([SHEETS_SCOPE])
  const url = new URL(`${SHEETS_API}/${encodeURIComponent(spreadsheetId)}/values/${encodeURIComponent(rangeA1)}`)
  const response = await fetch(url.toString(), {
    headers: { Authorization: `Bearer ${accessToken}` },
  })
  if (!response.ok) {
    const text = await response.text()
    throw new Error(`Sheets values.get failed (${response.status}): ${text}`)
  }
  const json = await response.json()
  const values = json?.values
  return Array.isArray(values) ? values.map((row: unknown) =>
    Array.isArray(row) ? row.map((cell) => String(cell ?? "")) : []
  ) : []
}

export async function updateSpreadsheetValues(
  spreadsheetId: string,
  rangeA1: string,
  values: SheetValues,
): Promise<void> {
  const accessToken = await fetchGoogleServiceAccountAccessToken([SHEETS_SCOPE])
  const url = new URL(
    `${SHEETS_API}/${encodeURIComponent(spreadsheetId)}/values/${encodeURIComponent(rangeA1)}`,
  )
  url.searchParams.set("valueInputOption", "USER_ENTERED")
  const response = await fetch(url.toString(), {
    method: "PUT",
    headers: {
      Authorization: `Bearer ${accessToken}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ values }),
  })
  if (!response.ok) {
    const text = await response.text()
    throw new Error(`Sheets values.update failed (${response.status}): ${text}`)
  }
}

export async function appendSpreadsheetValues(
  spreadsheetId: string,
  rangeA1: string,
  values: SheetValues,
): Promise<void> {
  const accessToken = await fetchGoogleServiceAccountAccessToken([SHEETS_SCOPE])
  const url = new URL(
    `${SHEETS_API}/${encodeURIComponent(spreadsheetId)}/values/${encodeURIComponent(rangeA1)}:append`,
  )
  url.searchParams.set("valueInputOption", "USER_ENTERED")
  url.searchParams.set("insertDataOption", "INSERT_ROWS")
  const response = await fetch(url.toString(), {
    method: "POST",
    headers: {
      Authorization: `Bearer ${accessToken}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ values }),
  })
  if (!response.ok) {
    const text = await response.text()
    throw new Error(`Sheets values.append failed (${response.status}): ${text}`)
  }
}

export async function batchUpdateSpreadsheetValues(
  spreadsheetId: string,
  data: Array<{ range: string; values: SheetValues }>,
): Promise<void> {
  if (data.length === 0) return
  const accessToken = await fetchGoogleServiceAccountAccessToken([SHEETS_SCOPE])
  const url = `${SHEETS_API}/${encodeURIComponent(spreadsheetId)}/values:batchUpdate`
  const response = await fetch(url, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${accessToken}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      valueInputOption: "USER_ENTERED",
      data,
    }),
  })
  if (!response.ok) {
    const text = await response.text()
    throw new Error(`Sheets values.batchUpdate failed (${response.status}): ${text}`)
  }
}
