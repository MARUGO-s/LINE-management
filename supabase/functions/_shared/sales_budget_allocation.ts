import { getJapaneseHolidayDateSet } from "./japanese_holidays.ts"

export type SalesBudgetAllocationWeights = {
  weekday: number
  pre_holiday: number
  holiday: number
}

export type DayKind = "weekday" | "pre_holiday" | "holiday"

/** 暦日を 1 日進める（YYYY-MM-DD、UTC 暦） */
export function addCalendarDaysIso(isoDate: string, deltaDays: number): string {
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(String(isoDate).trim())
  if (!m) return isoDate
  const y = Number(m[1])
  const mo = Number(m[2])
  const d = Number(m[3])
  const t = Date.UTC(y, mo - 1, d)
  const next = new Date(t + deltaDays * 86400000)
  const yy = next.getUTCFullYear()
  const mm = String(next.getUTCMonth() + 1).padStart(2, "0")
  const dd = String(next.getUTCDate()).padStart(2, "0")
  return `${yy}-${mm}-${dd}`
}

export function isHolidayLikeDay(isoDate: string, holidayDates: Set<string>): boolean {
  const dt = new Date(`${isoDate}T12:00:00+09:00`)
  if (Number.isNaN(dt.getTime())) return false
  if (dt.getDay() === 0) return true
  return holidayDates.has(isoDate)
}

export function classifySalesBudgetDay(isoDate: string, holidayDates: Set<string>): DayKind {
  if (isHolidayLikeDay(isoDate, holidayDates)) return "holiday"
  const next = addCalendarDaysIso(isoDate, 1)
  if (isHolidayLikeDay(next, holidayDates)) return "pre_holiday"
  return "weekday"
}

function weightForDayKind(kind: DayKind, w: SalesBudgetAllocationWeights): number {
  if (kind === "holiday") return w.holiday
  if (kind === "pre_holiday") return w.pre_holiday
  return w.weekday
}

export function enumerateMonthDates(targetMonth: string): string[] {
  const m = /^(\d{4})-(\d{2})$/.exec(String(targetMonth).trim())
  if (!m) return []
  const year = Number(m[1])
  const month = Number(m[2])
  const lastDay = new Date(Date.UTC(year, month, 0)).getUTCDate()
  const out: string[] = []
  for (let day = 1; day <= lastDay; day++) {
    out.push(
      `${String(year).padStart(4, "0")}-${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")}`,
    )
  }
  return out
}

/**
 * 月間予算を「平日 / 休日前日 / 休日(日曜+祝)」の重みで日別に按分（端数は最大剰余法）
 */
export function allocateDailyBudgetsForMonth(
  targetMonth: string,
  monthBudgetYen: number,
  weights: SalesBudgetAllocationWeights,
  holidayDates: Set<string>,
): Map<string, number> {
  const days = enumerateMonthDates(targetMonth)
  if (days.length === 0 || monthBudgetYen <= 0) return new Map()

  const kinds = days.map((d) => classifySalesBudgetDay(d, holidayDates))
  const rawWeights = kinds.map((k) => weightForDayKind(k, weights))
  const sumW = rawWeights.reduce((a, b) => a + b, 0)
  if (sumW <= 0) return new Map()

  const fractions = rawWeights.map((rw) => (monthBudgetYen * rw) / sumW)
  const floors = fractions.map((x) => Math.floor(x))
  let allocated = floors.reduce((a, b) => a + b, 0)
  const remainder = monthBudgetYen - allocated
  const remainders = fractions.map((x, i) => ({ i, r: x - floors[i] }))
  remainders.sort((a, b) => b.r - a.r)

  const result = new Map<string, number>()
  for (let i = 0; i < days.length; i++) {
    result.set(days[i], floors[i])
  }
  for (let k = 0; k < remainder && k < remainders.length; k++) {
    const idx = remainders[k].i
    const dateStr = days[idx]
    result.set(dateStr, (result.get(dateStr) ?? 0) + 1)
  }
  return result
}

export function getDailyBudgetForDateFromAllocation(
  targetMonth: string,
  isoDate: string,
  monthBudgetYen: number,
  weights: SalesBudgetAllocationWeights,
  holidayDates: Set<string>,
): number | null {
  const map = allocateDailyBudgetsForMonth(targetMonth, monthBudgetYen, weights, holidayDates)
  const v = map.get(isoDate)
  return v == null ? null : v
}

/** admin-api / line-webhook から共通で利用 */
export function getDefaultJapaneseHolidaySet(): Set<string> {
  return getJapaneseHolidayDateSet()
}
