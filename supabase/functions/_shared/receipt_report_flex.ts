import { createClient } from "https://esm.sh/@supabase/supabase-js@2.44.0"
import type { ReceiptReportAggregate } from "./receipt_report_aggregate.ts"
import { fetchManualMonthSales } from "./manual_month_sales.ts"
import {
  isFullCalendarMonthPeriod,
  loadReceiptReportAggregateForStoreByReceiptDate,
  shiftIsoDateByYears,
} from "./receipt_report_aggregate.ts"
import {
  buildReceiptBudgetComparisonRows,
  type ReceiptBudgetFlexRow,
} from "./receipt_budget_comparison.ts"

export type ReceiptReportYoyComparison = {
  priorPeriodStartDate: string
  priorPeriodEndDate: string
  priorGrossSalesYen: number | null
  priorPartyCount: number | null
  priorGuestCount: number | null
}

function formatYoyPercentChange(current: number, prior: number): { text: string; color: string } | null {
  if (!Number.isFinite(prior) || prior <= 0) return null
  const pct = ((current - prior) / prior) * 100
  const sign = pct >= 0 ? "+" : ""
  const color = pct > 0 ? "#0a7c42" : pct < 0 ? "#c62828" : "#888888"
  return { text: ` ${sign}${pct.toFixed(1)}%`, color }
}

/** 報告期間の前年同日付範囲を集計（暦月全体のとき手入力を優先） */
export async function loadReceiptReportYoyComparison(
  supabase: ReturnType<typeof createClient>,
  storePartitionKey: string,
  periodStartDate: string,
  periodEndDate: string,
): Promise<ReceiptReportYoyComparison | null> {
  const key = String(storePartitionKey ?? "").trim().toLowerCase()
  if (!key) return null

  const priorStart = shiftIsoDateByYears(periodStartDate, -1)
  const priorEnd = shiftIsoDateByYears(periodEndDate, -1)
  if (!priorStart || !priorEnd) return null

  const priorAggregate = await loadReceiptReportAggregateForStoreByReceiptDate(
    supabase,
    key,
    priorStart,
    priorEnd,
  )

  let priorGrossSalesYen: number | null = priorAggregate?.totalGrossSalesYen ?? null
  let priorPartyCount: number | null = priorAggregate?.totalPartyCount ?? null
  let priorGuestCount: number | null = priorAggregate?.totalGuestCount ?? null

  if (isFullCalendarMonthPeriod(periodStartDate, periodEndDate)) {
    const priorMonth = periodStartDate.slice(0, 7)
    const priorYearMonth = `${Number(priorMonth.slice(0, 4)) - 1}-${priorMonth.slice(5, 7)}`
    const manual = await fetchManualMonthSales(supabase, key, priorYearMonth)
    if (manual) {
      priorGrossSalesYen = manual.gross_sales_yen
      if (manual.party_count != null) priorPartyCount = manual.party_count
      if (manual.guest_count != null) priorGuestCount = manual.guest_count
    }
  }

  return {
    priorPeriodStartDate: priorStart,
    priorPeriodEndDate: priorEnd,
    priorGrossSalesYen,
    priorPartyCount,
    priorGuestCount,
  }
}

function formatYenAmount(value: number): string {
  return `¥${Math.round(value).toLocaleString("ja-JP")}`
}

function formatAverageCount(value: number | null): string {
  if (value == null || !Number.isFinite(value)) return "-"
  const rounded = Math.round(value * 10) / 10
  if (Math.abs(rounded - Math.round(rounded)) < 0.001) return String(Math.round(rounded))
  return rounded.toFixed(1)
}

/** 例: 25 組（2.5 組/日） */
function formatTotalWithDailyAverage(
  total: number,
  dailyAvg: number | null,
  unit: string,
  dailySuffix: string,
): string {
  const base = `${total.toLocaleString("ja-JP")} ${unit}`
  const avg = formatAverageCount(dailyAvg)
  if (avg === "-") return base
  return `${base}（${avg} ${dailySuffix}）`
}

function flexBaselineRow(label: string, value: string, valueColor = "#1F1F1F"): Record<string, unknown> {
  return {
    type: "box",
    layout: "baseline",
    spacing: "sm",
    contents: [
      { type: "text", text: label, size: "sm", color: "#888888", flex: 4, wrap: false },
      {
        type: "text",
        text: value,
        size: "sm",
        color: valueColor,
        flex: 6,
        wrap: true,
        weight: "bold",
      },
    ],
  }
}

function formatSignedYenDiffText(diffYen: number): string {
  const x = Math.round(diffYen)
  const absStr = `¥${Math.abs(x).toLocaleString("ja-JP")}`
  if (x > 0) return `（+${absStr}）`
  if (x < 0) return `（-${absStr}）`
  return "（±¥0）"
}

function formatSignedCountDiffText(diff: number, unit: string): string {
  const x = Math.round(diff)
  const absStr = Math.abs(x).toLocaleString("ja-JP")
  if (x > 0) return `（+${absStr}${unit}）`
  if (x < 0) return `（-${absStr}${unit}）`
  return `（±0${unit}）`
}

function flexYoyMetricRow(
  label: string,
  current: number,
  prior: number | null,
  formatAbsDiff?: (diff: number) => string,
): Record<string, unknown> {
  if (prior == null || prior <= 0) return flexBaselineRow(label, "—", "#888888")
  const change = formatYoyPercentChange(current, prior)
  if (!change) return flexBaselineRow(label, "—", "#888888")
  const pctText = change.text.trim()
  const absDiff = formatAbsDiff ? formatAbsDiff(Math.round(current) - Math.round(prior)) : null
  if (!absDiff) return flexBaselineRow(label, pctText, change.color)
  return {
    type: "box",
    layout: "baseline",
    spacing: "sm",
    contents: [
      { type: "text", text: label, size: "sm", color: "#888888", flex: 4, wrap: false },
      {
        type: "text",
        size: "sm",
        wrap: true,
        weight: "bold",
        flex: 6,
        contents: [
          { type: "span", text: pctText, color: change.color },
          { type: "span", text: absDiff, color: "#666666" },
        ],
      },
    ],
  }
}

function appendReceiptReportYoySection(
  bodyContents: Array<Record<string, unknown>>,
  aggregate: ReceiptReportAggregate,
  yoy: ReceiptReportYoyComparison,
): void {
  const hasAnyPrior = (yoy.priorGrossSalesYen ?? 0) > 0
    || (yoy.priorPartyCount ?? 0) > 0
    || (yoy.priorGuestCount ?? 0) > 0
  if (!hasAnyPrior) return

  bodyContents.push(flexSectionDivider())
  bodyContents.push({
    type: "text",
    text: "【前年同月比】",
    size: "sm",
    weight: "bold",
    color: "#7A7A7A",
    margin: "md",
    wrap: true,
  })
  bodyContents.push(
    flexBaselineRow(
      "昨年差異日",
      `${yoy.priorPeriodStartDate}〜${yoy.priorPeriodEndDate}`,
      "#666666",
    ),
  )
  bodyContents.push(
    flexYoyMetricRow(
      "売上",
      aggregate.totalGrossSalesYen,
      yoy.priorGrossSalesYen,
      formatSignedYenDiffText,
    ),
    flexYoyMetricRow(
      "組数",
      aggregate.totalPartyCount,
      yoy.priorPartyCount,
      (d) => formatSignedCountDiffText(d, "組"),
    ),
    flexYoyMetricRow(
      "客数",
      aggregate.totalGuestCount,
      yoy.priorGuestCount,
      (d) => formatSignedCountDiffText(d, "名"),
    ),
  )
}

function flexBudgetRow(row: ReceiptBudgetFlexRow): Record<string, unknown> {
  const suffix = row.valueSuffix
  if (suffix) {
    return {
      type: "box",
      layout: "baseline",
      spacing: "sm",
      contents: [
        { type: "text", text: row.label, size: "sm", color: "#888888", flex: 4, wrap: false },
        {
          type: "text",
          size: "sm",
          wrap: true,
          weight: "bold",
          flex: 6,
          contents: [
            { type: "span", text: row.value, color: "#1F1F1F" },
            { type: "span", text: suffix.text, color: suffix.color },
          ],
        },
      ],
    }
  }
  return flexBaselineRow(row.label, row.value, row.valueColor ?? "#1F1F1F")
}

function flexSectionDivider(): Record<string, unknown> {
  return {
    type: "text",
    text: "────────",
    size: "xxs",
    color: "#AAAAAA",
    margin: "md",
  }
}

export type BuildReceiptReportFlexOpts = {
  reportTitle: string
  periodStartDate: string
  periodEndDate: string
  storePartitionKey?: string | null
}

export async function buildReceiptReportFlexMessages(
  supabase: ReturnType<typeof createClient> | null,
  aggregate: ReceiptReportAggregate,
  opts: BuildReceiptReportFlexOpts,
): Promise<Array<Record<string, unknown>>> {
  const adminToken = Deno.env.get("ADMIN_DASHBOARD_TOKEN") ?? ""
  const dashboardUri =
    `https://marugo-s.github.io/LINE-management/analytics.html${adminToken ? `?t=${encodeURIComponent(adminToken)}` : ""}`

  const avgUnit = aggregate.avgGrossSalesYen == null
    ? null
    : (aggregate.totalGuestCount > 0
      ? Math.round(aggregate.totalGrossSalesYen / aggregate.totalGuestCount)
      : null)

  const altText =
    `【${opts.reportTitle}】${opts.periodStartDate}〜${opts.periodEndDate} 総売上: ${formatYenAmount(aggregate.totalGrossSalesYen)}`

  const bodyContents: Array<Record<string, unknown>> = [
    flexBaselineRow("総売上", formatYenAmount(aggregate.totalGrossSalesYen)),
    flexBaselineRow(
      "組数合計",
      formatTotalWithDailyAverage(aggregate.totalPartyCount, aggregate.avgPartyCount, "組", "組/日"),
    ),
    flexBaselineRow(
      "客数合計",
      formatTotalWithDailyAverage(aggregate.totalGuestCount, aggregate.avgGuestCount, "名", "名/日"),
    ),
  ]
  if (avgUnit != null) {
    bodyContents.push(flexBaselineRow("客単価", formatYenAmount(avgUnit)))
  }
  bodyContents.push(
    flexBaselineRow(
      "1日平均売上",
      aggregate.avgDailyGrossSalesYen == null ? "-" : formatYenAmount(aggregate.avgDailyGrossSalesYen),
    ),
    flexBaselineRow("レシート", `${aggregate.receiptCount.toLocaleString("ja-JP")} 件`),
  )

  const storeKey = String(opts.storePartitionKey ?? "").trim().toLowerCase()
  if (supabase && storeKey) {
    const yoy = await loadReceiptReportYoyComparison(
      supabase,
      storeKey,
      opts.periodStartDate,
      opts.periodEndDate,
    )
    if (yoy) appendReceiptReportYoySection(bodyContents, aggregate, yoy)

    const receiptMonthYyyyMm = opts.periodStartDate.slice(0, 7)
    const budgetRows = await buildReceiptBudgetComparisonRows(supabase, {
      storePartitionKey: storeKey,
      asOfDateIso: opts.periodEndDate,
      receiptMonthYyyyMm,
      monthActualYen: aggregate.totalGrossSalesYen,
      budgetCumulativeThroughDate: opts.periodEndDate,
      omitDailyBudgetLines: true,
    })
    if (budgetRows && budgetRows.length > 0) {
      bodyContents.push(flexSectionDivider())
      bodyContents.push({
        type: "text",
        text: "【予算】",
        size: "sm",
        weight: "bold",
        color: "#7A7A7A",
        margin: "md",
        wrap: true,
      })
      for (const br of budgetRows) {
        bodyContents.push(flexBudgetRow(br))
      }
    }
  }

  return [{
    type: "flex",
    altText: altText.slice(0, 400),
    contents: {
      type: "bubble",
      header: {
        type: "box",
        layout: "vertical",
        paddingTop: "md",
        paddingBottom: "md",
        paddingStart: "md",
        paddingEnd: "md",
        backgroundColor: "#006c3a",
        contents: [
          { type: "text", text: `📊 ${opts.reportTitle}`, size: "lg", weight: "bold", color: "#FFFFFF" },
          {
            type: "text",
            text: `${opts.periodStartDate}〜${opts.periodEndDate}`,
            size: "xs",
            color: "#CCFFDD",
            margin: "sm",
          },
        ],
      },
      body: {
        type: "box",
        layout: "vertical",
        spacing: "sm",
        paddingTop: "md",
        paddingBottom: "md",
        paddingStart: "md",
        paddingEnd: "md",
        contents: bodyContents,
      },
      footer: {
        type: "box",
        layout: "vertical",
        spacing: "sm",
        paddingTop: "md",
        paddingBottom: "md",
        paddingStart: "md",
        paddingEnd: "md",
        contents: [{
          type: "button",
          style: "secondary",
          height: "sm",
          action: { type: "uri", label: "📈 売上推移を見る", uri: dashboardUri },
        }],
      },
    },
  }]
}
