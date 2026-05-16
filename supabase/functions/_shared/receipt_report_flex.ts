import { createClient } from "https://esm.sh/@supabase/supabase-js@2.44.0"
import type { ReceiptReportAggregate } from "./receipt_report_aggregate.ts"
import {
  buildReceiptBudgetComparisonRows,
  type ReceiptBudgetFlexRow,
} from "./receipt_budget_comparison.ts"

function formatYenAmount(value: number): string {
  return `¥${Math.round(value).toLocaleString("ja-JP")}`
}

function formatAverageCount(value: number | null): string {
  if (value == null || !Number.isFinite(value)) return "-"
  if (Math.abs(value - Math.round(value)) < 0.0001) return String(Math.round(value))
  return value.toFixed(2).replace(/\.?0+$/, "")
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
