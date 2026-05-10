-- 売上分析 fetchAnalyticsMonthly: store_partition_key + receipt_date 範囲での検索を高速化
create index if not exists line_receipt_entries_store_partition_receipt_date_idx
  on public.line_receipt_entries (store_partition_key, receipt_date);
