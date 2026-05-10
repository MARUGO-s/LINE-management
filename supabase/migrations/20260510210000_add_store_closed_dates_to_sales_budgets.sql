-- 売上予算の日別按分から除外する「店舗休日」（対象月内の YYYY-MM-DD）

alter table public.line_sales_month_budgets
  add column if not exists store_closed_dates text[] not null default '{}'::text[];
