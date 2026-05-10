-- 店舗休日を jsonb に統一（text[] の API 表現差で読み取りが空になるのを防ぐ）

alter table public.line_sales_month_budgets
  alter column store_closed_dates drop default;

alter table public.line_sales_month_budgets
  alter column store_closed_dates type jsonb
  using (
    case
      when store_closed_dates is null then '[]'::jsonb
      else to_jsonb(store_closed_dates)
    end
  );

alter table public.line_sales_month_budgets
  alter column store_closed_dates set default '[]'::jsonb;

alter table public.line_sales_month_budgets
  alter column store_closed_dates set not null;
