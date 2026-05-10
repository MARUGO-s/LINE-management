alter table public.line_sales_month_budgets
  add column if not exists weekday_weight numeric(14, 4) not null default 1
    constraint line_sales_month_budgets_weekday_weight_chk check (weekday_weight > 0),
  add column if not exists pre_holiday_weight numeric(14, 4) not null default 1.5
    constraint line_sales_month_budgets_pre_holiday_weight_chk check (pre_holiday_weight > 0),
  add column if not exists holiday_weight numeric(14, 4) not null default 2
    constraint line_sales_month_budgets_holiday_weight_chk check (holiday_weight > 0);
