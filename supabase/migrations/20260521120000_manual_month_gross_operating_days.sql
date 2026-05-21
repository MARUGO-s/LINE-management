-- 過去売上（前年比用手入力）に月間営業日数を追加（途中期間の按分に使用）
alter table public.line_sales_manual_month_gross
  add column if not exists operating_days_count bigint;

do $$
begin
  if not exists (
    select 1 from pg_constraint
    where conname = 'line_sales_manual_operating_days_chk'
      and conrelid = 'public.line_sales_manual_month_gross'::regclass
  ) then
    alter table public.line_sales_manual_month_gross
      add constraint line_sales_manual_operating_days_chk
      check (operating_days_count is null or operating_days_count > 0);
  end if;
end $$;
