-- 過去売上（前年比用手入力）に会計組数・客数を追加

alter table public.line_sales_manual_month_gross
  add column if not exists party_count bigint,
  add column if not exists guest_count bigint;

do $$
begin
  if not exists (
    select 1 from pg_constraint
    where conname = 'line_sales_manual_party_count_chk'
      and conrelid = 'public.line_sales_manual_month_gross'::regclass
  ) then
    alter table public.line_sales_manual_month_gross
      add constraint line_sales_manual_party_count_chk
      check (party_count is null or party_count >= 0);
  end if;

  if not exists (
    select 1 from pg_constraint
    where conname = 'line_sales_manual_guest_count_chk'
      and conrelid = 'public.line_sales_manual_month_gross'::regclass
  ) then
    alter table public.line_sales_manual_month_gross
      add constraint line_sales_manual_guest_count_chk
      check (guest_count is null or guest_count >= 0);
  end if;
end;
$$;
