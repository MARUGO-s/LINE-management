alter table public.line_receipt_mid_reports
  add column if not exists report_kind text;

update public.line_receipt_mid_reports
set report_kind = case
  when trigger_type in ('month_end_post', 'month_end_fallback') then 'month_end'
  else 'mid_month'
end
where report_kind is null
  or report_kind not in ('mid_month', 'month_end');

alter table public.line_receipt_mid_reports
  alter column report_kind set default 'mid_month';

alter table public.line_receipt_mid_reports
  alter column report_kind set not null;

alter table public.line_receipt_mid_reports
  drop constraint if exists line_receipt_mid_reports_trigger_type_check;

alter table public.line_receipt_mid_reports
  add constraint line_receipt_mid_reports_trigger_type_check
  check (trigger_type in ('day15_post', 'day15_fallback', 'month_end_post', 'month_end_fallback'));

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'line_receipt_mid_reports_report_kind_check'
      and conrelid = 'public.line_receipt_mid_reports'::regclass
  ) then
    alter table public.line_receipt_mid_reports
      add constraint line_receipt_mid_reports_report_kind_check
      check (report_kind in ('mid_month', 'month_end'));
  end if;
end
$$;

drop index if exists line_receipt_mid_reports_room_month_uidx;

create unique index if not exists line_receipt_mid_reports_room_month_kind_uidx
  on public.line_receipt_mid_reports(room_id, report_month, report_kind);
