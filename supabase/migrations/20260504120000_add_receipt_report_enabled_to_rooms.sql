alter table public.room_summary_settings
  add column if not exists receipt_midreport_enabled boolean not null default true,
  add column if not exists receipt_monthend_report_enabled boolean not null default true;

comment on column public.room_summary_settings.receipt_midreport_enabled is
  'When true, this room receives the mid-month receipt summary report (15th of each month).';
comment on column public.room_summary_settings.receipt_monthend_report_enabled is
  'When true, this room receives the month-end receipt summary report (last day of each month).';
