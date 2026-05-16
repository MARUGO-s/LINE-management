alter table public.room_summary_settings
  add column if not exists receipt_report_store_partition_key text null;

comment on column public.room_summary_settings.receipt_report_store_partition_key is
  'Store partition key (line_receipt_entries.store_partition_key) for mid-month and month-end sales LINE reports to this room. Null = infer from room name / room receipts.';
