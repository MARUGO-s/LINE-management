alter table public.line_receipt_entries
  add column if not exists store_partition_key text;

update public.line_receipt_entries
set store_partition_key = coalesce(
  nullif(
    regexp_replace(
      regexp_replace(lower(coalesce(store_name, '')), '株式会社ワルツ', '', 'g'),
      '[^0-9a-zぁ-んァ-ヶ一-龠々]',
      '',
      'g'
    ),
    ''
  ),
  'unknown_store'
)
where store_partition_key is null
  or btrim(store_partition_key) = '';

alter table public.line_receipt_entries
  alter column store_partition_key set default 'unknown_store';

alter table public.line_receipt_entries
  alter column store_partition_key set not null;

create index if not exists line_receipt_entries_store_partition_created_idx
  on public.line_receipt_entries(store_partition_key, created_at desc);

create index if not exists line_receipt_entries_room_store_partition_created_idx
  on public.line_receipt_entries(room_id, store_partition_key, created_at desc);
