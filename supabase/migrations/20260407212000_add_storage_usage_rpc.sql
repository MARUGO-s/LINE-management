create or replace function public.get_storage_usage_stats()
returns jsonb
language plpgsql
security definer
set search_path = public, pg_catalog
as $$
declare
  db_size_bytes bigint;
  managed_total_bytes bigint;
  managed_tables jsonb;
begin
  select pg_database_size(current_database())
    into db_size_bytes;

  select coalesce(sum(pg_total_relation_size(c.oid)), 0)
    into managed_total_bytes
  from pg_class c
  join pg_namespace n on n.oid = c.relnamespace
  where n.nspname = 'public'
    and c.relkind = 'r'
    and c.relname in (
      'line_messages',
      'summary_delivery_logs',
      'room_summary_settings',
      'summary_settings',
      'calendar_pending_confirmations'
    );

  select coalesce(
      jsonb_agg(
        jsonb_build_object(
          'table_name', c.relname,
          'size_bytes', pg_total_relation_size(c.oid),
          'size_pretty', pg_size_pretty(pg_total_relation_size(c.oid))
        )
        order by pg_total_relation_size(c.oid) desc
      ),
      '[]'::jsonb
    )
    into managed_tables
  from pg_class c
  join pg_namespace n on n.oid = c.relnamespace
  where n.nspname = 'public'
    and c.relkind = 'r'
    and c.relname in (
      'line_messages',
      'summary_delivery_logs',
      'room_summary_settings',
      'summary_settings',
      'calendar_pending_confirmations'
    );

  return jsonb_build_object(
    'database_size_bytes', db_size_bytes,
    'database_size_pretty', pg_size_pretty(db_size_bytes),
    'managed_tables_total_bytes', managed_total_bytes,
    'managed_tables_total_pretty', pg_size_pretty(managed_total_bytes),
    'managed_tables', managed_tables
  );
end;
$$;

revoke all on function public.get_storage_usage_stats() from public;
grant execute on function public.get_storage_usage_stats() to service_role;
