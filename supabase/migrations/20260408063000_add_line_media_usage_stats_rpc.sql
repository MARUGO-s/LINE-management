create or replace function public.get_line_media_usage_stats(
  filter_room_id text default null,
  filter_media_type text default null
)
returns table (
  total_files bigint,
  total_bytes bigint
)
language sql
security definer
set search_path = public
as $$
  select
    count(*)::bigint as total_files,
    coalesce(sum(m.file_size_bytes), 0)::bigint as total_bytes
  from public.line_message_media m
  where (filter_room_id is null or m.room_id = filter_room_id)
    and (
      filter_media_type is null
      or filter_media_type = ''
      or m.media_type = filter_media_type
    );
$$;

revoke all on function public.get_line_media_usage_stats(text, text) from public;
grant execute on function public.get_line_media_usage_stats(text, text) to service_role;
