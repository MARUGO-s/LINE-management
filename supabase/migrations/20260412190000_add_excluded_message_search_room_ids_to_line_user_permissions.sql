alter table public.line_user_permissions
add column if not exists excluded_message_search_room_ids text[] not null default '{}';

update public.line_user_permissions
set excluded_message_search_room_ids = '{}'
where excluded_message_search_room_ids is null;
