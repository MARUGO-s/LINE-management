-- 新規ルーム作成時、既存ユーザーの会話検索対象はデフォルトで未チェック（除外）にする。
create or replace function public.append_new_room_to_user_message_search_exclusions()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
declare
  normalized_room_id text;
begin
  normalized_room_id := btrim(coalesce(new.room_id, ''));
  if normalized_room_id = '' then
    return new;
  end if;

  update public.line_user_permissions
  set
    excluded_message_search_room_ids = array_append(
      coalesce(excluded_message_search_room_ids, '{}'::text[]),
      normalized_room_id
    ),
    updated_at = now()
  where not (
    normalized_room_id = any(coalesce(excluded_message_search_room_ids, '{}'::text[]))
  );

  return new;
end;
$$;

drop trigger if exists trg_append_new_room_to_user_message_search_exclusions
  on public.room_summary_settings;

create trigger trg_append_new_room_to_user_message_search_exclusions
after insert on public.room_summary_settings
for each row
execute function public.append_new_room_to_user_message_search_exclusions();
