-- line_message_media の投稿者表示名を既存ユーザー権限テーブルから補完する
-- （過去データで「投稿者: （記録なし）」になっている行を対象）

update public.line_message_media as m
set
  sender_display_name = trim(p.display_name)
from public.line_user_permissions as p
where m.user_id is not null
  and m.user_id = p.line_user_id
  and coalesce(trim(m.sender_display_name), '') = ''
  and coalesce(trim(p.display_name), '') <> '';
