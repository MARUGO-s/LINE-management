alter table public.summary_settings
  add column if not exists media_upload_max_mb integer not null default 10;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'summary_settings_media_upload_max_mb_check'
      and conrelid = 'public.summary_settings'::regclass
  ) then
    alter table public.summary_settings
      add constraint summary_settings_media_upload_max_mb_check
      check (media_upload_max_mb >= 1 and media_upload_max_mb <= 20);
  end if;
end;
$$;

update public.summary_settings
set media_upload_max_mb = least(greatest(coalesce(media_upload_max_mb, 10), 1), 20)
where id = 1;
