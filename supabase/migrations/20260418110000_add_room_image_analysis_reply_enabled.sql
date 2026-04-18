alter table public.room_summary_settings
  add column if not exists image_analysis_reply_enabled boolean not null default true;

comment on column public.room_summary_settings.image_analysis_reply_enabled is
  'When true, reply with image analysis result text after saving an image. Independent from media_file_access_enabled save toggle.';
