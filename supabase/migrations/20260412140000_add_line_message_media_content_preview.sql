-- Human-readable snippet from PDF / DOCX / XLSX / plain text (LINE file messages).
alter table public.line_message_media
  add column if not exists content_preview text;

comment on column public.line_message_media.content_preview is
  'Short text extracted from file body (PDF, Word, Excel, text) to help identify the attachment.';
