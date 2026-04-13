-- Per-room: receive the multi-room consolidated summary in this chat (mutually exclusive with send_room_summary in app logic).
alter table public.room_summary_settings
  add column if not exists receive_overall_summary_enabled boolean not null default false;

comment on column public.room_summary_settings.receive_overall_summary_enabled is
  'When true, summary-cron sends the same consolidated overall report to this room at global delivery hours. Enforced mutually exclusive with send_room_summary in admin-api/UI.';
