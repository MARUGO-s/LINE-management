alter table public.room_summary_settings
add column if not exists gmail_reservation_alert_enabled boolean not null default false;

update public.room_summary_settings
set gmail_reservation_alert_enabled = false
where gmail_reservation_alert_enabled is null;
