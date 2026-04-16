create extension if not exists pg_trgm;

create index if not exists tabelog_reservation_visit_events_visit_at_desc_idx
  on public.tabelog_reservation_visit_events (visit_at desc);

create index if not exists ikyu_reservation_visit_events_visit_at_desc_idx
  on public.ikyu_reservation_visit_events (visit_at desc);

create index if not exists tabelog_reservation_visit_events_customer_name_trgm_idx
  on public.tabelog_reservation_visit_events using gin (customer_name gin_trgm_ops);

create index if not exists ikyu_reservation_visit_events_customer_name_trgm_idx
  on public.ikyu_reservation_visit_events using gin (customer_name gin_trgm_ops);

create index if not exists tabelog_reservation_visit_events_reservation_detail_trgm_idx
  on public.tabelog_reservation_visit_events using gin (reservation_detail gin_trgm_ops);

create index if not exists ikyu_reservation_visit_events_reservation_detail_trgm_idx
  on public.ikyu_reservation_visit_events using gin (reservation_detail gin_trgm_ops);
