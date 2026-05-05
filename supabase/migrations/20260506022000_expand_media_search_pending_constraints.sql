alter table public.media_search_pending_confirmations
  drop constraint if exists media_search_pending_confirmations_stage_check;

alter table public.media_search_pending_confirmations
  drop constraint if exists media_search_pending_confirmations_period_months_check;

alter table public.media_search_pending_confirmations
  add constraint media_search_pending_confirmations_stage_check
  check (stage in ('select_period', 'select_category', 'select_item', 'input_keyword', 'expand_confirm'));

alter table public.media_search_pending_confirmations
  add constraint media_search_pending_confirmations_period_months_check
  check (period_months in (0, 1, 3, 6, 12));
