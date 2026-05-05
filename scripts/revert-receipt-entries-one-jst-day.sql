-- Remove receipt rows for one Japan-local calendar day so sales totals match reality again.
-- Dashboard analytics and admin-api sums use public.line_receipt_entries.
--
-- Run in: Supabase Dashboard → SQL Editor → connect to Primary database (postgres).
--
-- Edit DATE '2026-05-05' if you need a different day.

-- -----------------------------------------------------------------------------
-- STEP 1 — Preview only (safe to run)
-- -----------------------------------------------------------------------------
select
  id,
  room_id,
  line_message_id,
  store_name,
  gross_sales_yen,
  party_count,
  guest_count,
  created_at
from public.line_receipt_entries
where (created_at at time zone 'Asia/Tokyo')::date = date '2026-05-05'
order by created_at asc;

select count(*) as rows_to_delete
from public.line_receipt_entries
where (created_at at time zone 'Asia/Tokyo')::date = date '2026-05-05';

-- -----------------------------------------------------------------------------
-- STEP 2 — Delete (run only after STEP 1 looks correct)
-- -----------------------------------------------------------------------------
-- begin;
-- delete from public.line_receipt_entries
-- where (created_at at time zone 'Asia/Tokyo')::date = date '2026-05-05';
-- commit;
