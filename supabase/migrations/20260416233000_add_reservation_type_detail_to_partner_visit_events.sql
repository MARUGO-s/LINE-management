alter table public.tabelog_reservation_visit_events
  add column if not exists reservation_type text,
  add column if not exists reservation_detail text;

alter table public.ikyu_reservation_visit_events
  add column if not exists reservation_type text,
  add column if not exists reservation_detail text;

create or replace function public.record_tabelog_reservation_visit(
  p_gmail_message_id text,
  p_customer_name text,
  p_customer_phone text,
  p_visit_at timestamp with time zone default null,
  p_reservation_type text default null,
  p_reservation_detail text default null
) returns integer
language plpgsql
security definer
set search_path = public
as $$
declare
  v_name text;
  v_phone text;
  v_visit_at timestamp with time zone;
  v_row_count integer;
  v_visit_count integer;
begin
  if nullif(trim(p_gmail_message_id), '') is null then
    return null;
  end if;

  v_name := nullif(trim(p_customer_name), '');
  v_phone := nullif(trim(p_customer_phone), '');
  if v_name is null or v_phone is null then
    return null;
  end if;

  v_visit_at := coalesce(p_visit_at, now());

  insert into public.tabelog_reservation_visit_events (
    gmail_message_id,
    customer_name,
    customer_phone,
    visit_at,
    reservation_type,
    reservation_detail
  ) values (
    p_gmail_message_id,
    v_name,
    v_phone,
    v_visit_at,
    nullif(trim(coalesce(p_reservation_type, '')), ''),
    nullif(trim(coalesce(p_reservation_detail, '')), '')
  )
  on conflict (gmail_message_id) do nothing;

  get diagnostics v_row_count = row_count;

  if v_row_count > 0 then
    insert into public.tabelog_reservation_visit_summaries (
      customer_name,
      customer_phone,
      visit_count,
      last_visit_at,
      created_at,
      updated_at
    ) values (
      v_name,
      v_phone,
      1,
      v_visit_at,
      now(),
      now()
    )
    on conflict (customer_name, customer_phone) do update
      set visit_count = public.tabelog_reservation_visit_summaries.visit_count + 1,
          last_visit_at = greatest(
            coalesce(public.tabelog_reservation_visit_summaries.last_visit_at, excluded.last_visit_at),
            excluded.last_visit_at
          ),
          updated_at = now();
  end if;

  select visit_count
    into v_visit_count
  from public.tabelog_reservation_visit_summaries
  where customer_name = v_name
    and customer_phone = v_phone
  limit 1;

  return coalesce(v_visit_count, 0);
end;
$$;

grant execute on function public.record_tabelog_reservation_visit(text, text, text, timestamp with time zone, text, text) to service_role;

create or replace function public.record_ikyu_reservation_visit(
  p_gmail_message_id text,
  p_customer_name text,
  p_customer_phone text,
  p_visit_at timestamp with time zone default null,
  p_reservation_type text default null,
  p_reservation_detail text default null
) returns integer
language plpgsql
security definer
set search_path = public
as $$
declare
  v_name text;
  v_phone text;
  v_visit_at timestamp with time zone;
  v_row_count integer;
  v_visit_count integer;
begin
  if nullif(trim(p_gmail_message_id), '') is null then
    return null;
  end if;

  v_name := nullif(trim(p_customer_name), '');
  v_phone := nullif(trim(p_customer_phone), '');
  if v_name is null or v_phone is null then
    return null;
  end if;

  v_visit_at := coalesce(p_visit_at, now());

  insert into public.ikyu_reservation_visit_events (
    gmail_message_id,
    customer_name,
    customer_phone,
    visit_at,
    reservation_type,
    reservation_detail
  ) values (
    p_gmail_message_id,
    v_name,
    v_phone,
    v_visit_at,
    nullif(trim(coalesce(p_reservation_type, '')), ''),
    nullif(trim(coalesce(p_reservation_detail, '')), '')
  )
  on conflict (gmail_message_id) do nothing;

  get diagnostics v_row_count = row_count;

  if v_row_count > 0 then
    insert into public.ikyu_reservation_visit_summaries (
      customer_name,
      customer_phone,
      visit_count,
      last_visit_at,
      created_at,
      updated_at
    ) values (
      v_name,
      v_phone,
      1,
      v_visit_at,
      now(),
      now()
    )
    on conflict (customer_name, customer_phone) do update
      set visit_count = public.ikyu_reservation_visit_summaries.visit_count + 1,
          last_visit_at = greatest(
            coalesce(public.ikyu_reservation_visit_summaries.last_visit_at, excluded.last_visit_at),
            excluded.last_visit_at
          ),
          updated_at = now();
  end if;

  select visit_count
    into v_visit_count
  from public.ikyu_reservation_visit_summaries
  where customer_name = v_name
    and customer_phone = v_phone
  limit 1;

  return coalesce(v_visit_count, 0);
end;
$$;

grant execute on function public.record_ikyu_reservation_visit(text, text, text, timestamp with time zone, text, text) to service_role;
