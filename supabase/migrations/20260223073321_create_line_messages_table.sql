create table public.line_messages (
    id uuid default gen_random_uuid() primary key,
    room_id text not null,
    user_id text,
    content text not null,
    created_at timestamp with time zone default now() not null,
    processed boolean default false not null
);

-- Index to quickly find unprocessed messages for cron job
create index line_messages_processed_idx on public.line_messages(processed) where processed = false;
-- Index to quickly filter messages by room_id if needed
create index line_messages_room_id_idx on public.line_messages(room_id);

-- Enable RLS (though for edge functions using service role it might be bypassed, good practice)
alter table public.line_messages enable row level security;
