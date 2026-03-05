-- Conversations and Messages schema with Realtime and basic RLS
create extension if not exists pgcrypto;

create table if not exists public.conversations (
  id uuid primary key default gen_random_uuid(),
  wa_id text not null,
  project_name text,
  phone_number_id text,
  last_message_at timestamptz,
  last_message_text text,
  last_direction text check (last_direction in ('inbound','outbound_bot','outbound_agent','outbound_system')),
  unread_count int default 0,
  created_at timestamptz default now(),
  updated_at timestamptz default now(),
  unique (wa_id, project_name, phone_number_id)
);

create table if not exists public.messages (
  id bigserial primary key,
  created_at timestamptz default now(),
  project_name text,
  sender_name text,
  wa_id text not null,
  phone_number_id text,
  direction text not null check (direction in ('inbound','outbound_bot','outbound_agent','outbound_system')),
  message_text text,
  whatsapp_message_id text unique,
  status text check (status in ('sent','delivered','read','failed','ignored_paused')),
  response_time_seconds numeric,
  attempt_count int,
  required_action text,
  error_message text,
  estado_conversacion text,
  conversation_id uuid references public.conversations(id) on delete cascade
);

create index if not exists idx_messages_wa_id_created_at on public.messages (wa_id, created_at);
create index if not exists idx_messages_phone_id_created_at on public.messages (phone_number_id, created_at);
create index if not exists idx_messages_whatsapp_id on public.messages (whatsapp_message_id);
create index if not exists idx_messages_conversation_id_created_at on public.messages (conversation_id, created_at);
create index if not exists idx_conversations_wa_id on public.conversations (wa_id);
create index if not exists idx_conversations_phone_id_wa_id on public.conversations (phone_number_id, wa_id);

create or replace function public.set_updated_at()
returns trigger language plpgsql as $$
begin
  new.updated_at = now();
  return new;
end $$;

drop trigger if exists trg_conversations_updated_at on public.conversations;
create trigger trg_conversations_updated_at
before update on public.conversations
for each row execute function public.set_updated_at();

-- Realtime publication
alter publication supabase_realtime add table public.messages;
alter publication supabase_realtime add table public.conversations;

-- Enable RLS; temporary permissive policy for authenticated users
alter table public.messages enable row level security;
alter table public.conversations enable row level security;

drop policy if exists "authenticated_all_messages" on public.messages;
create policy "authenticated_all_messages" on public.messages
for all using (auth.role() = 'authenticated') with check (auth.role() = 'authenticated');

drop policy if exists "authenticated_all_conversations" on public.conversations;
create policy "authenticated_all_conversations" on public.conversations
for all using (auth.role() = 'authenticated') with check (auth.role() = 'authenticated');


