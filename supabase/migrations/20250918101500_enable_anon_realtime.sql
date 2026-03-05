-- Permissive read policies for realtime dashboards using anon key
-- NOTE: If you want to restrict later, switch to authenticated and use service role on server.

alter table public.messages enable row level security;
alter table public.conversations enable row level security;
alter table public.enrollment_contexts enable row level security;

drop policy if exists "anon_read_messages" on public.messages;
create policy "anon_read_messages" on public.messages
for select using (true);

drop policy if exists "anon_read_conversations" on public.conversations;
create policy "anon_read_conversations" on public.conversations
for select using (true);

drop policy if exists "anon_read_enrollment" on public.enrollment_contexts;
create policy "anon_read_enrollment" on public.enrollment_contexts
for select using (true);


