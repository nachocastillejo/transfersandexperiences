-- Remove anonymous read access and ensure only authenticated users can access data.

-- Drop the permissive anonymous read policies if they exist.
drop policy if exists "anon_read_messages" on public.messages;
drop policy if exists "anon_read_conversations" on public.conversations;

-- Ensure RLS is enabled.
alter table public.messages enable row level security;
alter table public.conversations enable row level security;

-- Re-apply or create the policy to restrict access to authenticated users.
-- This ensures that even if `0001` was somehow modified, we have the correct policy.
drop policy if exists "authenticated_all_messages" on public.messages;
create policy "authenticated_all_messages" on public.messages
for all using (auth.role() = 'authenticated') with check (auth.role() = 'authenticated');

drop policy if exists "authenticated_all_conversations" on public.conversations;
create policy "authenticated_all_conversations" on public.conversations
for all using (auth.role() = 'authenticated') with check (auth.role() = 'authenticated');
