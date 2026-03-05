-- Status definitions catalog
create table if not exists public.status_definitions (
  id uuid primary key default gen_random_uuid(),
  name text unique not null,
  created_at timestamptz default now()
);

-- Add to realtime publication
alter publication supabase_realtime add table public.status_definitions;

-- Enable RLS
alter table public.status_definitions enable row level security;

-- Policies: allow authenticated full access
drop policy if exists "authenticated_all_status_definitions" on public.status_definitions;
create policy "authenticated_all_status_definitions" on public.status_definitions
for all using (auth.role() = 'authenticated') with check (auth.role() = 'authenticated');

-- Optional: anon read for dashboard realtime (match existing pattern)
drop policy if exists "anon_read_status_definitions" on public.status_definitions;
create policy "anon_read_status_definitions" on public.status_definitions
for select using (true);


