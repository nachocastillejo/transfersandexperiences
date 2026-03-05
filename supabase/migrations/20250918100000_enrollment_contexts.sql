-- Enrollment contexts table for capturing real-time enrollment data per wa_id
create table if not exists public.enrollment_contexts (
  id uuid primary key default gen_random_uuid(),
  wa_id text not null,
  project_name text,
  phone_number_id text,
  context jsonb not null default '{}'::jsonb,
  created_at timestamptz default now(),
  updated_at timestamptz default now(),
  unique (wa_id, project_name, phone_number_id)
);

-- Trigger to auto-update updated_at
create or replace function public.set_updated_at()
returns trigger language plpgsql as $$
begin
  new.updated_at = now();
  return new;
end $$;

drop trigger if exists trg_enrollment_contexts_updated_at on public.enrollment_contexts;
create trigger trg_enrollment_contexts_updated_at
before update on public.enrollment_contexts
for each row execute function public.set_updated_at();

-- Helpful index for wa_id lookups
create index if not exists idx_enrollment_contexts_wa_id on public.enrollment_contexts (wa_id);
create index if not exists idx_enrollment_contexts_phone_id_wa_id on public.enrollment_contexts (phone_number_id, wa_id);

-- Enable Realtime publication
alter publication supabase_realtime add table public.enrollment_contexts;

-- Enable RLS and restrict to authenticated
alter table public.enrollment_contexts enable row level security;

drop policy if exists "authenticated_all_enrollment_contexts" on public.enrollment_contexts;
create policy "authenticated_all_enrollment_contexts" on public.enrollment_contexts
for all using (auth.role() = 'authenticated') with check (auth.role() = 'authenticated');


