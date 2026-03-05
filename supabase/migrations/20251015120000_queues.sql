-- Queues model for agent visibility scoping
create table if not exists public.queues (
  id uuid primary key default gen_random_uuid(),
  name text not null unique,
  modes text[] default '{}', -- allowed conversation modes, e.g. {'bot','agent'}
  statuses text[] default '{}', -- allowed conversation estados, e.g. {'Abierta','Cerrada'}
  attention text check (attention in ('needs','attended')),
  phone_number_id text, -- scope per workspace/phone
  created_by text, -- admin email
  created_at timestamptz default now()
);

create table if not exists public.queue_members (
  queue_id uuid not null references public.queues(id) on delete cascade,
  email text not null,
  created_at timestamptz default now(),
  primary key (queue_id, email)
);

-- Helpful indexes
create index if not exists idx_queue_members_email on public.queue_members(email);
create index if not exists idx_queues_phone_id on public.queues(phone_number_id);


