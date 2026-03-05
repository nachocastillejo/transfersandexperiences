-- Users table with roles and notification preferences

-- 1) Enum for roles (admin | agent)
do $$
begin
  if not exists (select 1 from pg_type where typname = 'user_role') then
    create type public.user_role as enum ('admin','agent');
  end if;
end $$;

-- 2) Updated-at trigger helper (idempotent)
create or replace function public.set_updated_at()
returns trigger
language plpgsql
as $$
begin
  new.updated_at = now();
  return new;
end
$$;

-- 3) Users table (profiles + notification preferences)
create table if not exists public.users (
  id uuid primary key references auth.users(id) on delete cascade,
  email text unique,
  role public.user_role not null default 'agent',
  is_active boolean not null default true,
  timezone text default 'UTC',
  locale text default 'es-ES',

  -- Notification preferences (MVP unified in users)
  email_enabled boolean not null default true,
  notification_frequency text not null default 'immediate', -- 'immediate' | 'daily' | 'weekly'
  quiet_hours jsonb,                      -- e.g. {"start":"22:00","end":"08:00"}
  notification_categories jsonb,          -- e.g. {"alerts":true,"mentions":true,"system":false}

  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

-- Ensure updated_at maintained
do $$
begin
  if not exists (
    select 1 from pg_trigger
    where tgname = 'set_users_updated_at'
  ) then
    create trigger set_users_updated_at
    before update on public.users
    for each row execute procedure public.set_updated_at();
  end if;
end $$;

-- Helpful index for lookups by email (in addition to unique)
create index if not exists idx_users_email on public.users(email);

-- 4) RLS and policies
alter table public.users enable row level security;

-- Select: allow authenticated to read users (adjust if you need stricter)
do $$
begin
  if not exists (
    select 1 from pg_policies
    where schemaname = 'public' and tablename = 'users' and policyname = 'Users can read all users'
  ) then
    create policy "Users can read all users"
      on public.users for select
      to authenticated
      using (true);
  end if;
end $$;

-- Insert: user can insert their own profile (id must equal auth.uid())
do $$
begin
  if not exists (
    select 1 from pg_policies
    where schemaname = 'public' and tablename = 'users' and policyname = 'Users can insert self'
  ) then
    create policy "Users can insert self"
      on public.users for insert
      to authenticated
      with check (id = auth.uid());
  end if;
end $$;

-- Update: user can update self
do $$
begin
  if not exists (
    select 1 from pg_policies
    where schemaname = 'public' and tablename = 'users' and policyname = 'Users can update self'
  ) then
    create policy "Users can update self"
      on public.users for update
      to authenticated
      using (id = auth.uid());
  end if;
end $$;

-- Update: admins can update anyone
do $$
begin
  if not exists (
    select 1 from pg_policies
    where schemaname = 'public' and tablename = 'users' and policyname = 'Admins can update anyone'
  ) then
    create policy "Admins can update anyone"
      on public.users for update
      to authenticated
      using (
        exists (
          select 1 from public.users me
          where me.id = auth.uid() and me.role = 'admin'
        )
      );
  end if;
end $$;


