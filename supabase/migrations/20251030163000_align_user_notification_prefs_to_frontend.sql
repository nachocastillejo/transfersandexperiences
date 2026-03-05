-- Align users notification preference columns with frontend toggles

-- Add precise frontend-driven preference flags
alter table public.users
  add column if not exists system_inbound_enabled boolean not null default false,
  add column if not exists system_enrollment_enabled boolean not null default false,
  add column if not exists email_inbound_enabled boolean not null default false,
  add column if not exists email_enrollment_enabled boolean not null default false,
  add column if not exists sound_enabled boolean not null default false;

-- Remove generic/unnecessary preference columns introduced previously
do $$
begin
  if exists (
    select 1 from information_schema.columns
    where table_schema = 'public' and table_name = 'users' and column_name = 'email_enabled'
  ) then
    execute 'alter table public.users drop column email_enabled';
  end if;

  if exists (
    select 1 from information_schema.columns
    where table_schema = 'public' and table_name = 'users' and column_name = 'notification_frequency'
  ) then
    execute 'alter table public.users drop column notification_frequency';
  end if;

  if exists (
    select 1 from information_schema.columns
    where table_schema = 'public' and table_name = 'users' and column_name = 'notification_categories'
  ) then
    execute 'alter table public.users drop column notification_categories';
  end if;
end $$;






























