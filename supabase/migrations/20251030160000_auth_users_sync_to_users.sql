-- Seed existing users from auth.users and keep users in sync going forward

-- 1) Backfill/seed: create rows in public.users for every auth.users
insert into public.users (id, email)
select u.id, u.email
from auth.users u
on conflict (id) do update set email = excluded.email;

-- 2) Trigger to insert on new auth.users rows
create or replace function public.handle_new_auth_user()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
begin
  insert into public.users (id, email)
  values (new.id, new.email)
  on conflict (id) do update set email = excluded.email;
  return new;
end;
$$;

drop trigger if exists on_auth_user_created on auth.users;
create trigger on_auth_user_created
after insert on auth.users
for each row execute function public.handle_new_auth_user();

-- 3) Trigger to update email changes from auth.users
create or replace function public.handle_auth_user_updated()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
begin
  update public.users
  set email = new.email
  where id = new.id;
  return new;
end;
$$;

drop trigger if exists on_auth_user_updated on auth.users;
create trigger on_auth_user_updated
after update of email on auth.users
for each row execute function public.handle_auth_user_updated();

-- 4) Optional: set a sensible default for categories
alter table public.users
  alter column notification_categories
  set default '{"alerts":true,"mentions":true,"system":true}'::jsonb;






























