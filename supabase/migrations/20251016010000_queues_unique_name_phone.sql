-- Adjust uniqueness of queues to be per (name, phone_number_id)
-- Previously: name was globally unique (queues_name_key)
-- Now: allow same name across different phone_number_id values

do $$
begin
  -- Drop old unique constraint on name if it exists
  if exists (
    select 1 from pg_constraint c
    join pg_class t on t.oid = c.conrelid
    join pg_namespace n on n.oid = t.relnamespace
    where c.conname = 'queues_name_key'
      and n.nspname = 'public'
  ) then
    execute 'alter table public.queues drop constraint queues_name_key';
  end if;
end $$;

-- Create new unique constraint on (name, phone_number_id)
alter table public.queues
  add constraint queues_name_phone_key unique (name, phone_number_id);

-- Helpful index retained from previous migration for filtering by phone_number_id
-- (idx_queues_phone_id already exists; keeping this note for clarity)


