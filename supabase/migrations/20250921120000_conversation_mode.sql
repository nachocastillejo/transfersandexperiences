-- Add per-conversation mode column and basic constraints
alter table if exists public.conversations
  add column if not exists mode text check (mode in ('bot','agent')) default 'bot';

-- Backfill: set mode based on pause behavior if you have historical hint
-- (No-op here; UI will start writing it)

-- Ensure table is in realtime publication (idempotent)
do $$
begin
  if not exists (
    select 1
    from pg_publication p
    join pg_publication_rel pr on pr.prpubid = p.oid
    join pg_class c on c.oid = pr.prrelid
    join pg_namespace n on n.oid = c.relnamespace
    where p.pubname = 'supabase_realtime'
      and n.nspname = 'public'
      and c.relname = 'conversations'
  ) then
    alter publication supabase_realtime add table public.conversations;
  end if;
end$$;

