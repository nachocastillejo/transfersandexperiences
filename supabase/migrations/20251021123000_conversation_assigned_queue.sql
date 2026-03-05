-- Store multiple assigned queues directly on conversations as a uuid[] array
do $$
begin
  -- Add array column if missing
  if not exists (
    select 1 from information_schema.columns 
    where table_schema = 'public' and table_name = 'conversations' and column_name = 'assigned_queue_ids'
  ) then
    alter table public.conversations
      add column assigned_queue_ids uuid[] default '{}'::uuid[];
  end if;

  -- Drop legacy single-column if present
  if exists (
    select 1 from information_schema.columns 
    where table_schema = 'public' and table_name = 'conversations' and column_name = 'assigned_queue_id'
  ) then
    alter table public.conversations drop column assigned_queue_id;
  end if;

  -- Optionally drop join table if it was created previously
  if exists (
    select 1 from information_schema.tables
    where table_schema = 'public' and table_name = 'conversation_queues'
  ) then
    drop table public.conversation_queues;
  end if;
end $$;


