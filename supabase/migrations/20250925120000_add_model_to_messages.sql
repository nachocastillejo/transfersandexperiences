-- Add 'model' column to public.messages if it doesn't exist
do $$
begin
  if not exists (
    select 1 from information_schema.columns
    where table_schema = 'public' and table_name = 'messages' and column_name = 'model'
  ) then
    alter table public.messages add column model text;
  end if;
end$$;


