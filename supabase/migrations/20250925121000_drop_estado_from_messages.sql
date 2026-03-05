-- Drop 'estado_conversacion' from public.messages if exists
do $$
begin
  if exists (
    select 1 from information_schema.columns
    where table_schema = 'public' and table_name = 'messages' and column_name = 'estado_conversacion'
  ) then
    alter table public.messages drop column estado_conversacion;
  end if;
end$$;


