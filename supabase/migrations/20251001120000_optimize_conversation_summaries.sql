create or replace function get_conversation_summaries(p_phone_number_id text)
returns table (
    wa_id text,
    last_message_time timestamptz,
    sender_name text,
    last_message_text text,
    last_message_direction text,
    last_message_status text,
    proyecto text,
    last_message_model text
) as $$
begin
    return query
    with last_messages as (
        select
            m.wa_id,
            (array_agg(m.id order by m.created_at desc))[1] as last_message_id,
            (array_agg(m.id order by case when m.direction = 'inbound' and m.sender_name is not null then m.created_at else '1970-01-01' end desc nulls last))[1] as last_inbound_message_id
        from messages m
        where m.phone_number_id = p_phone_number_id
        group by m.wa_id
    )
    select
        lm.wa_id,
        m.created_at as last_message_time,
        im.sender_name as sender_name,
        m.message_text as last_message_text,
        m.direction as last_message_direction,
        m.status as last_message_status,
        m.project_name as proyecto,
        m.model as last_message_model
    from last_messages lm
    join messages m on m.id = lm.last_message_id
    left join messages im on im.id = lm.last_inbound_message_id;
end;
$$ language plpgsql;
