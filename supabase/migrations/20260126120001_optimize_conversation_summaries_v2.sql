-- Optimized get_conversation_summaries_v2 that works with synced conversations table
-- Much faster than v1 because it doesn't need to aggregate all messages

CREATE OR REPLACE FUNCTION get_conversation_summaries_v2(
    p_phone_number_id text,
    p_limit int default 50,
    p_offset int default 0
)
RETURNS TABLE (
    wa_id text,
    last_message_time timestamptz,
    sender_name text,
    last_message_text text,
    last_message_direction text,
    last_message_status text,
    proyecto text,
    last_message_model text,
    needs_attention boolean,
    mode text,
    estado_conversacion text,
    assigned_queue_ids uuid[]
)
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
    RETURN QUERY
    SELECT
        c.wa_id,
        c.last_message_at as last_message_time,
        -- Subquery for sender_name (most recent inbound name)
        (
            SELECT m.sender_name
            FROM messages m
            WHERE m.wa_id = c.wa_id
              AND m.phone_number_id = c.phone_number_id
              AND m.direction = 'inbound'
              AND m.sender_name IS NOT NULL
            ORDER BY m.created_at DESC
            LIMIT 1
        ) as sender_name,
        c.last_message_text,
        c.last_direction as last_message_direction,
        -- Status from the latest message
        (
            SELECT m.status
            FROM messages m
            WHERE m.wa_id = c.wa_id
              AND m.phone_number_id = c.phone_number_id
            ORDER BY m.created_at DESC
            LIMIT 1
        ) as last_message_status,
        c.project_name as proyecto,
        -- Model from the latest message
        (
            SELECT m.model
            FROM messages m
            WHERE m.wa_id = c.wa_id
              AND m.phone_number_id = c.phone_number_id
            ORDER BY m.created_at DESC
            LIMIT 1
        ) as last_message_model,
        COALESCE(c.needs_attention, false),
        COALESCE(c.mode, 'bot'),
        c.estado_conversacion,
        c.assigned_queue_ids
    FROM conversations c
    WHERE (p_phone_number_id IS NULL OR c.phone_number_id = p_phone_number_id)
      AND c.last_message_at IS NOT NULL  -- Only show conversations with messages
    ORDER BY c.last_message_at DESC NULLS LAST
    LIMIT p_limit OFFSET p_offset;
END;
$$;
