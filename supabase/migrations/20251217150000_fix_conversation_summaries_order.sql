-- Fix: Add ORDER BY to get_conversation_summaries to ensure consistent pagination
-- Without ORDER BY, the Range header pagination returns arbitrary rows,
-- causing some conversations to be "lost" when paginating.

CREATE OR REPLACE FUNCTION get_conversation_summaries(p_phone_number_id text)
RETURNS TABLE (
    wa_id text,
    last_message_time timestamptz,
    sender_name text,
    last_message_text text,
    last_message_direction text,
    last_message_status text,
    proyecto text,
    last_message_model text,
    needs_attention boolean
) AS $$
BEGIN
    RETURN QUERY
    WITH last_messages AS (
        SELECT
            m.wa_id,
            (array_agg(m.id ORDER BY m.created_at DESC))[1] AS last_message_id,
            (array_agg(m.id ORDER BY CASE WHEN m.direction = 'inbound' AND m.sender_name IS NOT NULL THEN m.created_at ELSE '1970-01-01' END DESC NULLS LAST))[1] AS last_inbound_message_id
        FROM messages m
        WHERE m.phone_number_id = p_phone_number_id
        GROUP BY m.wa_id
    )
    SELECT
        lm.wa_id,
        m.created_at AS last_message_time,
        im.sender_name AS sender_name,
        m.message_text AS last_message_text,
        m.direction AS last_message_direction,
        m.status AS last_message_status,
        m.project_name AS proyecto,
        m.model AS last_message_model,
        COALESCE(c.needs_attention, false) AS needs_attention
    FROM last_messages lm
    JOIN messages m ON m.id = lm.last_message_id
    LEFT JOIN messages im ON im.id = lm.last_inbound_message_id
    LEFT JOIN conversations c ON c.wa_id = lm.wa_id AND c.phone_number_id = p_phone_number_id
    ORDER BY m.created_at DESC;  -- Critical: ensures consistent pagination order
END;
$$ LANGUAGE plpgsql;

