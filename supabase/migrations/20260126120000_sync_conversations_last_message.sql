-- Trigger to keep conversations.last_message_at synchronized with latest message
-- This enables the faster get_conversation_summaries_v2 RPC to work correctly

-- Function to update conversation last_message fields when a message is inserted
CREATE OR REPLACE FUNCTION sync_conversation_last_message()
RETURNS TRIGGER AS $$
BEGIN
    -- Only update if this message is newer than current last_message_at
    -- or if last_message_at is NULL
    UPDATE conversations
    SET 
        last_message_at = NEW.created_at,
        last_message_text = NEW.message_text,
        last_direction = NEW.direction,
        updated_at = NOW()
    WHERE 
        wa_id = NEW.wa_id 
        AND phone_number_id = NEW.phone_number_id
        AND (last_message_at IS NULL OR last_message_at < NEW.created_at);
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger on messages table
DROP TRIGGER IF EXISTS trg_sync_conversation_last_message ON messages;
CREATE TRIGGER trg_sync_conversation_last_message
    AFTER INSERT ON messages
    FOR EACH ROW
    EXECUTE FUNCTION sync_conversation_last_message();

-- Backfill existing data: update conversations with latest message info
-- This is a one-time operation to sync existing data
WITH latest_messages AS (
    SELECT DISTINCT ON (wa_id, phone_number_id)
        wa_id,
        phone_number_id,
        created_at,
        message_text,
        direction
    FROM messages
    ORDER BY wa_id, phone_number_id, created_at DESC
)
UPDATE conversations c
SET 
    last_message_at = lm.created_at,
    last_message_text = lm.message_text,
    last_direction = lm.direction
FROM latest_messages lm
WHERE c.wa_id = lm.wa_id 
  AND c.phone_number_id = lm.phone_number_id
  AND (c.last_message_at IS NULL OR c.last_message_at < lm.created_at);

-- Add index on conversations.last_message_at for faster sorting
CREATE INDEX IF NOT EXISTS idx_conversations_last_message_at_desc
ON conversations (phone_number_id, last_message_at DESC NULLS LAST);
