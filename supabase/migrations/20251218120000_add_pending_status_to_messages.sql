-- Add 'pending' to the status check constraint for template messages awaiting webhook confirmation
-- This allows messages to be stored immediately when sent, then updated when webhook confirms delivery

-- Drop the existing constraint
ALTER TABLE public.messages DROP CONSTRAINT IF EXISTS messages_status_check;

-- Add the new constraint with 'pending' included
ALTER TABLE public.messages ADD CONSTRAINT messages_status_check 
  CHECK (status IN ('pending', 'sent', 'delivered', 'read', 'failed', 'ignored_paused'));

-- Add index for efficient lookup of pending messages by whatsapp_message_id
-- This will be used by webhooks to find and update pending messages
CREATE INDEX IF NOT EXISTS idx_messages_pending_wamid 
  ON public.messages (whatsapp_message_id) 
  WHERE status = 'pending';
