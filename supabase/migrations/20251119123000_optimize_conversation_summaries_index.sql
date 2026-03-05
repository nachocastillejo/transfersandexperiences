-- Index to speed up get_conversation_summaries RPC
-- It helps Postgres find the latest message per (phone_number_id, wa_id)
-- much faster by scanning in created_at DESC order.

CREATE INDEX IF NOT EXISTS idx_messages_phone_wa_created_desc
ON public.messages (phone_number_id, wa_id, created_at DESC);


