-- Enable REPLICA IDENTITY FULL for conversations table
-- This allows Supabase Realtime to send all columns in UPDATE events
-- Required for needs_attention changes to propagate to the frontend in real-time

ALTER TABLE public.conversations REPLICA IDENTITY FULL;
