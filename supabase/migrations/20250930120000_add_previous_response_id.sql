-- Add previous_response_id to conversations table
ALTER TABLE conversations
ADD COLUMN previous_response_id TEXT;
