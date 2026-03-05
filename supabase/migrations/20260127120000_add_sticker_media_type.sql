-- Add 'sticker' to the media_type check constraint
-- Stickers are WebP images sent via WhatsApp

-- Drop the existing constraint
ALTER TABLE public.messages DROP CONSTRAINT IF EXISTS messages_media_type_check;

-- Add the new constraint with 'sticker' included
ALTER TABLE public.messages 
ADD CONSTRAINT messages_media_type_check 
CHECK (media_type IN ('image', 'video', 'audio', 'document', 'sticker'));

-- Update comment
COMMENT ON COLUMN public.messages.media_type IS 'Type of media attachment: image, video, audio, document, or sticker';
