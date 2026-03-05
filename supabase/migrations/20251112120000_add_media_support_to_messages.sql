-- Add media support columns to messages table
ALTER TABLE public.messages 
ADD COLUMN IF NOT EXISTS media_type TEXT CHECK (media_type IN ('image', 'video', 'audio', 'document')),
ADD COLUMN IF NOT EXISTS media_url TEXT,
ADD COLUMN IF NOT EXISTS media_filename TEXT,
ADD COLUMN IF NOT EXISTS media_mime_type TEXT,
ADD COLUMN IF NOT EXISTS media_size_bytes BIGINT;

-- Add index for media queries
CREATE INDEX IF NOT EXISTS idx_messages_media_type ON public.messages (media_type) WHERE media_type IS NOT NULL;

-- Comment
COMMENT ON COLUMN public.messages.media_type IS 'Type of media attachment: image, video, audio, or document';
COMMENT ON COLUMN public.messages.media_url IS 'Storage URL or WhatsApp media URL';
COMMENT ON COLUMN public.messages.media_filename IS 'Original filename of the media';
COMMENT ON COLUMN public.messages.media_mime_type IS 'MIME type of the media file';
COMMENT ON COLUMN public.messages.media_size_bytes IS 'Size of the media file in bytes';

