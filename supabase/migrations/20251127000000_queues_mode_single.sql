-- Change queues.modes (array) to queues.mode (single text value)
-- Each queue can only have one mode: 'bot' or 'agent'

-- Add the new column
ALTER TABLE public.queues
ADD COLUMN IF NOT EXISTS mode text CHECK (mode IN ('bot', 'agent'));

-- Migrate existing data: take first element from modes array if exists, default to 'bot'
UPDATE public.queues
SET mode = COALESCE(modes[1], 'bot')
WHERE mode IS NULL;

-- Special case: 'Documentación' queue should always be 'agent' mode
UPDATE public.queues
SET mode = 'agent'
WHERE name = 'Documentación';

-- Set default to 'bot' for new queues
ALTER TABLE public.queues
ALTER COLUMN mode SET DEFAULT 'bot';

-- Drop the old modes array column
ALTER TABLE public.queues
DROP COLUMN IF EXISTS modes;

-- Add comment for documentation
COMMENT ON COLUMN public.queues.mode IS 'Queue mode: bot or agent. When a conversation is assigned to this queue, it automatically switches to this mode.';

