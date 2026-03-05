-- Add target_queue_id column to track which queue was selected for template submissions
ALTER TABLE public.template_submissions
ADD COLUMN IF NOT EXISTS target_queue_id UUID;

-- Index for filtering by target_queue_id
CREATE INDEX IF NOT EXISTS idx_template_submissions_target_queue_id 
    ON public.template_submissions(target_queue_id);

-- Add foreign key reference to queues table (optional, depends on your constraint requirements)
-- ALTER TABLE public.template_submissions
-- ADD CONSTRAINT fk_target_queue
-- FOREIGN KEY (target_queue_id) REFERENCES public.queues(id) ON DELETE SET NULL;


