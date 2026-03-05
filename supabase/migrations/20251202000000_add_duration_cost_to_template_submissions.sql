-- Add duration, cost and category fields to template_submissions table
ALTER TABLE public.template_submissions 
ADD COLUMN IF NOT EXISTS duration_seconds NUMERIC(10,2) DEFAULT NULL,
ADD COLUMN IF NOT EXISTS estimated_cost NUMERIC(10,4) DEFAULT NULL,
ADD COLUMN IF NOT EXISTS category TEXT DEFAULT NULL;

-- Comment explaining the fields
COMMENT ON COLUMN public.template_submissions.duration_seconds IS 'Time in seconds to complete the batch send';
COMMENT ON COLUMN public.template_submissions.estimated_cost IS 'Estimated cost in EUR based on successful messages and template category';
COMMENT ON COLUMN public.template_submissions.category IS 'Template category: MARKETING, UTILITY, or AUTHENTICATION';

