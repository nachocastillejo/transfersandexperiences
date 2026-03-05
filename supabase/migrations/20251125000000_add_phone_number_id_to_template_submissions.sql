-- Add phone_number_id column to template_submissions table
ALTER TABLE public.template_submissions 
ADD COLUMN IF NOT EXISTS phone_number_id TEXT;

-- Create index for filtering by phone_number_id
CREATE INDEX IF NOT EXISTS idx_template_submissions_phone_number_id 
    ON public.template_submissions(phone_number_id);










