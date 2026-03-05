CREATE TABLE IF NOT EXISTS public.template_submissions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    phone_number_id TEXT,
    template_name TEXT NOT NULL,
    language TEXT NOT NULL,
    total_recipients INT NOT NULL DEFAULT 0,
    successful_count INT NOT NULL DEFAULT 0,
    failed_count INT NOT NULL DEFAULT 0,
    raw_data JSONB DEFAULT '{}'::jsonb,
    sent_by TEXT
);

-- Index for filtering by phone_number_id
CREATE INDEX IF NOT EXISTS idx_template_submissions_phone_number_id 
    ON public.template_submissions(phone_number_id);

-- RLS policies
ALTER TABLE public.template_submissions ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Enable read access for authenticated users" ON public.template_submissions
    FOR SELECT
    TO authenticated
    USING (true);

CREATE POLICY "Enable insert for authenticated users" ON public.template_submissions
    FOR INSERT
    TO authenticated
    WITH CHECK (true);

CREATE POLICY "Enable update for authenticated users" ON public.template_submissions
    FOR UPDATE
    TO authenticated
    USING (true);
