-- Migration: Add RPC functions for enrollment metrics
-- This adds functions to get enrollment counts for dashboard metrics

-- Function to get total COMPLETED enrollments count (filtered by phone_number_id)
-- Counts the total number of elements in the 'inscripciones' JSON array within each context
CREATE OR REPLACE FUNCTION public.get_total_enrollments_count(p_phone_number_id TEXT DEFAULT NULL)
RETURNS BIGINT
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
    IF p_phone_number_id IS NULL THEN
        RETURN (
            SELECT COALESCE(SUM(jsonb_array_length(context->'inscripciones')), 0)
            FROM public.enrollment_contexts
            WHERE context->'inscripciones' IS NOT NULL 
              AND jsonb_typeof(context->'inscripciones') = 'array'
        );
    ELSE
        RETURN (
            SELECT COALESCE(SUM(jsonb_array_length(context->'inscripciones')), 0)
            FROM public.enrollment_contexts
            WHERE phone_number_id = p_phone_number_id
              AND context->'inscripciones' IS NOT NULL 
              AND jsonb_typeof(context->'inscripciones') = 'array'
        );
    END IF;
END;
$$;

-- Function to get daily COMPLETED enrollment counts within a date range
-- Extracts each enrollment from the 'inscripciones' JSON array and groups by the enrollment date
-- Returns rows with 'day' (date) and 'count' (bigint)
CREATE OR REPLACE FUNCTION public.get_daily_enrollment_counts(
    p_phone_number_id TEXT DEFAULT NULL,
    p_start_date TIMESTAMPTZ DEFAULT NULL,
    p_end_date TIMESTAMPTZ DEFAULT NULL
)
RETURNS TABLE(day DATE, count BIGINT)
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
    RETURN QUERY
    SELECT 
        DATE((enrollment->>'fecha')::TIMESTAMPTZ) AS day,
        COUNT(*)::BIGINT AS count
    FROM public.enrollment_contexts ec,
         jsonb_array_elements(ec.context->'inscripciones') AS enrollment
    WHERE 
        (p_phone_number_id IS NULL OR ec.phone_number_id = p_phone_number_id)
        AND ec.context->'inscripciones' IS NOT NULL 
        AND jsonb_typeof(ec.context->'inscripciones') = 'array'
        AND enrollment->>'fecha' IS NOT NULL
        AND (p_start_date IS NULL OR (enrollment->>'fecha')::TIMESTAMPTZ >= p_start_date)
        AND (p_end_date IS NULL OR (enrollment->>'fecha')::TIMESTAMPTZ <= p_end_date)
    GROUP BY DATE((enrollment->>'fecha')::TIMESTAMPTZ)
    ORDER BY DATE((enrollment->>'fecha')::TIMESTAMPTZ);
END;
$$;

-- Function to get daily CONVERSATION counts (unique wa_ids per day)
-- Returns rows with 'day' (date) and 'count' (bigint)
CREATE OR REPLACE FUNCTION public.get_daily_conversation_counts(
    p_phone_number_id TEXT DEFAULT NULL,
    p_start_date TIMESTAMPTZ DEFAULT NULL,
    p_end_date TIMESTAMPTZ DEFAULT NULL
)
RETURNS TABLE(day DATE, count BIGINT)
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
    RETURN QUERY
    SELECT 
        (m.created_at AT TIME ZONE 'UTC')::DATE AS day,
        COUNT(DISTINCT m.wa_id)::BIGINT AS count
    FROM public.messages m
    WHERE 
        m.direction != 'outbound_system'
        AND (p_phone_number_id IS NULL OR m.phone_number_id = p_phone_number_id)
        AND (p_start_date IS NULL OR m.created_at >= p_start_date)
        AND (p_end_date IS NULL OR m.created_at <= p_end_date)
    GROUP BY (m.created_at AT TIME ZONE 'UTC')::DATE
    ORDER BY (m.created_at AT TIME ZONE 'UTC')::DATE;
END;
$$;

-- Grant execute permissions to authenticated users
GRANT EXECUTE ON FUNCTION public.get_total_enrollments_count(TEXT) TO authenticated;
GRANT EXECUTE ON FUNCTION public.get_daily_enrollment_counts(TEXT, TIMESTAMPTZ, TIMESTAMPTZ) TO authenticated;
GRANT EXECUTE ON FUNCTION public.get_daily_conversation_counts(TEXT, TIMESTAMPTZ, TIMESTAMPTZ) TO authenticated;

-- Also grant to anon for dashboard access (if needed)
GRANT EXECUTE ON FUNCTION public.get_total_enrollments_count(TEXT) TO anon;
GRANT EXECUTE ON FUNCTION public.get_daily_enrollment_counts(TEXT, TIMESTAMPTZ, TIMESTAMPTZ) TO anon;
GRANT EXECUTE ON FUNCTION public.get_daily_conversation_counts(TEXT, TIMESTAMPTZ, TIMESTAMPTZ) TO anon;

