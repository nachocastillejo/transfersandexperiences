-- Functions to optimize dashboard metrics by running aggregations on the database side

-- 1. Get unique conversations count
CREATE OR REPLACE FUNCTION get_unique_conversations_count(p_phone_number_id text)
RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
  RETURN (
    SELECT COUNT(DISTINCT wa_id)
    FROM messages
    WHERE direction != 'outbound_system'
      AND (p_phone_number_id IS NULL OR phone_number_id = p_phone_number_id)
  );
END;
$$;

-- 2. Get average bot response time
CREATE OR REPLACE FUNCTION get_average_bot_response_time(p_phone_number_id text)
RETURNS numeric
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
  RETURN (
    SELECT COALESCE(AVG(response_time_seconds), 0)
    FROM messages
    WHERE direction = 'outbound_bot'
      AND response_time_seconds > 0
      AND (p_phone_number_id IS NULL OR phone_number_id = p_phone_number_id)
  );
END;
$$;

-- 3. Get daily message counts (for timeline)
-- Returns a list of objects with day and count
CREATE OR REPLACE FUNCTION get_daily_message_counts(
    p_phone_number_id text,
    p_start_date timestamptz,
    p_end_date timestamptz
)
RETURNS TABLE (day date, count bigint)
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
  RETURN QUERY
  SELECT
    (created_at AT TIME ZONE 'UTC')::date as day,
    COUNT(*) as count
  FROM messages
  WHERE direction != 'outbound_system'
    AND (p_phone_number_id IS NULL OR phone_number_id = p_phone_number_id)
    AND (p_start_date IS NULL OR created_at >= p_start_date)
    AND (p_end_date IS NULL OR created_at <= p_end_date)
  GROUP BY 1
  ORDER BY 1;
END;
$$;

-- 4. Get hourly inbound distribution
CREATE OR REPLACE FUNCTION get_hourly_inbound_distribution(p_phone_number_id text)
RETURNS TABLE (hour int, count bigint)
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
  RETURN QUERY
  SELECT
    EXTRACT(HOUR FROM (created_at AT TIME ZONE 'UTC'))::int as hour,
    COUNT(*) as count
  FROM messages
  WHERE direction = 'inbound'
    AND (p_phone_number_id IS NULL OR phone_number_id = p_phone_number_id)
  GROUP BY 1
  ORDER BY 1;
END;
$$;

