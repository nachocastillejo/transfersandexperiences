-- Functions to get distribution metrics for dashboard (queues, statuses, attention)

-- 1. Get queue distribution (conversations per queue)
-- Returns queue_id, queue_name, and count of conversations
CREATE OR REPLACE FUNCTION get_queue_distribution(p_phone_number_id text)
RETURNS TABLE (queue_id uuid, queue_name text, count bigint)
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
  RETURN QUERY
  WITH queue_counts AS (
    SELECT 
      unnest(COALESCE(c.assigned_queue_ids, ARRAY[]::uuid[])) as qid
    FROM conversations c
    WHERE p_phone_number_id IS NULL 
       OR c.phone_number_id = p_phone_number_id
  ),
  unassigned AS (
    SELECT 
      NULL::uuid as qid,
      COUNT(*) as cnt
    FROM conversations c
    WHERE (p_phone_number_id IS NULL OR c.phone_number_id = p_phone_number_id)
      AND (c.assigned_queue_ids IS NULL OR c.assigned_queue_ids = ARRAY[]::uuid[])
  ),
  assigned AS (
    SELECT 
      qc.qid,
      COUNT(*) as cnt
    FROM queue_counts qc
    WHERE qc.qid IS NOT NULL
    GROUP BY qc.qid
  ),
  raw_results AS (
    SELECT 
      a.qid as queue_id,
      COALESCE(q.name, 'Sin cola') as queue_name,
      a.cnt as count
    FROM assigned a
    LEFT JOIN queues q ON q.id = a.qid
    UNION ALL
    SELECT 
      NULL::uuid as queue_id,
      'Sin cola'::text as queue_name,
      u.cnt as count
    FROM unassigned u
    WHERE u.cnt > 0
  )
  -- Group by queue_name to merge duplicate "Sin cola" entries
  -- (from orphaned queue_ids + genuinely unassigned conversations)
  SELECT 
    MIN(r.queue_id) as queue_id,
    r.queue_name,
    SUM(r.count) as count
  FROM raw_results r
  GROUP BY r.queue_name
  ORDER BY count DESC;
END;
$$;

-- 2. Get status distribution (conversations per estado_conversacion)
CREATE OR REPLACE FUNCTION get_status_distribution(p_phone_number_id text)
RETURNS TABLE (status text, count bigint)
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
  RETURN QUERY
  SELECT 
    COALESCE(c.estado_conversacion, 'Sin estado') as status,
    COUNT(*) as count
  FROM conversations c
  WHERE p_phone_number_id IS NULL 
     OR c.phone_number_id = p_phone_number_id
  GROUP BY c.estado_conversacion
  ORDER BY count DESC;
END;
$$;

-- 3. Get attention distribution (needs_attention flag)
CREATE OR REPLACE FUNCTION get_attention_distribution(p_phone_number_id text)
RETURNS TABLE (needs_attention boolean, count bigint)
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
  RETURN QUERY
  SELECT 
    COALESCE(c.needs_attention, false) as needs_attention,
    COUNT(*) as count
  FROM conversations c
  WHERE p_phone_number_id IS NULL 
     OR c.phone_number_id = p_phone_number_id
  GROUP BY COALESCE(c.needs_attention, false)
  ORDER BY needs_attention DESC;
END;
$$;

