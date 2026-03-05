-- Atomic processing lock for message concatenation
-- This ensures that when multiple messages arrive quickly, they are properly buffered
-- and processed together, avoiding race conditions in multi-process environments.

-- Function to atomically try to acquire a processing lock
-- Returns: {acquired: boolean, buffered: boolean, existing_worker_id: string|null}
CREATE OR REPLACE FUNCTION try_acquire_processing_lock(
  p_wa_id TEXT,
  p_project_name TEXT,
  p_phone_number_id TEXT,
  p_worker_id TEXT,
  p_lock_duration_seconds INT DEFAULT 60,
  p_message_to_buffer JSONB DEFAULT NULL  -- {text, message_id, message_type, timestamp}
) RETURNS JSONB AS $$
DECLARE
  v_current_lock_until NUMERIC;
  v_current_worker_id TEXT;
  v_current_start_time NUMERIC;
  v_now NUMERIC := EXTRACT(EPOCH FROM NOW());
  v_lock_until NUMERIC;
  v_pending_messages JSONB;
  v_result JSONB;
BEGIN
  -- Try to get the current lock state with row-level lock
  SELECT 
    (context->>'processing_lock_until')::NUMERIC,
    context->>'processing_worker_id',
    (context->>'processing_start_time')::NUMERIC,
    COALESCE(context->'pending_messages', '[]'::JSONB)
  INTO v_current_lock_until, v_current_worker_id, v_current_start_time, v_pending_messages
  FROM enrollment_contexts
  WHERE wa_id = p_wa_id 
    AND (project_name = p_project_name OR (project_name IS NULL AND p_project_name IS NULL))
    AND (phone_number_id = p_phone_number_id OR (phone_number_id IS NULL AND p_phone_number_id IS NULL))
  FOR UPDATE;  -- Lock the row to prevent race conditions

  -- Check if we found the row
  IF NOT FOUND THEN
    -- Row doesn't exist, create it with the lock
    v_lock_until := v_now + p_lock_duration_seconds;
    INSERT INTO enrollment_contexts (wa_id, project_name, phone_number_id, context)
    VALUES (
      p_wa_id, 
      p_project_name, 
      p_phone_number_id,
      jsonb_build_object(
        'processing_lock_until', v_lock_until,
        'processing_worker_id', p_worker_id,
        'processing_start_time', v_now,
        'pending_messages', '[]'::JSONB
      )
    );
    RETURN jsonb_build_object('acquired', true, 'buffered', false, 'existing_worker_id', NULL);
  END IF;

  -- Check for stale locks (older than 30 seconds - same as lock duration for quick recovery)
  IF v_current_start_time IS NOT NULL AND (v_now - v_current_start_time) > 30 THEN
    -- Stale lock, clear it and acquire new one
    v_lock_until := v_now + p_lock_duration_seconds;
    UPDATE enrollment_contexts
    SET context = context || jsonb_build_object(
      'processing_lock_until', v_lock_until,
      'processing_worker_id', p_worker_id,
      'processing_start_time', v_now,
      'pending_messages', '[]'::JSONB  -- Clear stale pending messages too
    )
    WHERE wa_id = p_wa_id 
      AND (project_name = p_project_name OR (project_name IS NULL AND p_project_name IS NULL))
      AND (phone_number_id = p_phone_number_id OR (phone_number_id IS NULL AND p_phone_number_id IS NULL));
    
    RETURN jsonb_build_object('acquired', true, 'buffered', false, 'existing_worker_id', v_current_worker_id, 'stale_lock_cleared', true);
  END IF;

  -- Check if lock is active
  IF v_current_lock_until IS NOT NULL AND v_now < v_current_lock_until THEN
    -- Lock is active, buffer the message if provided
    IF p_message_to_buffer IS NOT NULL THEN
      -- Add message to pending_messages array
      v_pending_messages := v_pending_messages || p_message_to_buffer;
      
      UPDATE enrollment_contexts
      SET context = context || jsonb_build_object('pending_messages', v_pending_messages)
      WHERE wa_id = p_wa_id 
        AND (project_name = p_project_name OR (project_name IS NULL AND p_project_name IS NULL))
        AND (phone_number_id = p_phone_number_id OR (phone_number_id IS NULL AND p_phone_number_id IS NULL));
      
      RETURN jsonb_build_object('acquired', false, 'buffered', true, 'existing_worker_id', v_current_worker_id, 'buffer_size', jsonb_array_length(v_pending_messages));
    ELSE
      RETURN jsonb_build_object('acquired', false, 'buffered', false, 'existing_worker_id', v_current_worker_id);
    END IF;
  END IF;

  -- No active lock, acquire it
  v_lock_until := v_now + p_lock_duration_seconds;
  UPDATE enrollment_contexts
  SET context = context || jsonb_build_object(
    'processing_lock_until', v_lock_until,
    'processing_worker_id', p_worker_id,
    'processing_start_time', v_now
  )
  WHERE wa_id = p_wa_id 
    AND (project_name = p_project_name OR (project_name IS NULL AND p_project_name IS NULL))
    AND (phone_number_id = p_phone_number_id OR (phone_number_id IS NULL AND p_phone_number_id IS NULL));

  RETURN jsonb_build_object('acquired', true, 'buffered', false, 'existing_worker_id', NULL);
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;


-- Function to atomically get and clear pending messages
-- Returns the array of pending messages and clears it in one atomic operation
CREATE OR REPLACE FUNCTION get_and_clear_pending_messages(
  p_wa_id TEXT,
  p_project_name TEXT,
  p_phone_number_id TEXT
) RETURNS JSONB AS $$
DECLARE
  v_pending_messages JSONB;
BEGIN
  -- Get and clear in one atomic operation
  UPDATE enrollment_contexts
  SET context = context || jsonb_build_object('pending_messages', '[]'::JSONB)
  WHERE wa_id = p_wa_id 
    AND (project_name = p_project_name OR (project_name IS NULL AND p_project_name IS NULL))
    AND (phone_number_id = p_phone_number_id OR (phone_number_id IS NULL AND p_phone_number_id IS NULL))
  RETURNING COALESCE(context->'pending_messages', '[]'::JSONB) INTO v_pending_messages;

  -- The RETURNING gives us the OLD value before the update, but we want it
  -- Actually, we need to get the value BEFORE clearing it
  -- Let's fix this with a different approach
  RETURN v_pending_messages;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Actually, let's rewrite to get the value before clearing
DROP FUNCTION IF EXISTS get_and_clear_pending_messages(TEXT, TEXT, TEXT);
CREATE OR REPLACE FUNCTION get_and_clear_pending_messages(
  p_wa_id TEXT,
  p_project_name TEXT,
  p_phone_number_id TEXT
) RETURNS JSONB AS $$
DECLARE
  v_pending_messages JSONB;
BEGIN
  -- First get the current pending messages with row lock
  SELECT COALESCE(context->'pending_messages', '[]'::JSONB)
  INTO v_pending_messages
  FROM enrollment_contexts
  WHERE wa_id = p_wa_id 
    AND (project_name = p_project_name OR (project_name IS NULL AND p_project_name IS NULL))
    AND (phone_number_id = p_phone_number_id OR (phone_number_id IS NULL AND p_phone_number_id IS NULL))
  FOR UPDATE;

  -- If no row found, return empty array
  IF NOT FOUND THEN
    RETURN '[]'::JSONB;
  END IF;

  -- Clear the pending messages
  UPDATE enrollment_contexts
  SET context = context || jsonb_build_object('pending_messages', '[]'::JSONB)
  WHERE wa_id = p_wa_id 
    AND (project_name = p_project_name OR (project_name IS NULL AND p_project_name IS NULL))
    AND (phone_number_id = p_phone_number_id OR (phone_number_id IS NULL AND p_phone_number_id IS NULL));

  RETURN v_pending_messages;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;


-- Function to release the processing lock
CREATE OR REPLACE FUNCTION release_processing_lock(
  p_wa_id TEXT,
  p_project_name TEXT,
  p_phone_number_id TEXT,
  p_worker_id TEXT DEFAULT NULL  -- Optional: only release if this worker owns the lock
) RETURNS BOOLEAN AS $$
DECLARE
  v_current_worker_id TEXT;
BEGIN
  -- Get current worker with lock
  SELECT context->>'processing_worker_id'
  INTO v_current_worker_id
  FROM enrollment_contexts
  WHERE wa_id = p_wa_id 
    AND (project_name = p_project_name OR (project_name IS NULL AND p_project_name IS NULL))
    AND (phone_number_id = p_phone_number_id OR (phone_number_id IS NULL AND p_phone_number_id IS NULL))
  FOR UPDATE;

  IF NOT FOUND THEN
    RETURN FALSE;
  END IF;

  -- If worker_id specified, only release if it matches
  IF p_worker_id IS NOT NULL AND v_current_worker_id IS NOT NULL AND v_current_worker_id != p_worker_id THEN
    RETURN FALSE;  -- Not our lock
  END IF;

  -- Release the lock
  UPDATE enrollment_contexts
  SET context = context || jsonb_build_object(
    'processing_lock_until', NULL,
    'processing_worker_id', NULL,
    'processing_start_time', NULL
  )
  WHERE wa_id = p_wa_id 
    AND (project_name = p_project_name OR (project_name IS NULL AND p_project_name IS NULL))
    AND (phone_number_id = p_phone_number_id OR (phone_number_id IS NULL AND p_phone_number_id IS NULL));

  RETURN TRUE;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;


-- Grant execute permissions
GRANT EXECUTE ON FUNCTION try_acquire_processing_lock(TEXT, TEXT, TEXT, TEXT, INT, JSONB) TO authenticated;
GRANT EXECUTE ON FUNCTION try_acquire_processing_lock(TEXT, TEXT, TEXT, TEXT, INT, JSONB) TO anon;
GRANT EXECUTE ON FUNCTION try_acquire_processing_lock(TEXT, TEXT, TEXT, TEXT, INT, JSONB) TO service_role;

GRANT EXECUTE ON FUNCTION get_and_clear_pending_messages(TEXT, TEXT, TEXT) TO authenticated;
GRANT EXECUTE ON FUNCTION get_and_clear_pending_messages(TEXT, TEXT, TEXT) TO anon;
GRANT EXECUTE ON FUNCTION get_and_clear_pending_messages(TEXT, TEXT, TEXT) TO service_role;

GRANT EXECUTE ON FUNCTION release_processing_lock(TEXT, TEXT, TEXT, TEXT) TO authenticated;
GRANT EXECUTE ON FUNCTION release_processing_lock(TEXT, TEXT, TEXT, TEXT) TO anon;
GRANT EXECUTE ON FUNCTION release_processing_lock(TEXT, TEXT, TEXT, TEXT) TO service_role;
