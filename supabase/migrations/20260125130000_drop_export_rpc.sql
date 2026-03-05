-- Remove the export RPC function (has 1000 row limit, not useful)
DROP FUNCTION IF EXISTS get_messages_for_export(TEXT, TEXT[]);
