CREATE OR REPLACE FUNCTION public.list_all_tables()
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
  result jsonb;
BEGIN
  SELECT jsonb_agg(jsonb_build_object('schema', table_schema, 'table', table_name))
  INTO result
  FROM information_schema.tables
  WHERE table_schema NOT IN ('information_schema', 'pg_catalog');
  
  RETURN COALESCE(result, '[]'::jsonb);
END;
$$;
