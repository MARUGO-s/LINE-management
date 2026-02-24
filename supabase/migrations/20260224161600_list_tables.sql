CREATE OR REPLACE FUNCTION public.list_public_tables()
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
  result jsonb;
BEGIN
  SELECT jsonb_agg(table_name)
  INTO result
  FROM information_schema.tables
  WHERE table_schema = 'public';
  
  RETURN COALESCE(result, '[]'::jsonb);
END;
$$;
