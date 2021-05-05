SELECT ordinal_position,
       column_name,
       data_type,
       character_maximum_length,
       numeric_precision,
       numeric_scale
FROM information_schema.columns
WHERE table_schema = %(schema_name)s
  AND table_name = %(table_name)s
ORDER BY ordinal_position;