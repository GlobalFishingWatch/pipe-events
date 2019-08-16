-- Ensure we run vacuumming after deleting most of the rows in the table
VACUUM ANALYZE public.{{ table_name }};
