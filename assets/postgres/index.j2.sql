-- Setup constraints and indices
CREATE INDEX {{ table_name }}_event_id ON public.{{ table_name }} (event_id);
CREATE INDEX {{ table_name }}_event_type ON public.{{ table_name }} (event_type);
CREATE INDEX {{ table_name }}_event_start ON public.{{ table_name }} (event_type, event_start);
CREATE INDEX {{ table_name }}_vessel_id ON public.{{ table_name }} (event_type, vessel_id);

-- Ensure we run vacuumming after deleting most of the rows in the table
VACUUM ANALYZE public.{{ table_name }};
