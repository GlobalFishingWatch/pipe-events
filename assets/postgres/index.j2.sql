-- Setup constraints and indices
CREATE INDEX {{ table_name }}_event_id ON public.{{ table_name }} (event_id);
CREATE INDEX {{ table_name }}_gis ON public.{{ table_name }} USING gist (event_geography);
