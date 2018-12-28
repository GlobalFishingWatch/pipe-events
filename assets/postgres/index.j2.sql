-- Setup constraints and indices
CREATE INDEX {{ table_name }}_event_id ON public.{{ table_name }} (event_id);
CREATE INDEX {{ table_name }}_event_type ON public.{{ table_name }} (event_type);
CREATE INDEX {{ table_name }}_event_start ON public.{{ table_name }} (event_start);
CREATE INDEX {{ table_name }}_vessel_id ON public.{{ table_name }} (vessel_id);
CREATE INDEX {{ table_name }}_event_geography_gis ON public.{{ table_name }} USING gist (event_geography);
CREATE INDEX {{ table_name }}_event_mean_position_gis ON public.{{ table_name }} USING gist (event_mean_position);
