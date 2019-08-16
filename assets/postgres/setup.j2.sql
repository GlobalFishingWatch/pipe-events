-- Create base events table
CREATE TABLE IF NOT EXISTS public.{{ table_name }} (
  id bigserial PRIMARY KEY NOT NULL,
  event_id character varying NOT NULL,
  event_type character varying NOT NULL,
  vessel_id character varying NOT NULL,
  event_start timestamp without time zone NOT NULL,
  event_end timestamp without time zone,
  event_info jsonb NOT NULL,
  event_vessels jsonb NOT NULL,
  event_mean_position public.geography(Point, 4326) NOT NULL
);

-- Setup constraints and indices
-- Hash indices for quick equality lookups when looking for a certain event, all the events for a given vessel or all the events of a given type
CREATE INDEX IF NOT EXISTS {{ table_name }}_event_id ON public.{{ table_name }} USING hash (event_id);
CREATE INDEX IF NOT EXISTS {{ table_name }}_vessel_id ON public.{{ table_name }} USING hash (vessel_id);
CREATE INDEX IF NOT EXISTS {{ table_name }}_event_type ON public.{{ table_name }} USING hash (event_type);
-- Datetime range indices together with event type for date between queries
CREATE INDEX IF NOT EXISTS {{ table_name }}_event_start ON public.{{ table_name }} (event_type, event_start);
-- GIS indices for geographic filtering
CREATE INDEX IF NOT EXISTS {{ table_name }}_event_mean_position_gis ON public.{{ table_name }} USING gist (event_mean_position);

-- Ensure the table is empty
DELETE FROM public.{{ table_name }} WHERE event_type = '{{ event_type }}';
