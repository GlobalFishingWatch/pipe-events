-- Create base events table
CREATE TABLE IF NOT EXISTS public.{{ table_name }} (
  event_id character varying PRIMARY KEY NOT NULL,
  event_type character varying NOT NULL,
  vessel_id character varying NOT NULL,
  event_start timestamp without time zone NOT NULL,
  event_end timestamp without time zone,
  event_info jsonb NOT NULL,
  event_vessels jsonb NOT NULL,
  event_mean_position public.geography(Point, 4326) NOT NULL
);

-- Ensure the table is empty
DELETE FROM public.{{ table_name }}
WHERE event_type = '{{ event_type }}'
    AND event_start >= '{{ start_date }}' AND event_start <= '{{ end_date }}';


-- Drop all constraints and indices if they exist
DROP INDEX IF EXISTS {{ table_name }}_event_id;
DROP INDEX IF EXISTS {{ table_name }}_event_type;
DROP INDEX IF EXISTS {{ table_name }}_event_start;
DROP INDEX IF EXISTS {{ table_name }}_vessel_id;
DROP INDEX IF EXISTS {{ table_name }}_event_mean_position_gis;
