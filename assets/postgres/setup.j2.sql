-- Create base events table
CREATE TABLE IF NOT EXISTS public.{{ table_name }} (
  event_id character varying PRIMARY KEY NOT NULL,
  event_type character varying NOT NULL,
  vessel_id character varying NOT NULL,
  seg_id character varying NOT NULL,
  event_start timestamp without time zone NOT NULL,
  event_end timestamp without time zone,
  lat_mean double precision NOT NULL,
  lot_mean double precision NOT NULL,
  lat_min double precision NOT NULL,
  lon_min double precision NOT NULL,
  lat_max double precision NOT NULL,
  lon_max double precision NOT NULL,
  regions_mean_position jsonb NOT NULL,
  start_distance_from_shore_km double precision,
  end_distance_from_shore_km double precision,
  start_distance_from_port_km double precision,
  end_distance_from_port_km double precision,
  event_info jsonb NOT NULL,
  event_vessels jsonb NOT NULL
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
