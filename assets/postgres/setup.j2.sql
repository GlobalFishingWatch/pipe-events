-- Ensure POSTGIS extensions are enabled in this db
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_topology;
CREATE EXTENSION IF NOT EXISTS postgis_tiger_geocoder CASCADE;

-- Create base events table
CREATE TABLE IF NOT EXISTS public.{{ table_name }} (
    id bigserial NOT NULL,
    event_id character varying NOT NULL,
    event_type character varying NOT NULL,
    vessel_id character varying NOT NULL,
    event_start timestamp without time zone NOT NULL,
    event_end timestamp without time zone NOT NULL,
    event_info jsonb NOT NULL,
    event_geography public.geography(MultiPoint,4326) NOT NULL
);

-- Ensure the table is empty
TRUNCATE TABLE public.{{ table_name }};

-- Drop all constraints and indices if they exist
DROP INDEX IF EXISTS {{ table_name }}_event_id;
DROP INDEX IF EXISTS {{ table_name }}_gis;


