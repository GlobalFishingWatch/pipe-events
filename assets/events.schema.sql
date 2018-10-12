-- Events database schema

-- Enable POSTGIS extensions
CREATE EXTENSION postgis;
CREATE EXTENSION postgis_topology;
CREATE EXTENSION postgis_tiger_geocoder CASCADE;

-- Events table schemas
CREATE TABLE public.fishing_events (
    event_id character varying NOT NULL,
    event_type character varying NOT NULL,
    vessel_id character varying NOT NULL,
    event_start timestamp without time zone NOT NULL,
    event_end timestamp without time zone NOT NULL,
    event_info jsonb NOT NULL,
    event_geography public.geography(MultiPoint,4326) NOT NULL
);

ALTER TABLE ONLY public.fishing_events
    ADD CONSTRAINT fishing_events_pkey PRIMARY KEY (event_id);

CREATE INDEX fishing_events_gis ON public.fishing_events USING gist (event_geography);
