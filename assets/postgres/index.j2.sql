-- Setup constraints and indices
ALTER TABLE ONLY public.{{ table_name }}
    ADD CONSTRAINT {{ table_name }}_pkey PRIMARY KEY (event_id);

CREATE INDEX {{ table_name }}_gis ON public.{{ table_name }} USING gist (event_geography);
