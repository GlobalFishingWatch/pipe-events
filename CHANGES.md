Changes
=======

0.3.0 - 2018-09-07
------------------

* [#17](https://github.com/GlobalFishingWatch/pipe-events/pull/17)
  * Implements postgres event publication. Requires new airflow variables: `pipe_events.postgres_instance`, `pipe_events.postgres_connection_string` and one `postgres_table` variable for each event type subpipeline. See the README for more information.
* [#7](https://github.com/GlobalFishingWatch/encounters_pipeline/pull/7)
  * Adds the publication of the rest of the event types into this pipeline.
  * Bump version of pipe-tools to 0.2.0
  * Requires a new global airflow variable, `INFERENCE_BUFFER_DAYS`, which contains the number of days the fishing events might have to be updated in the past due to inference buffering.
  * Changes the schema of the events table, removing some field duplication (both `timestamp` and `event_start` meant the same) and adding a new `event_geography` column, which is a GIS column allowing geospatial queries.

v0.2.0 - 2018-07-23
-------------------
* [#3](https://github.com/GlobalFishingWatch/pipe-events/pull/3)
 Bugfix for airflow monthy gap events
* [#5](https://github.com/GlobalFishingWatch/pipe-events/pull/3) 
 update pipe-tools to v0.1.6

v0.1.0 - 2018-05-11
-------------------

* Initial release.  
* First pass implementation of AIS on/off events
