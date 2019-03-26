# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a
Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
* [926](https://github.com/GlobalFishingWatch/GFW-Tasks/issues/926): Adds basic
  information about related entities on events. For example, fishing events now
  include vessel name, ports contain the port label, and so on. Removes
  configuration options for cloudsql, since we are now using a custom postgres
  database.

## v0.4.0 2019-03-14

### Added
* [#944](https://github.com/GlobalFishingWatch/GFW-Tasks/issues/944): Adds
  various indices to the postgis tables.

* [#895](https://github.com/GlobalFishingWatch/GFW-Tasks/issues/895): Gaps and
  port events now come as a single event for each on-off / in-out raw
  combination.

* [#921](https://github.com/GlobalFishingWatch/GFW-Tasks/issues/921): All event
  types are now imported to postgis.

* [#862](https://github.com/GlobalFishingWatch/GFW-Tasks/issues/862):
  Implements postgres event publication. Requires new airflow variables:
  `pipe_events.postgres_instance`, `pipe_events.postgres_connection_string` and
  one `postgres_table` variable for each event type subpipeline. See the
  [README](README.md#Configuration) for more information.

* [#18](https://github.com/GlobalFishingWatch/pipe-events/issues/18): Include
  the `encountered_vessel_id` in the `event_info` for encounters. 

* [#22](https://github.com/GlobalFishingWatch/pipe-events/issues/22): Fix a bug in 
  fishing events where events crossing the dateline had the wrong min/max/avg 
  for longitude
  
## v0.3.1 2018-11-29

### Changed
* Hotfixes fishing events that had a null `event_end` field when the event
  finished at the end of a given day.

## v0.3.0 - 2018-09-07

### Added
* [#828](https://github.com/GlobalFishingWatch/GFW-Tasks/issues/828): Adds the
  publication of the rest of the event types into this pipeline.  Bump version
  of pipe-tools to 0.2.0 Requires a new global airflow variable,
  `INFERENCE_BUFFER_DAYS`, which contains the number of days the fishing events
  might have to be updated in the past due to inference buffering.  Changes the
  schema of the events table, removing some field duplication (both `timestamp`
  and `event_start` meant the same) and adding a new `event_geography` column,
  which is a GIS column allowing geospatial queries.

## v0.2.0 - 2018-07-23
### Changed
* [#5](https://github.com/GlobalFishingWatch/pipe-events/issues/5): Updates
  pipe-tools to v0.1.6. Fixes gap events when running monthly mode.

# v0.1.0 - 2018-05-11

* Initial release

