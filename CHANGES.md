# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a
Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

* Added the `SAFE.` in the BQ fishing event scripts since with some coordinates
  for VMS it was throwing the `results in precision loss of more than 1mm`
  error. 
* [#1163](https://github.com/GlobalFishingWatch/GFW-Tasks/issues/1163):
  Consolidates the functionality from the carrier portal queries into the
  regular events. Adds a new event dag which contains the carrier-spefic
  encounters. Adds new functionality to enable or disable specific event types
  from an installation. Adds new functionality which allows an installation to
  specify source filters for a given event type.

## v1.4.1 - 2019-09-10

### Added

* [1125](https://github.com/GlobalFishingWatch/pipe-events/pull/1125): Adds
  a flag to indicate if we want to publish the results on postgres.

## v1.4.0 - 2019-08-29

### Changed

* [33](https://github.com/GlobalFishingWatch/pipe-events/pull/33): Improved
  the id generation hash to be unique by including both of the vessel ids and 
  the start and end times. Previous version used only one vessel id and start time.


## v1.3.0 - 2019-08-05

### Added

* [1100](https://github.com/GlobalFishingWatch/GFW-Tasks/issues/1100): Adds
  Supports the handler of FlexibleOperator. Also moves the pipe-events subdags to
  independent dags. and converts each operator to a KubernetesPodOperator.


## v1.2.1 - 2019-07-05

### Added

* [1070](https://github.com/GlobalFishingWatch/GFW-Tasks/issues/1070): Bugfixes
  ship names encoded to unicode.

## v1.2.0 - 2019-05-17

### Added

* [1007](https://github.com/GlobalFishingWatch/GFW-Tasks/issues/1007): Restores
  the cloudsql configuration options and code since google just released a new
  version of their postgres offering which includes cloudsql. This requires new
  configuration options, check the [README.md](README.md) for more information
* [1005](https://github.com/GlobalFishingWatch/GFW-Tasks/issues/1005): Adds
  more vessel information embedded in the event. It now includes ssvid in
  addition to name and vessel id.

## v0.4.1 (LEGACY SUPPORT) and v1.1.0 2019-04-06

### Added
* [926](https://github.com/GlobalFishingWatch/GFW-Tasks/issues/926): Adds basic
  information about related entities on events. For example, fishing events now
  include vessel name, ports contain the port label, and so on. Removes
  configuration options for cloudsql, since we are now using a custom postgres
  database. This requires new configuration options, see the
  [README.md](README.md) for more information.
* [926](https://github.com/GlobalFishingWatch/GFW-Tasks/issues/926): Introduces
  yearly mode to the events pipe.

## v1.0.0 - 2019-03-27

### Added

* [GlobalFishingWatch/GFW-Tasks#991](https://github.com/GlobalFishingWatch/GFW-Tasks/issues/991)
  * Migrates to use the new [airflow-gfw](https://github.com/GlobalFishingWatch/airflow-gfw) library and use the pipe-tools [v2.0.0](https://github.com/GlobalFishingWatch/pipe-tools/releases/tag/v2.0.0)

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

