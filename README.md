# Events pipeline

This repository contains the events pipeline, a simple pipeline which extracts summarized events from various datasets we produce.

# Running

## Dependencies

You just need [docker](https://www.docker.com/) and [docker-compose](https://docs.docker.com/compose/) in your machine to run the pipeline. No other dependency is required.

## Setup

First of all build the image.

```
$ docker compose build
```

The pipeline reads it's input from BigQuery, so you need to first authenticate with your google cloud account inside the docker images. To do that, you need to run this command and follow the instructions:

```
$ docker compose run gcloud auth login
```

## Configuration

### The fishing events incremental load

Incremental fishing events consists of 4 steps. Only the first step is actually incremental and the rest are just a series of transformations to get the final events. The steps are:
1. **Incremental merge**: This step loads messages and calculates fishing events. It runs twice, once for `nnet_score` and once for `night_loitering` because at this point we don't know yet what shiptype a vessel is. It is incremental because it only loads the messages that have been added since the last time it was run. Given an input date range of start_date and end_date, it loads the day before start_date as padding in order to perform the merge step, as well as data from start_date until end_date (exclusive). The incremental query is run as a temporary table which is merged into the `incremental_fishing/night_loitering_events_merged` table.
2. **Filter**: This step applies noise filters on segments, filters for potential fishing vessels, and for fishing events that actually fit the criteria of fishing events (e.g. duration > 20 minutes). This step also runs twice, once for `nnet_score` and once for `night_loitering`.
3. **Authorization**: This step adds the authorization information to the events. It also combines `nnet_score` and `night_loitering` events into a single table.
4. **Restrictive**: This step applies the more restrictive `prod_shiptype='fishing'` filter which is required in our API.

#### CLI

The way the fishing events was calculated took unexpected time and resources, to improve the calculation the incremental load was developed. See more in `./assets/bigquery/README.md`.

There is a specific python client to make the calls under `./pipe_events/cli.py`.
It has 4 subcommands:
* `incremental_events` : Opens a BQ session and calculates fishing event by segment. Merges messges by seg_id and timestamp versus a historical fishing events table and updates event_end/event_start stitching the history with what has produced the latest day. If the events in the history overlaps with the events in the daily they are updated, if not the events are addded. The output for this process is the table with the `_merged` suffix.
    It can be invoked (using the default values of the paramters that points to a scratch):
    ```bash
    # run daily incremental fishing events, outputs: scratch_matias_ttl_7_days.incremental_fishing_events_merged
    $ docker compose run --entrypoint pipe pipeline -v --project world-fishing-827 incremental_events
    # runs daily night loitering. outputs: scratch_matias_ttl_7_days.incremental_night_loitering_events_merged
    $ docker compose run --entrypoint pipe pipeline -v --project world-fishing-827 incremental_events -sfield night_loitering -dest_tbl_prefix incremental_night_loitering_events
    ```
    NOTE: the default dataset source is "pipe_ais_test_202408290000" and the dataset out is "scratch_matias_ttl_7_days".
    Show all options:
    ```bash
    $ docker compose run --entrypoint pipe pipeline incremental_events -h
    ```

* `incremental_filter_events`: Tooks a fishing events history table (`_merged`) and applies filters, add vessel_id, identities fields and remove overlapping_and_short segments. It outputs the `_filtered` table.
It can be invoked (using the default values of the paramters that points to a scratch):
    ```bash
    # run filter for the fishing events. outputs: scratch_matias_ttl_7_days.incremental_fishing_events_filtered
    $ docker compose run --entrypoint pipe pipeline -v --project world-fishing-827 incremental_filter_events
    # run filter for the night loitering. outputs: scratch_matias_ttl_7_days.incremental_night_loitering_events_filtered
    $ docker compose run --entrypoint pipe pipeline -v --project world-fishing-827 incremental_filter_events sfield night_loitering --merged_table world-fishing-827.scratch_matias_ttl_7_days.incremental_night_loitering_events_merged -incremental_night_loitering_events_filtered
    ```
    NOTE: the default dataset source is "pipe_ais_test_202408290000" and the dataset out is "scratch_matias_ttl_7_days".
    Show all options:
    ```bash
    $ docker compose run --entrypoint pipe pipeline incremental_filter_events -h
    ```
* `auth_and_regions_fishing_events`: Adds authorization and regions position.
    It can be invoked:
    ```bash
    # run daily incremental fishing events. outputs: scratch_matias_ttl_7_days.fishing_events_v20200102 and scratch_matias_ttl_7_days.fishing_events
    $ docker compose run --entrypoint pipe pipeline -v --project world-fishing-827 auth_and_regions_fishing_events
    ```
    NOTE: the default dataset source is "pipe_ais_test_202408290000" and the dataset out is "scratch_matias_ttl_7_days".
    Show all options:
    ```bash
    $ docker compose run --entrypoint pipe pipeline auth_and_regions_fishing_events -h
    ```
* `fishing_restrictive`: restrict the events using a specific list.
    It can be invoked:
    ```bash
    # run daily incremental fishing events. outputs: scratch_matias_ttl_7_days.fishing_events_restrictive_v20200102 and scratch_matias_ttl_7_days.fishing_events_restrictive
    $ docker compose run --entrypoint pipe pipeline -v --project world-fishing-827 fishing_restrictive
    ```
    NOTE: the default dataset source is "pipe_ais_test_202408290000" and the dataset out is "scratch_matias_ttl_7_days".
    Show all options:
    ```bash
    $ docker compose run --entrypoint pipe pipeline fishing_restrictive -h
    ```


#### Docker compose

A pipeline that runs all incremental fishing events steps subsequently is available in [scripts/generate_incremental_fishing_events.sh](scripts/generate_incremental_fishing_events.sh). It uses docker compose to run the pipeline in a containerized environment.

To run a full backfill on the staging pipeline you can use the following command:

```
docker compose build
cd scripts
./generate_incremental_fishing_events.sh --pipeline_prefix PIPELINE12345_staging_test --start_d 2020-01-01 --end_d 2020-12-31
```

### The former standard way

The pipeline exposes the following standard settings:

* `pipe_events.docker_run`: Command to run docker inside the airflow server.
* `pipe_events.project_id`: Google cloud project id containing all the resources that running this pipeline requires.
* `pipe_events.temp_bucket`: GCS bucket where temp files may be stored to.
* `pipe_events.pipeline_bucket`: GCS bucket where all final files generated by this pipeline may be stored to.
* `pipe_events.pipeline_dataset`: BigQuery dataset containing various tables used in this pipeline.

In addition to this, the following custom settings are required for this pipeline, and come with default values:

* `INFERENCE_BUFFER_DAYS`: Global airflow variable which determines the amount of days to reprocess because nnet scores might be avaialble. Defaults to `7`.
* `FLEXIBLE_OPERATOR`: Global airflow variable which determines the operator that will be used to process the events, the possible values could be `bash` or `kubernetes`. Defaults to `bash`.
* `pipe_events.events_dataset`: BigQuery dataset which will contain the published events. Defaults to `EVENTS_DATASET`.
* `pipe_events.source_dataset`: BigQuery dataset which contains the different tables that are read to produce the summarized events. Defaults to `PIPELINE_DATASET`.
* `pipe_events.publish_to_postgres`: Flag to decide if the results are published to posgres. It also can be specified at event level instead of pipe_events level. Since if you only want to publish ports, encounters and fishing but not gaps you can do it. Defaults to `false`.
* `pipe_events.gaps.source_table`: BigQuery table to read gaps from. Defaults to `position_messages_`.
* `pipe_events.gaps.events_table`: BigQuery table to publish gap events to. Defaults to `published_events_gaps`.
* `pipe_events.gaps.segment_vessel`: BigQuery table containing segment information, used to calculate gaps. Defaults to `segment_vessel`.
* `pipe_events.gaps.vessel_info`: BigQuery table to read vessel information from. Defaults to `vessel_info`.
* `pipe_events.gaps.gap_min_pos_count`: Only consider segments with a given minimum amount of positions. Defaults to `3`.
* `pipe_events.gaps.gap_min_dist`: Minimum distance to shore to consider the transponder off events. Defaults to `10000`.
* `pipe_events.encounters.source_table`: BigQuery table containing the raw encounters to read from. Defaults to `encounters`.
* `pipe_events.encounters.vessel_info`: BigQuery table to read vessel information from. Defaults to `vessel_info`.
* `pipe_events.encounters.events_table`: BigQuery table to publish the encounters to. Defaults to `published_events_encounters`.
* `pipe_events.anchorages.source_table`: BigQuery table containing the raw port events to read from. Defaults to `port_events_`.
* `pipe_events.anchorages.vessel_info`: BigQuery table to read vessel information from. Defaults to `vessel_info`.
* `pipe_events.anchorages.events_table`: BigQuery table to publish the anchorages to. Defaults to `published_events_ports`.
* `pipe_events.anchorages.anchorages_dataset`: BigQuery dataset which contains the named anchorages table. Defaults to `gfw_research`.
* `pipe_events.anchorages.named_anchorages`: BigQuery table containing anchorage information. Defaults to `named_anchorages_v20190307`.
    **DEPRECATED BY INCREMENTAL LOAD**
* `pipe_events.fishing.source_table`: BigQuery table containing the scored messages to read from. Defaults to `messages_scored_`.
* `pipe_events.fishing.events_table`: BigQuery table to publish the fishing events to. Defaults to `published_events_fishing`.
* `pipe_events.fishing.segment_vessel`: BigQuery table containing segent information. Defaults to `segment_vessel`.
* `pipe_events.fishing.segment_info`: BigQuery table containing segment information. Defaults to `segment_info`.
* `pipe_events.fishing.vessel_info`: BigQuery table to read vessel information from. Defaults to `vessel_info`.
* `pipe_events.fishing.min_event_duration`: BigQuery table containing the minimum amount of seconds to consider a fishing event actually a fishing event. Defaults to `300`.

Finally, the following custom entries do not provide a default value and must be manually configured before using this pipeline:

* `pipe_events.postgres_instance`: CloudSQL postgres instance where the data is published to.
* `pipe_events.postgres_connection_string`: Connection string for the postgres database to publish the events to.
* `pipe_events.postgres_table`: Table in postgres to publish the events to.

# License

Copyright 2017 Global Fishing Watch

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
