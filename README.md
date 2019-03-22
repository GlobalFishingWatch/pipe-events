# Events pipeline

This repository contains the events pipeline, a simple pipeline which extracts summarized events from various datasets we produce.

# Running

## Dependencies

You just need [docker](https://www.docker.com/) and [docker-compose](https://docs.docker.com/compose/) in your machine to run the pipeline. No other dependency is required.

## Setup

The pipeline reads it's input from BigQuery, so you need to first authenticate with your google cloud account inside the docker images. To do that, you need to run this command and follow the instructions:

```
docker-compose run gcloud auth login
```

## Configuration

The pipeline exposes the following standard settings:

* `pipe_events.docker_run`: Command to run docker inside the airflow server.
* `pipe_events.project_id`: Google cloud project id containing all the resources that running this pipeline requires.
* `pipe_events.temp_bucket`: GCS bucket where temp files may be stored to.
* `pipe_events.pipeline_bucket`: GCS bucket where all final files generated by this pipeline may be stored to.
* `pipe_events.pipeline_dataset`: BigQuery dataset containing various tables used in this pipeline.

In addition to this, the following custom settings are required for this
pipeline, and come with default values:

* `INFERENCE_BUFFER_DAYS`: Global airflow variable which determines the amount of days to reprocess because nnet scores might be avaialble. Defaults to `7`.
* `pipe_events.events_dataset`: BigQuery dataset which will contain the published events. Defaults to `EVENTS_DATASET`.
* `pipe_events.source_dataset`: BigQuery dataset which contains the different tables that are read to produce the summarized events. Defaults to `PIPELINE_DATASET`.
* `pipe_events.gaps.source_table`: BigQuery table to read gaps from. Defaults to `position_messages_`.
* `pipe_events.gaps.events_table`: BigQuery table to publish gap events to. Defaults to `published_events_gaps`.
* `pipe_events.gaps.segment_vessel`: BigQuery table containing segment information, used to calculate gaps. Defaults to `segment_vessel`.
* `pipe_events.gaps.gap_min_pos_count`: Only consider segments with a given minimum amount of positions. Defaults to `3`.
* `pipe_events.gaps.gap_min_dist`: Minimum distance to shore to consider the transponder off events. Defaults to `10000`.
* `pipe_events.encounters.source_table`: BigQuery table containing the raw encounters to read from. Defaults to `encounters`.
* `pipe_events.encounters.events_table`: BigQuery table to publish the encounters to. Defaults to `published_events_encounters`.
* `pipe_events.anchorages.source_table`: BigQuery table containing the raw port events to read from. Defaults to `port_events_`.
* `pipe_events.anchorages.events_table`: BigQuery table to publish the anchorages to. Defaults to `published_events_ports`.
* `pipe_events.fishing.source_table`: BigQuery table containing the scored messages to read from. Defaults to `messages_scored_`.
* `pipe_events.fishing.events_table`: BigQuery table to publish the fishing events to. Defaults to `published_events_fishing`.
* `pipe_events.fishing.segment_vessel`: BigQuery table containing segent information. Defaults to `segment_vessel`.
* `pipe_events.fishing.segment_info`: BigQuery table containing segment information. Defaults to `segment_info`.
* `pipe_events.fishing.min_event_duration`: BigQuery table containing the minimum amount of seconds to consider a fishing event actually a fishing event. Defaults to `300`.

Finally, the following custom entries do not provide a default value and must be manually configured before using this pipeline:

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
