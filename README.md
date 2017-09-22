# Events pipeline

This repository contains the fishing events pipeline, a dataflow pipeline which
extracts summarized fishing events from a collection of scored AIS messages.

# Running

## Dependencies

You just need [docker](https://www.docker.com/) and
[docker-compose](https://docs.docker.com/compose/) in your machine to run the
pipeline. No other dependency is required.

## Prerequisites

The pipeline reads it's input from BigQuery, so you need to first authenticate
with your google cloud account inside the docker images. To do that, you need
to run this command and follow the instructions:

```
docker-compose run gcloud auth login
```

## Running locally

To run the pipeline locally, you only need to configure a source query and a
sink to upload the results to. For example, you could run this:

```
docker-compose run pipeline \
  --project world-fishing-827 \
  --source @examples/local.sql \
  --sink world-fishing-827:scratch_andres.sample \
  --sink_write_disposition WRITE_TRUNCATE
```

* `--project` defines the project used to run the bigquery
  source queries in.

* `--source` defines where the source data is coming from. You can use either
  the `@file.sql` syntax to specify a file which contains the actual bigquery
query, or you can inline the query by using `--source "SELECT * FROM table"`.

* `--sink` indicates the fully qualified bigquery table name where results are
  pushed to.

* `--sink_write_disposition` indicates what to do if the sink table already
  exists and has records. `WRITE_TRUNCATE` deletes all previous records before
inserting the new ones produced by the pipeline. `WRITE_APPEND` appends the new
records instead.

## Running in Google Cloud Dataflow

To run the pipline remotely on google cloud dataflow you need to setup a couple
more parameters that specify how the pipeline runs in the cloud infrastructure.
For example, you could run this:

```
docker-compose run pipeline
  --runner dataflow  \
  --project world-fishing-827 \
  --source @examples/source.sql \
  --sink world-fishing-827:dataset.table \
  --sink_write_disposition WRITE_TRUNCATE \
  --temp_location gs://my-pipeline-scratch/pipe-events/ \
  --setup_file ./setup.py \
  --job_name my-pipline \
  --max_num_workers=200
```

These are the same parameters that we use on the local run, plus the following
dataflow-specific ones:

* `--runner dataflow` specifies that we want to run this pipeline in google
  cloud dataflow.

* `--project` now specifies both where the source bigquery queries will be run
  and where the dataflow workers will be created.

* `--temp_location` specifies where temporary files for the pipeline run will
  be created.

* `--setup_file ./setup.py` specifies the setup file that is used in the
  pipeline workers.

* `--job_name` specifies the google cloud dataflow job name which identifies
  this run.

* `--max_num_workers` specifies the maximum number of workers that will be
  created in dataflow. Defaults to 15 if not specified.

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
