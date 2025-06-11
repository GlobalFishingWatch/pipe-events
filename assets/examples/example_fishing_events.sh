#!/bin/bash
## Usage:
## ./example_segment.sh scratch_output
if [ -z $1 ]; then grep "^##" $(dirname $0)/$(basename $0); exit 1; else DATASET_OUT=$1; fi
echo "Output dataset ${DATASET_OUT}."
PROJ="world-fishing-827"

# cleaning
bq rm -f ${DATASET_OUT}.incremental_fishing_events_merged && \
bq rm -f ${DATASET_OUT}.incremental_night_loitering_events_merged && \
bq rm -f ${DATASET_OUT}.incremental_fishing_events_filtered && \
bq rm -f ${DATASET_OUT}.incremental_night_loitering_events_filtered && \
bq rm -f ${DATASET_OUT}.fishing_events_v20200102 && \
bq rm -f ${DATASET_OUT}.fishing_events && \
bq rm -f ${DATASET_OUT}.fishing_events_restrictive_v20200102 && \
bq rm -f ${DATASET_OUT}.fishing_events_restrictive

docker compose run --rm --entrypoint pipe pipeline -v --table_description "Local Testing" incremental_events -dest ${DATASET_OUT}

docker compose run --rm --entrypoint pipe pipeline -v --table_description "Local Testing" incremental_events -sfield night_loitering -dest ${DATASET_OUT} -dest_tbl_prefix incremental_night_loitering_events

docker compose run --rm --entrypoint pipe pipeline -v --table_description "Local Testing" incremental_filter_events -dest ${DATASET_OUT}

docker compose run --rm --entrypoint pipe pipeline -v --table_description "Local Testing" incremental_filter_events -sfield night_loitering -dest ${DATASET_OUT} -dest_tbl_prefix incremental_night_loitering_events

docker compose run --rm --entrypoint pipe pipeline -v --table_description "Local Testing" auth_and_regions_fishing_events -dest ${PROJ}.${DATASET_OUT}.fishing_events_v -dest_view ${PROJ}.${DATASET_OUT}.fishing_events

docker compose run --rm --entrypoint pipe pipeline -v --table_description "Local Testing" fishing_restrictive -destrest ${PROJ}.${DATASET_OUT}.fishing_events_restrictive_v -destrestview ${PROJ}.${DATASET_OUT}.fishing_events_restrictive
