#!/bin/bash

# set name of pipeline
ts=$(date +%Y%m%d%H%M%S)
pipeline_test_prefix="pipe3_bf_bfd_bftruncate"
pipeline_prefix="${pipeline_test_prefix}_${ts}"

# set date range
start_year=2012
end_year=2012
end_year_plus_one=$((end_year + 1))


# set input datasets
source_pipe_dataset=${source_pipe_dataset:-pipe_ais_v3}
pipe_static_measures=${pipe_static_measures:-world-fishing-827.pipe_static.spatial_measures}
pipe_regions_layers=${pipe_regions_layers:-world-fishing-827.pipe_regions_layers.event_regions}

# 1. run a normal backfill from start_year-01-01 to end_year-12-31
../scripts/generate_incremental_fishing_events.sh \
    --pipeline_prefix ${pipeline_prefix}_1_bf \
    --start_d $start_year-01-01 \
    --end_d $end_year_plus_one-01-01 \
    --source_pipe_dataset $source_pipe_dataset \
    --pipe_static_measures $pipe_static_measures \
    --pipe_regions_layers $pipe_regions_layers


# 2. run a backfill from start_year-01-01 to end_year-12-28, and then a daily load from end_year-12-29 to end_year-12-31
../scripts/generate_incremental_fishing_events.sh \
    --pipeline_prefix ${pipeline_prefix}_2_bfd \
    --start_d $start_year-01-01 \
    --end_d $end_year-12-29 \
    --source_pipe_dataset $source_pipe_dataset \
    --pipe_static_measures $pipe_static_measures \
    --pipe_regions_layers $pipe_regions_layers


# loop over end_year-12-29 to end_year-12-31
# TODO: currently, this runs the non-incremental steps in each iteration instead of `latest_only`
# TODO: this is unnecessarily slow and expensive
for d in $(seq -w 29 31); do
current_day=$end_year-12-${d}
next_day=$(date -d "$current_day + 1 day" +%Y-%m-%d)
    ../scripts/generate_incremental_fishing_events.sh \
        --pipeline_prefix ${pipeline_prefix}_2_bfd \
        --start_d $current_day \
        --end_d $next_day \
        --source_pipe_dataset $source_pipe_dataset \
        --pipe_static_measures $pipe_static_measures \
        --pipe_regions_layers $pipe_regions_layers
done

# 3. run a backfill from start_year-01-01 to end_year-12-31, and then a (backfill) daily load from end_year-12-29 to end_year-12-31 to test truncation
../scripts/generate_incremental_fishing_events.sh \
    --pipeline_prefix ${pipeline_prefix}_3_bftruncate \
    --start_d $start_year-01-01 \
    --end_d $end_year_plus_one-01-01 \
    --source_pipe_dataset $source_pipe_dataset \
    --pipe_static_measures $pipe_static_measures \
    --pipe_regions_layers $pipe_regions_layers

# loop over end_year-12-29 to end_year-12-31
# TODO: currently, this runs the non-incremental steps in each iteration instead of `latest_only`
# TODO: this is unnecessarily slow and expensive
for d in $(seq -w 29 31); do
current_day=$end_year-12-${d}
next_day=$(date -d "$current_day + 1 day" +%Y-%m-%d)
    ../scripts/generate_incremental_fishing_events.sh \
        --pipeline_prefix ${pipeline_prefix}_3_bftruncate \
        --start_d $current_day \
        --end_d $next_day \
        --source_pipe_dataset $source_pipe_dataset \
        --pipe_static_measures $pipe_static_measures \
        --pipe_regions_layers $pipe_regions_layers
done