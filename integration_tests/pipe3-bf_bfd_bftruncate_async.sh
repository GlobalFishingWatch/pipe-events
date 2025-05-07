#!/bin/bash

# set name of pipeline
ts=$(date +%Y%m%d%H%M%S)
pipeline_test_prefix="pipe3_bf_bfd_bftruncate"
pipeline_prefix="${pipeline_test_prefix}_${ts}"

# set date range
start_date="2025-02-01"
end_date="2025-05-04"
x_days_incremental=5
end_date_minus_x_days=$(date -d "$end_date - $x_days_incremental days" +%Y-%m-%d)

# if an argument is passed it is pipeline_description
if [ $# -eq 1 ]; then
    pipeline_description="$1"
else
    pipeline_description="staging_bf_bfd_bftruncate"
fi

# set input datasets
source_pipe_dataset=${source_pipe_dataset:-pipe_ais_v3}
pipe_static_measures=${pipe_static_measures:-world-fishing-827.pipe_static.spatial_measures_clustered_20230307}
pipe_regions_layers=${pipe_regions_layers:-world-fishing-827.pipe_regions_layers.event_regions}

# Create log directory
log_dir="logs_${ts}"
mkdir -p $log_dir

# 1. Run a normal backfill from start_date to end_date
(
    echo "Starting pipeline 1 - simple full load"
    ../scripts/generate_incremental_fishing_events.sh \
        --pipeline_prefix ${pipeline_prefix}_1_bf \
        --start_d $start_date \
        --end_d $end_date \
        --source_pipe_dataset $source_pipe_dataset \
        --pipe_static_measures $pipe_static_measures \
        --pipe_regions_layers $pipe_regions_layers \
        --pipeline_description "${pipeline_description}"
    echo "Completed pipeline 1"
) > "${log_dir}/pipeline_1_bf.log" 2>&1 &

# 2. Run a backfill with daily load
(
    echo "Starting pipeline 2 - backfill with daily load"
    ../scripts/generate_incremental_fishing_events.sh \
        --pipeline_prefix ${pipeline_prefix}_2_bfd \
        --start_d $start_date \
        --end_d $end_date_minus_x_days \
        --source_pipe_dataset $source_pipe_dataset \
        --pipe_static_measures $pipe_static_measures \
        --pipe_regions_layers $pipe_regions_layers \
        --pipeline_description "${pipeline_description}" \
        --mode incremental

    # loop over end_date_minus_x_days to end_date
    current_day=$end_date_minus_x_days
    end_date_minus_one=$(date -d "$end_date - 1 day" +%Y-%m-%d)
    while [ "$current_day" != "$end_date" ]; do

        # set mode to incremental unless current_day == end_date
        if [ "$current_day" == "$end_date_minus_one" ]; then
            loop_mode=""
        else
            loop_mode="incremental"
        fi

        next_day=$(date -d "$current_day + 1 day" +%Y-%m-%d)
        echo "Running daily load for $current_day"
        ../scripts/generate_incremental_fishing_events.sh \
            --pipeline_prefix ${pipeline_prefix}_2_bfd \
            --start_d $current_day \
            --end_d $next_day \
            --source_pipe_dataset $source_pipe_dataset \
            --pipe_static_measures $pipe_static_measures \
            --pipe_regions_layers $pipe_regions_layers \
            --pipeline_description "${pipeline_description}" \
            --mode $loop_mode
        current_day=$next_day
    done
    echo "Completed pipeline 2"
) > "${log_dir}/pipeline_2_bfd.log" 2>&1 &

# 3. Run a backfill with daily backfill
(
    echo "Starting pipeline 3 - backfill with truncation"
    ../scripts/generate_incremental_fishing_events.sh \
        --pipeline_prefix ${pipeline_prefix}_3_bftruncate \
        --start_d $start_date \
        --end_d $end_date \
        --source_pipe_dataset $source_pipe_dataset \
        --pipe_static_measures $pipe_static_measures \
        --pipe_regions_layers $pipe_regions_layers \
        --pipeline_description "${pipeline_description}" \
        --mode incremental

    # loop over end_date_minus_x_days to end_date
    current_day=$end_date_minus_x_days
    end_date_minus_one=$(date -d "$end_date - 1 day" +%Y-%m-%d)
    while [ "$current_day" != "$end_date" ]; do

        # set mode to incremental unless current_day == end_date
        if [ "$current_day" == "$end_date_minus_one" ]; then
            loop_mode=""
        else
            loop_mode="incremental"
        fi

        next_day=$(date -d "$current_day + 1 day" +%Y-%m-%d)
        echo "Running truncation for $current_day"
        ../scripts/generate_incremental_fishing_events.sh \
            --pipeline_prefix ${pipeline_prefix}_3_bftruncate \
            --start_d $current_day \
            --end_d $next_day \
            --source_pipe_dataset $source_pipe_dataset \
            --pipe_static_measures $pipe_static_measures \
            --pipe_regions_layers $pipe_regions_layers \
            --pipeline_description "${pipeline_description}" \
            --mode $loop_mode
        current_day=$next_day
    done
    echo "Completed pipeline 3"
) > "${log_dir}/pipeline_3_bftruncate.log" 2>&1 &

# Wait for all background processes to complete
wait

# listen to Ctrl+C which should kill all background processes when pressed once
trap 'kill $(jobs -p)' SIGINT
# wait for all background processes to finish
wait
# Check if all pipelines completed successfully
if [ $? -eq 0 ]; then
    echo "All pipelines completed successfully."
else
    echo "One or more pipelines failed. Check logs for details."
fi

echo "All pipelines completed. Logs saved in $log_dir directory"