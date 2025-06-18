#!/bin/bash

ts=$(date +%Y%m%d%H%M%S)
pipeline_test_prefix="staging_bf_bfd_bftruncate"
pipeline_prefix="${pipeline_test_prefix}_${ts}"

# if an argument is passed it is pipeline_description
if [ $# -eq 1 ]; then
    pipeline_description="$1"
else
    pipeline_description="staging_bf_bfd_bftruncate"
fi

echo "Pipeline prefix: ${pipeline_prefix}"
echo "Pipeline description: ${pipeline_description}"

# Create log directory
log_dir="logs_${ts}"
mkdir -p "$log_dir"

# 1. Run a normal backfill from 2020-01-01 to 2020-12-31 (in background)
{
    echo "Starting pipeline 1: simple full load" 
    ../scripts/generate_incremental_fishing_events.sh \
        --pipeline_prefix ${pipeline_prefix}_1_bf \
        --start_d 2020-01-01 \
        --end_d 2021-01-01 \
        --pipeline_description "${pipeline_description}"
    echo "Completed pipeline 1"
} > "${log_dir}/pipeline1.log" 2>&1 &
pid1=$!

# 2. Run a backfill from 2020-01-01 to 2020-12-20, and then a daily load (in background)
{
    echo "Starting pipeline 2: full load followed by daily load"
    ../scripts/generate_incremental_fishing_events.sh \
        --pipeline_prefix ${pipeline_prefix}_2_bfd \
        --start_d 2020-01-01 \
        --end_d 2020-12-29 \
        --pipeline_description "${pipeline_description}"

    # Loop over 2020-12-29 to 2020-12-31
    for d in $(seq -w 29 31); do
        current_day=2020-12-${d}
        next_day=$(date -d "$current_day + 1 day" +%Y-%m-%d)
        ../scripts/generate_incremental_fishing_events.sh \
            --pipeline_prefix ${pipeline_prefix}_2_bfd \
            --start_d $current_day \
            --end_d $next_day \
            --pipeline_description "${pipeline_description}"
    done
    echo "Completed pipeline 2"
} > "${log_dir}/pipeline2.log" 2>&1 &
pid2=$!

# 3. Run a backfill from 2020-01-01 to 2020-12-31, then daily load for truncation test (in background)
{
    echo "Starting pipeline 3: full load followed by backfill daily load" 
    ../scripts/generate_incremental_fishing_events.sh \
        --pipeline_prefix ${pipeline_prefix}_3_bftruncate \
        --start_d 2020-01-01 \
        --end_d 2021-01-01 \
        --pipeline_description "${pipeline_description}"

    # Loop over 2020-12-29 to 2020-12-31
    for d in $(seq -w 29 31); do
        current_day=2020-12-${d}
        next_day=$(date -d "$current_day + 1 day" +%Y-%m-%d)
        ../scripts/generate_incremental_fishing_events.sh \
            --pipeline_prefix ${pipeline_prefix}_3_bftruncate \
            --start_d $current_day \
            --end_d $next_day \
            --pipeline_description "${pipeline_description}"
    done
    echo "Completed pipeline 3"
} > "${log_dir}/pipeline3.log" 2>&1 &
pid3=$!

# Wait for all pipelines to complete
echo "All pipelines running in parallel. Check logs in ${log_dir}/"
echo "Process IDs: $pid1 $pid2 $pid3"
wait $pid1 $pid2 $pid3
echo "All pipelines completed"