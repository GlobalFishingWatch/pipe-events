#!/bin/bash

# set defaults for all parameters
pipeline_prefix=${pipeline_prefix:-incremental_fishing_events_fix_daily_load_two}
pipeline_project=${pipeline_project:-world-fishing-827}
destination_dataset_no_project=${destination_dataset_no_project:-scratch_christian_homberg_ttl120d}
dest_ds=${dest_ds:-$pipeline_project.$destination_dataset_no_project}
internal_ds=${internal_ds:-$pipeline_project.pipe_ais_test_202408290000_internal}
published_ds=${published_ds:-$pipeline_project.pipe_ais_test_202408290000_published}
pipe_static=${pipe_static:-$pipeline_project.pipe_static}
pipe_regions_layers=${pipe_regions_layers:-$pipeline_project.pipe_regions_layers}
udfs_dataset=${udfs_dataset:-global-fishing-watch.udfs_v2}
start_d=${start_d:-2020-01-01}
end_d=${end_d:-2020-01-10}

# read those parameters from the command line using named arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --pipeline_prefix)
      pipeline_prefix="$2"
      shift
      shift
      ;;
    --pipeline_project)
      pipeline_project="$2"
      shift
      shift
      ;;
    --destination_dataset_no_project)
      destination_dataset_no_project="$2"
      shift
      shift
      ;;
    --dest_ds)
      dest_ds="$2"
      shift
      shift
      ;;
    --internal_ds)
      internal_ds="$2"
      shift
      shift
      ;;
    --published_ds)
      published_ds="$2"
      shift
      shift
      ;;
    --pipe_static)
      pipe_static="$2"
      shift
      shift
      ;;
    --pipe_regions_layers)
      pipe_regions_layers="$2"
      shift
      shift
      ;;
    --start_d)
      start_d="$2"
      shift
      shift
      ;;
    --end_d)
      end_d="$2"
      shift
      shift
      ;;
    --backup_prefix)
        backup_prefix="$2"
        shift
        shift
        ;;
    *)
        echo "Unknown parameter passed: $1"
        exit 1
  esac
done

end_d_shard=${end_d//-/}

echo "----------------------------------------"
echo "Pipeline prefix: $pipeline_prefix"
echo "Pipeline project: $pipeline_project"
echo "Destination dataset: $dest_ds"
echo "Internal dataset: $internal_ds"
echo "Published dataset: $published_ds"
echo "Pipe static dataset: $pipe_static"
echo "Pipe regions layers dataset: $pipe_regions_layers"
echo "Start date: $start_d"
echo "End date: $end_d"
echo "End date shard: $end_d_shard"
echo "Backup prefix: $backup_prefix"
echo "----------------------------------------"



echo "1. Merged"
for nnet_score_nl in nnet_score night_loitering; do
  echo $nnet_score_nl
  docker compose run \
    --remove-orphans \
    --entrypoint pipe-events pipeline \
    -v \
    --project $pipeline_project \
    --table-description "Incremental fishing events based on $nnet_score_nl" \
    fishing_events_incremental \
    --start-date $start_d \
    --end-date $end_d \
    --bq-in-messages $internal_ds.research_messages \
    --score-field $nnet_score_nl \
    --bq-out-merged-events $dest_ds.${pipeline_prefix}_${nnet_score_nl}_merged \
    --labels '{"environment": "production", "resource_creator": "chris", "project": "core_pipeline", "version": "v3", "step": "fishing_events", "stage":  "productive"}'
done


echo "2. Filtered"
for nnet_score_nl in nnet_score night_loitering; do
  echo $nnet_score_nl
    docker compose run \
    --remove-orphans \
    --entrypoint pipe-events pipeline  \
    -v  \
    --project $pipeline_project  \
    --table-description '"Filtered fishing events based on $nnet_score_nl"' \
    fishing_events_incremental_filter \
    --score-field $nnet_score_nl \
    --bq-in-udfs-dataset $udfs_dataset \
    --bq-in-segments-activity $published_ds.segs_activity \
    --bq-in-segment-vessel $internal_ds.segment_vessel \
    --bq-in-product-vessel-info-summary $published_ds.product_vessel_info_summary \
    --bq-in-merged-events $dest_ds.${pipeline_prefix}_${nnet_score_nl}_merged \
    --bq-out-filtered-events $dest_ds.${pipeline_prefix}_${nnet_score_nl}_filtered \
    --labels '{"environment": "production", "resource_creator": "chris", "project": "core_pipeline", "version": "v3", "step": "fishing_events", "stage":  "productive"}'
done


echo "3. Authorizations"
 docker compose run \
 --remove-orphans \
 --entrypoint pipe-events pipeline  \
 -v  \
 --project $pipeline_project  \
 --table-description '"Fishing events with authorizations"' \
 fishing_events_auth_and_regions      \
 --bq-in-udfs-dataset $udfs_dataset \
 --bq-in-fishing-events $dest_ds.${pipeline_prefix}_nnet_score_filtered \
 --bq-in-night-loitering-events $dest_ds.${pipeline_prefix}_night_loitering_filtered \
 --bq-in-vessel-identity-core $published_ds.identity_core \
 --bq-in-vessel-identity-authorization $published_ds.identity_authorization \
 --bq-in-spatial-measures $pipe_static.spatial_measures_20201105 \
 --bq-in-regions $pipe_regions_layers.event_regions \
 --bq-in-product-vessel-info-summary $published_ds.product_vessel_info_summary \
 --bq-out-events $dest_ds.${pipeline_prefix}_fishing_events_v \
 --bq-out-events-view $dest_ds.${pipeline_prefix}_fishing_events \
 --reference-date $end_d \
    --labels '{"environment": "production", "resource_creator": "chris", "project": "core_pipeline", "version": "v3", "step": "fishing_events", "stage":  "productive"}'


echo "4. Restrictive"
 docker compose run \
 --remove-orphans \
 --entrypoint pipe-events pipeline  \
 -v  \
 --project $pipeline_project  \
 --table-description '"Restrictive fishing events used in products"' \
 fishing_events_restrictive \
 --bq-in-events $dest_ds.${pipeline_prefix}_fishing_events_v  \
 --bq-out-events $dest_ds.${pipeline_prefix}_product_events_fishing_v \
 --bq-out-events-view $dest_ds.${pipeline_prefix}_product_events_fishing \
 --reference-date $end_d \
    --labels '{"environment": "production", "resource_creator": "chris", "project": "core_pipeline", "version": "v3", "step": "fishing_events", "stage":  "productive"}'


# clone all created tables if backup_prefix exists
# this is useful for running a full backfill first and then incrementally backfilling single days
if [ -n "$backup_prefix" ]; then
echo "----------------------------------------"
echo "Backup tables"
echo "----------------------------------------"
echo "1. Merged"
echo "nnet_score"
bq cp --clone --no_clobber $destination_dataset_no_project.${pipeline_prefix}_nnet_score_merged $destination_dataset_no_project.${backup_prefix}_${pipeline_prefix}_nnet_score_merged

echo "night_loitering"
bq cp --clone --no_clobber $destination_dataset_no_project.${pipeline_prefix}_night_loitering_merged $destination_dataset_no_project.${backup_prefix}_${pipeline_prefix}_night_loitering_merged

echo "2. Filtered"
echo "nnet_score"
bq cp --clone --no_clobber $destination_dataset_no_project.${pipeline_prefix}_nnet_score_filtered $destination_dataset_no_project.${backup_prefix}_${pipeline_prefix}_nnet_score_filtered

echo "night_loitering"
bq cp --clone --no_clobber $destination_dataset_no_project.${pipeline_prefix}_night_loitering_filtered $destination_dataset_no_project.${backup_prefix}_${pipeline_prefix}_night_loitering_filtered

echo "3. Authorizations"
echo "fishing_events_v"
bq cp --clone --no_clobber $destination_dataset_no_project.${pipeline_prefix}_fishing_events_v$end_d_shard $destination_dataset_no_project.${backup_prefix}_${pipeline_prefix}_fishing_events_v$end_d_shard

echo "4. Restrictive"
bq cp --clone --no_clobber $destination_dataset_no_project.${pipeline_prefix}_product_events_fishing_v$end_d_shard $destination_dataset_no_project.${backup_prefix}_${pipeline_prefix}_product_events_fishing_v$end_d_shard

fi
