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
    --entrypoint pipe pipeline \
    -v \
    --project $pipeline_project \
    --table_description "Incremental fishing events based on $nnet_score_nl" \
    incremental_events \
    -start $start_d \
    -end $end_d \
    -messages $internal_ds.research_messages \
    -sfield $nnet_score_nl \
    -dest $dest_ds \
    -dest_tbl_prefix ${pipeline_prefix}_${nnet_score_nl} \
    -labels '{"environment": "production", "resource_creator": "chris", "project": "core_pipeline", "version": "v3", "step": "fishing_events", "stage":  "productive"}'
done


echo "2. Filtered"
for nnet_score_nl in nnet_score night_loitering; do
  echo $nnet_score_nl
    docker compose run \
    --remove-orphans \
    --entrypoint pipe pipeline  \
    -v  \
    --project $pipeline_project  \
    --table_description '"Filtered fishing events based on $nnet_score_nl"' \
    incremental_filter_events \
    -sfield $nnet_score_nl \
    -segsact $published_ds.segs_activity \
    -segvessel $internal_ds.segment_vessel \
    -pvesselinfo $published_ds.product_vessel_info_summary \
    -mtbl $dest_ds.${pipeline_prefix}_${nnet_score_nl}_merged \
    -dest $dest_ds \
    -dest_tbl_prefix ${pipeline_prefix}_${nnet_score_nl} \
    -labels '{"environment": "production", "resource_creator": "chris", "project": "core_pipeline", "version": "v3", "step": "fishing_events", "stage":  "productive"}'
done


echo "3. Authorizations"
 docker compose run \
 --remove-orphans \
 --entrypoint pipe pipeline  \
 -v  \
 --project $pipeline_project  \
 --table_description '"Fishing events with authorizations"' \
 auth_and_regions_fishing_events      \
 -source_fishing $dest_ds.${pipeline_prefix}_nnet_score_filtered \
 -source_nl $dest_ds.${pipeline_prefix}_night_loitering_filtered \
 -idcore $published_ds.identity_core \
 -idauth $published_ds.identity_authorization \
 -measures $pipe_static.spatial_measures_20201105 \
 -regions $pipe_regions_layers.event_regions \
 -allvessels $published_ds.product_vessel_info_summary \
 -dest $dest_ds.${pipeline_prefix}_fishing_events_v \
 -dest_view $dest_ds.${pipeline_prefix}_fishing_events \
 -rdate $end_d \
    -labels '{"environment": "production", "resource_creator": "chris", "project": "core_pipeline", "version": "v3", "step": "fishing_events", "stage":  "productive"}'


echo "4. Restrictive"
 docker compose run \
 --remove-orphans \
 --entrypoint pipe pipeline  \
 -v  \
 --project $pipeline_project  \
 --table_description '"Restrictive fishing events used in products"' \
 fishing_restrictive \
 -source_events $dest_ds.${pipeline_prefix}_fishing_events_v  \
 -destrest $dest_ds.${pipeline_prefix}_product_events_fishing_v \
 -destrestview $dest_ds.${pipeline_prefix}_product_events_fishing \
 -rdate $end_d \
    -labels '{"environment": "production", "resource_creator": "chris", "project": "core_pipeline", "version": "v3", "step": "fishing_events", "stage":  "productive"}'


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
