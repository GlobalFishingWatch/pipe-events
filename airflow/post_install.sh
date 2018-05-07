#!/bin/bash

THIS_SCRIPT_DIR="$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )"

python $AIRFLOW_HOME/utils/set_default_variables.py \
    --force docker_image=$1 \
    pipe_events \
    dag_install_path="${THIS_SCRIPT_DIR}" \
    docker_run="{{ var.value.DOCKER_RUN }}" \
    project_id="{{ var.value.PROJECT_ID }}" \
    temp_bucket="{{ var.value.TEMP_BUCKET }}"  \
    pipeline_bucket="{{ var.value.PIPELINE_BUCKET }}" \
    pipeline_dataset="{{ var.value.PIPELINE_DATASET }}" \


    source_dataset="{{ var.value.PIPELINE_DATASET }}" \
    source_table="messages_segmented_" \
    dest_table="position_messages_" \
    measures="spatial_measures" \
    segments="segments_" \
    segment_identity="segment_identity_" \
    add_regions_source="position_messages_" \
    add_regions_dest="regions_" \
    regions_vector_source="gs://scratch-paul-ttl100/measures/oceans.zip" \
    region_fields="Ocean" \
    spatial_measures_source="world-fishing-827:pipe_staging_a.spatial_measures"

echo "Installation Complete"

