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
    events_dataset="{{ var.value.EVENTS_DATASET }}" \
    source_dataset="{{ var.value.PIPELINE_DATASET }}" \

python $AIRFLOW_HOME/utils/set_default_variables.py \
    --force docker_image=$1 \
    pipe_events.gaps \
    source_table="position_messages_" \
    events_table="published_events_gaps" \
    segment_vessel="segment_vessel" \
    vessel_info="vessel_info" \
    gap_min_pos_count="3" \
    gap_min_dist="10000" \

python $AIRFLOW_HOME/utils/set_default_variables.py \
    --force docker_image=$1 \
    pipe_events.encounters \
    source_table="encounters" \
    vessel_info="vessel_info" \
    events_table="published_events_encounters" \

python $AIRFLOW_HOME/utils/set_default_variables.py \
    --force docker_image=$1 \
    pipe_events.anchorages \
    source_table="port_events_" \
    vessel_info="vessel_info" \
    events_table="published_events_ports" \

python $AIRFLOW_HOME/utils/set_default_variables.py \
    --force docker_image=$1 \
    pipe_events.fishing \
    source_table="messages_scored_" \
    segment_vessel="segment_vessel" \
    segment_info="segment_info" \
    vessel_info="vessel_info" \
    min_event_duration="300" \
    events_table="published_events_fishing" \

echo "Installation Complete"
