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


for PARENT_DAG in pipe_events_daily pipe_events_monthly; do

  python $AIRFLOW_HOME/utils/set_default_variables.py \
      --force docker_image=$1 \
      $PARENT_DAG.gaps \
      source_table="position_messages_" \
      events_table="published_events_gaps" \
      gap_min_pos_count="3" \
      gap_min_dist="10000" \


  python $AIRFLOW_HOME/utils/set_default_variables.py \
      --force docker_image=$1 \
      $PARENT_DAG.encounters \
      source_table="encounters" \
      events_table="published_events_encounters" \


  python $AIRFLOW_HOME/utils/set_default_variables.py \
      --force docker_image=$1 \
      $PARENT_DAG.anchorages \
      source_table="port_events_" \
      events_table="published_events_ports" \


  python $AIRFLOW_HOME/utils/set_default_variables.py \
      --force docker_image=$1 \
      $PARENT_DAG.fishing \
      source_table="messages_scored_" \
      events_table="published_events_fishing" \

done

echo "Installation Complete"
