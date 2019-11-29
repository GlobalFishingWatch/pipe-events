#!/bin/bash

THIS_SCRIPT_DIR="$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )"

# XXX: This is a hack which causes all postgres operations to be executed
# serially while we develop a strategy to not overload the database via the
# import processes.
airflow pool -s postgres 1 "Ensure serial access to the postgres database"

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
    publish_to_postgres="false" \
    all_vessels_table="vessel_database.all_vessels_v20191001" \
    spatial_measures_table="pipe_static.spatial_measures_20181025" \
    country_codes_table="gfw_research.country_codes" \
    named_anchorages_table="anchorages.named_anchorages_v20190827" \
    voyages="voyages" \
    vessel_info="vessel_info" \
    segment_vessel="segment_vessel" \
    segment_info="segment_info" \

python $AIRFLOW_HOME/utils/set_default_variables.py \
    --force docker_image=$1 \
    pipe_events.anchorages \
    source_table="port_events_" \
    events_table="published_events_ports" \
    enabled="false" \

python $AIRFLOW_HOME/utils/set_default_variables.py \
    --force docker_image=$1 \
    pipe_events.carrier_encounters \
    source_table="encounters" \
    max_median_speed_knots="2" \
    enabled="false" \
    fishing_vessels_table="proj_carrier_portal_pew.carrier_portal_fishing_vessels_v20190916" \
    events_table="published_events_encounters" \

python $AIRFLOW_HOME/utils/set_default_variables.py \
    --force docker_image=$1 \
    pipe_events.encounters \
    source_table="encounters" \
    vessel_info="vessel_info" \
    events_table="published_events_encounters" \
    max_median_speed_knots="2" \
    enabled="false" \

python $AIRFLOW_HOME/utils/set_default_variables.py \
    --force docker_image=$1 \
    pipe_events.fishing \
    source_table="messages_scored_" \
    min_event_duration="300" \
    enabled="false" \
    events_table="published_events_fishing" \

python $AIRFLOW_HOME/utils/set_default_variables.py \
    --force docker_image=$1 \
    pipe_events.gaps \
    source_table="position_messages_" \
    events_table="published_events_gaps" \
    gap_min_pos_count="3" \
    gap_min_dist="10000" \
    enabled="false" \

echo "Installation Complete"
