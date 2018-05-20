#!/bin/bash
set -e

source pipe-tools-utils

THIS_SCRIPT_DIR="$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )"
ASSETS=${THIS_SCRIPT_DIR}/../assets
source ${THIS_SCRIPT_DIR}/pipeline.sh

display_usage() {
	echo -e "\nUsage:\n gap_events YYYY-MM-DD[,YYYY-MM-DD] MESSAGES_TABLE EVENTS_TABLE MIN_POS_COUNT \n"
	}


if [[ $# -ne 4  ]]
then
    display_usage
    exit 1
fi

DATE_RANGE=$1
MESSAGES_TABLE=$2
EVENTS_TABLE=$3
MIN_POS_COUNT=$4

IFS=, read START_DATE END_DATE <<<"${DATE_RANGE}"
if [[ -z $END_DATE ]]; then
  END_DATE=${START_DATE}
fi


DELETE_SQL=${ASSETS}/delete-daterange.sql.j2
INSERT_SQL=${ASSETS}/gap-events.sql.j2
SCHEMA=${ASSETS}/events.schema.json
TABLE_DESC=(
  "* Pipeline: ${PIPELINE} ${PIPELINE_VERSION}"
  "* Source: ${MESSAGES_TABLE}"
  "* Command:"
  "$(basename $0)"
  "$@"
)
TABLE_DESC=$( IFS=$'\n'; echo "${TABLE_DESC[*]}" )


echo "Publishing gap events to ${EVENTS_TABLE}..."
echo "${TABLE_DESC}"

echo "  Create table"

bq mk --force \
  --description "${TABLE_DESC}" \
  --schema ${SCHEMA} \
  --time_partitioning_field=timestamp \
  ${EVENTS_TABLE}

echo "  Deleting existing records for ${START_DATE} to ${END_DATE}"

jinja2 ${DELETE_SQL} -D table=${EVENTS_TABLE//:/.} -D start_date=${START_DATE} -D end_date=${END_DATE} \
     | bq query --max_rows=0

echo "  Inserting new records for ${START_DATE} to ${END_DATE}"

jinja2 ${INSERT_SQL} \
   -D source=${MESSAGES_TABLE//:/.} \
   -D dest=${EVENTS_TABLE//:/.} \
   -D start_date=${START_DATE} \
   -D end_date=${END_DATE} \
   -D min_pos_count=${MIN_POS_COUNT} \
   | bq query --max_rows=0


echo "  ${EVENTS_TABLE} Done."


