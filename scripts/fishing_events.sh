#!/bin/bash
source pipe-tools-utils

THIS_SCRIPT_DIR="$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )"
ASSETS=${THIS_SCRIPT_DIR}/../assets
source ${THIS_SCRIPT_DIR}/pipeline.sh

display_usage() {
	echo -e "\nUsage:\n$0 YYYY-MM-DD[,YYYY-MM-DD] SOURCE_TABLE SEGMENT_VESSEL SEGMENT_INFO DEST_TABLE \n"
	}


if [[ $# -ne 5  ]]
then
    display_usage
    exit 1
fi

DATE_RANGE=$1
SOURCE_TABLE=$2
SEGMENT_VESSEL=$3
SEGMENT_INFO=$4
DEST_TABLE=$5

IFS=, read START_DATE END_DATE <<<"${DATE_RANGE}"
if [[ -z $END_DATE ]]; then
  END_DATE=${START_DATE}
fi


DELETE_SQL=${ASSETS}/delete-daterange.sql.j2
INSERT_SQL=${ASSETS}/fishing-events.sql.j2
SCHEMA=${ASSETS}/events.schema.json
TABLE_DESC=(
  "* Pipeline: ${PIPELINE} ${PIPELINE_VERSION}"
  "* Source: ${SOURCE_TABLE}"
  "* Command:"
  "$(basename $0)"
  "$@"
)
TABLE_DESC=$( IFS=$'\n'; echo "${TABLE_DESC[*]}" )


echo "Publishing fishing events to ${DEST_TABLE}..."
echo "${TABLE_DESC}"

echo "  Create table"

bq mk --force \
  --description "${TABLE_DESC}" \
  --schema ${SCHEMA} \
  --time_partitioning_field=timestamp \
  ${DEST_TABLE}

if [ "$?" -ne 0 ]; then
  echo "  Unable to create table ${DEST_TABLE}"
  exit 1
fi

echo "  Deleting existing records for ${START_DATE} to ${END_DATE}"

jinja2 ${DELETE_SQL} -D table=${DEST_TABLE//:/.} -D start_date=${START_DATE} -D end_date=${END_DATE} \
     | bq query --max_rows=0

if [ "$?" -ne 0 ]; then
  echo "  Unable to delete records for table ${DEST_TABLE} from ${START_DATE} to ${END_DATE}"
  exit 1
fi

echo "  Inserting new records for ${START_DATE} to ${END_DATE}"

jinja2 ${INSERT_SQL} \
   -D messages=${SOURCE_TABLE//:/.} \
   -D segment_vessel=${SEGMENT_VESSEL//:/.} \
   -D segment_info=${SEGMENT_INFO//:/.} \
   -D dest=${DEST_TABLE//:/.} \
   -D start_yyyymmdd=$(yyyymmdd ${START_DATE}) \
   -D end_yyyymmdd=$(yyyymmdd ${END_DATE}) \
   | bq query --max_rows=0

if [ "$?" -ne 0 ]; then
  echo "  Unable to insert records for table ${DEST_TABLE} from ${START_DATE} to ${END_DATE}"
  exit 1
fi

echo "  ${DEST_TABLE} Done."
