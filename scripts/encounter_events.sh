#!/bin/bash
source pipe-tools-utils

THIS_SCRIPT_DIR="$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )"
ASSETS=${THIS_SCRIPT_DIR}/../assets
source ${THIS_SCRIPT_DIR}/pipeline.sh

display_usage() {
	echo -e "\nUsage:\n$0 SOURCE_TABLE DEST_TABLE \n"
	}


if [[ $# -ne 2  ]]
then
    display_usage
    exit 1
fi

SOURCE_TABLE=$1
DEST_TABLE=$2

SQL=${ASSETS}/encounter-events.sql.j2
SOURCE_TABLE=${SOURCE_TABLE//:/.}
SCHEMA=${ASSETS}/events.schema.json
TABLE_DESC=(
  "* Pipeline: ${PIPELINE} ${PIPELINE_VERSION}"
  "* Source: ${SOURCE_TABLE}"
  "* Command:"
  "$(basename $0)"
  "$@"
)
TABLE_DESC=$( IFS=$'\n'; echo "${TABLE_DESC[*]}" )

echo "Publishing encounters events to ${DEST_TABLE}..."
echo "${TABLE_DESC}"

echo "  Create table"

bq mk --force \
  --description "${TABLE_DESC}" \
  --schema ${SCHEMA} \
  ${DEST_TABLE}

if [ "$?" -ne 0 ]; then
  echo "  Unable to create table ${DEST_TABLE}"
  exit 1
fi

echo "  Regenerating records"

jinja2 ${SQL} -D source=${SOURCE_TABLE} \
     | bq -q query --max_rows=0 --allow_large_results --replace \
      --destination_table ${DEST_TABLE}

if [ "$?" -ne 0 ]; then
  echo "  Unable to insert records for table ${DEST_TABLE}"
  exit 1
fi

echo "Updating table description ${DEST_TABLE}"

if [ "$?" -ne 0 ]; then
  echo "  Unable to update table descriptions for table ${DEST_TABLE}"
  exit 1
fi

bq update --description "${TABLE_DESC}" ${DEST_TABLE}

if [ "$?" -ne 0 ]; then
  echo "  Unable to update table descriptions for table ${DEST_TABLE}"
  exit 1
fi

echo "  ${DEST_TABLE} Done."


