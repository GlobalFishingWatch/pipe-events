#!/bin/bash
set -e

source pipe-tools-utils

THIS_SCRIPT_DIR="$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )"
ASSETS=${THIS_SCRIPT_DIR}/../assets
source ${THIS_SCRIPT_DIR}/pipeline.sh

display_usage() {
	echo -e "\nUsage:\n correlate_detections YYYY-MM-DD[,YYYY-MM-DD] POSITIONS_TABLE SEGMENTS_TABLE DEST_TABLE \n"
	}


if [[ $# -ne 4  ]]
then
    display_usage
    exit 1
fi

DATE_RANGE=$1
POSITIONS_TABLE=$2
SEGMENTS_TABLE=$3
DEST_TABLE=$4

IFS=, read START_DATE END_DATE <<<"${DATE_RANGE}"
if [[ -z $END_DATE ]]; then
  END_DATE=${START_DATE}
fi


SQL=${ASSETS}/correlate-detections.sql.j2
TABLE_DESC=(
  "* Pipeline: ${PIPELINE} ${PIPELINE_VERSION}"
  "* Source: ${SOURCE_TABLE}"
  "* Command:"
  "$(basename $0)"
  "$@"
)
TABLE_DESC=$( IFS=$'\n'; echo "${TABLE_DESC[*]}" )

echo "Publishing correlated detections to ${DEST_TABLE}..."
echo "  ${START_DATE} to ${END_DATE}"
echo "${TABLE_DESC}"


jinja2 ${SQL} \
   -D source=${SOURCE_TABLE//:/.} \
   -D start_date=${START_DATE} \
   -D end_date=${END_DATE} \
   | bq query --max_rows=0 --allow_large_results --replace \
     --destination_table ${DEST_TABLE}


echo "  ${DEST_TABLE} Done."


