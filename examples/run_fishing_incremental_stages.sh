#!/bin/bash
set -euo pipefail

# Runs the daily/incremental stage of the fishing events pipeline
# (`fishing_events_incremental`) over a date range, once for each score field.
#
# This is the only genuinely incremental step, so it is usually tested on its own over a
# couple of days at a time. The whole-history steps that consume its `*_merged` output
# live in run_fishing_consolidated_stages.sh.

# Fixed for local testing: billing/execution project and output-table labels.
EXECUTION_PROJECT="world-fishing-827"
LABELS='{"project": "ais", "mode": "development", "stage": "fishing_intervals--v1"}'

# Required parameters (no defaults).
start_date=""
end_date=""
bq_in_messages=""
bq_out_dataset=""
bq_out_table_prefix=""

usage() {
  cat <<'EOF'
Usage: run_fishing_incremental_stages.sh \
  --start-date YYYY-MM-DD \
  --end-date YYYY-MM-DD \
  --bq-in-messages PROJECT.DATASET.TABLE \
  --bq-out-dataset PROJECT.DATASET \
  --bq-out-table-prefix PREFIX

Runs fishing_events_incremental for both nnet_score and night_loitering.
Each variant writes to:
  <bq-out-dataset>.<bq-out-table-prefix>_<score_field>_merged
EOF
}

while [[ $# -gt 0 ]]; do
  case $1 in
    --start-date) start_date="$2"; shift 2 ;;
    --end-date) end_date="$2"; shift 2 ;;
    --bq-in-messages) bq_in_messages="$2"; shift 2 ;;
    --bq-out-dataset) bq_out_dataset="$2"; shift 2 ;;
    --bq-out-table-prefix) bq_out_table_prefix="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown parameter passed: $1" >&2; usage >&2; exit 1 ;;
  esac
done

# Validate required parameters.
missing=()
[[ -z "$start_date" ]] && missing+=(--start-date)
[[ -z "$end_date" ]] && missing+=(--end-date)
[[ -z "$bq_in_messages" ]] && missing+=(--bq-in-messages)
[[ -z "$bq_out_dataset" ]] && missing+=(--bq-out-dataset)
[[ -z "$bq_out_table_prefix" ]] && missing+=(--bq-out-table-prefix)
if [[ ${#missing[@]} -gt 0 ]]; then
  echo "Missing required parameter(s): ${missing[*]}" >&2
  usage >&2
  exit 1
fi

echo "----------------------------------------"
echo "Execution project: $EXECUTION_PROJECT"
echo "Start date:        $start_date"
echo "End date:          $end_date"
echo "Input messages:    $bq_in_messages"
echo "Output dataset:    $bq_out_dataset"
echo "Output prefix:     $bq_out_table_prefix"
echo "----------------------------------------"

for score_field in nnet_score night_loitering; do
  echo "==> fishing_events_incremental ($score_field)"
  docker compose run \
    --rm \
    --entrypoint pipe-events pipeline \
    -v \
    --project "$EXECUTION_PROJECT" \
    --table-description "Incremental fishing events based on $score_field" \
    fishing_events_incremental \
    --start-date "$start_date" \
    --end-date "$end_date" \
    --bq-in-messages "$bq_in_messages" \
    --score-field "$score_field" \
    --bq-out-merged-events "${bq_out_dataset}.${bq_out_table_prefix}_${score_field}_merged" \
    --labels "$LABELS"
done
