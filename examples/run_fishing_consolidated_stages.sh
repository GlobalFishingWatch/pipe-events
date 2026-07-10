#!/bin/bash
set -euo pipefail

# Runs the whole-history stages of the fishing events pipeline in sequence, chaining the
# output of each into the next:
#
#   fishing_events_incremental_filter   (once per score field)  -> *_filtered
#   fishing_events_auth_and_regions                             -> *_fishing_events(_v)
#   fishing_events_restrictive                                  -> *_product_events_fishing(_v)
#
# The daily/incremental step that produces the `*_merged` inputs lives in
# run_fishing_incremental_stages.sh. This script only needs the two merged tables it
# produced, plus the upstream reference tables.
#
# Naming conventions are ignored on purpose: this is a local-testing helper, so inputs
# are grouped by source dataset to make them easy to point around.

# Fixed for local testing: billing/execution project and output-table labels.
EXECUTION_PROJECT="world-fishing-827"
LABELS='{"project": "ais", "mode": "development", "stage": "fishing_intervals--v1"}'

# Required parameters (no defaults).
reference_date=""
bq_in_merged_nnet_score=""
bq_in_merged_night_loitering=""
bq_in_identity_published_dataset=""   # identity_core, identity_authorization, product_vessel_info_summary
bq_in_ais_published_dataset=""        # segs_activity
bq_in_ais_internal_dataset=""         # segment_vessel
bq_out_dataset=""
bq_out_table_prefix=""

# Stable reference inputs; defaulted but overridable.
bq_in_udfs_dataset="global-fishing-watch.udfs_v2"
bq_in_spatial_measures="world-fishing-827.pipe_static.spatial_measures_20201105"
bq_in_regions="world-fishing-827.pipe_regions_layers.event_regions"

usage() {
  cat <<'EOF'
Usage: run_fishing_consolidated_stages.sh \
  --reference-date YYYY-MM-DD \
  --bq-in-merged-nnet-score PROJECT.DATASET.TABLE \
  --bq-in-merged-night-loitering PROJECT.DATASET.TABLE \
  --bq-in-identity-published-dataset PROJECT.DATASET \
  --bq-in-ais-published-dataset PROJECT.DATASET \
  --bq-in-ais-internal-dataset PROJECT.DATASET \
  --bq-out-dataset PROJECT.DATASET \
  --bq-out-table-prefix PREFIX \
  [--bq-in-udfs-dataset PROJECT.DATASET] \
  [--bq-in-spatial-measures PROJECT.DATASET.TABLE] \
  [--bq-in-regions PROJECT.DATASET.TABLE]

Chains filter -> auth_and_regions -> restrictive. Output tables:
  <bq-out-dataset>.<bq-out-table-prefix>_<score_field>_filtered
  <bq-out-dataset>.<bq-out-table-prefix>_fishing_events_v          (view: _fishing_events)
  <bq-out-dataset>.<bq-out-table-prefix>_product_events_fishing_v  (view: _product_events_fishing)

Identity-published dataset supplies identity_core, identity_authorization and
product_vessel_info_summary. AIS-published supplies segs_activity; AIS-internal supplies
segment_vessel.
EOF
}

while [[ $# -gt 0 ]]; do
  case $1 in
    --reference-date) reference_date="$2"; shift 2 ;;
    --bq-in-merged-nnet-score) bq_in_merged_nnet_score="$2"; shift 2 ;;
    --bq-in-merged-night-loitering) bq_in_merged_night_loitering="$2"; shift 2 ;;
    --bq-in-identity-published-dataset) bq_in_identity_published_dataset="$2"; shift 2 ;;
    --bq-in-ais-published-dataset) bq_in_ais_published_dataset="$2"; shift 2 ;;
    --bq-in-ais-internal-dataset) bq_in_ais_internal_dataset="$2"; shift 2 ;;
    --bq-out-dataset) bq_out_dataset="$2"; shift 2 ;;
    --bq-out-table-prefix) bq_out_table_prefix="$2"; shift 2 ;;
    --bq-in-udfs-dataset) bq_in_udfs_dataset="$2"; shift 2 ;;
    --bq-in-spatial-measures) bq_in_spatial_measures="$2"; shift 2 ;;
    --bq-in-regions) bq_in_regions="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown parameter passed: $1" >&2; usage >&2; exit 1 ;;
  esac
done

# Validate required parameters.
missing=()
[[ -z "$reference_date" ]] && missing+=(--reference-date)
[[ -z "$bq_in_merged_nnet_score" ]] && missing+=(--bq-in-merged-nnet-score)
[[ -z "$bq_in_merged_night_loitering" ]] && missing+=(--bq-in-merged-night-loitering)
[[ -z "$bq_in_identity_published_dataset" ]] && missing+=(--bq-in-identity-published-dataset)
[[ -z "$bq_in_ais_published_dataset" ]] && missing+=(--bq-in-ais-published-dataset)
[[ -z "$bq_in_ais_internal_dataset" ]] && missing+=(--bq-in-ais-internal-dataset)
[[ -z "$bq_out_dataset" ]] && missing+=(--bq-out-dataset)
[[ -z "$bq_out_table_prefix" ]] && missing+=(--bq-out-table-prefix)
if [[ ${#missing[@]} -gt 0 ]]; then
  echo "Missing required parameter(s): ${missing[*]}" >&2
  usage >&2
  exit 1
fi

# Derived table names.
out="${bq_out_dataset}.${bq_out_table_prefix}"
filtered_nnet_score="${out}_nnet_score_filtered"
filtered_night_loitering="${out}_night_loitering_filtered"
fishing_events_v="${out}_fishing_events_v"
fishing_events_view="${out}_fishing_events"
product_events_v="${out}_product_events_fishing_v"
product_events_view="${out}_product_events_fishing"

# Upstream reference tables.
identity_core="${bq_in_identity_published_dataset}.identity_core"
identity_authorization="${bq_in_identity_published_dataset}.identity_authorization"
product_vessel_info_summary="${bq_in_identity_published_dataset}.product_vessel_info_summary"
segs_activity="${bq_in_ais_published_dataset}.segs_activity"
segment_vessel="${bq_in_ais_internal_dataset}.segment_vessel"

echo "----------------------------------------"
echo "Execution project: $EXECUTION_PROJECT"
echo "Reference date:    $reference_date"
echo "Output dataset:    $bq_out_dataset"
echo "Output prefix:     $bq_out_table_prefix"
echo "Merged (nnet):     $bq_in_merged_nnet_score"
echo "Merged (nl):       $bq_in_merged_night_loitering"
echo "Identity dataset:  $bq_in_identity_published_dataset"
echo "AIS published:     $bq_in_ais_published_dataset"
echo "AIS internal:      $bq_in_ais_internal_dataset"
echo "UDFs dataset:      $bq_in_udfs_dataset"
echo "Spatial measures:  $bq_in_spatial_measures"
echo "Regions:           $bq_in_regions"
echo "----------------------------------------"

echo "1. Filtered"
for score_field in nnet_score night_loitering; do
  case $score_field in
    nnet_score) merged_in="$bq_in_merged_nnet_score"; filtered_out="$filtered_nnet_score" ;;
    night_loitering) merged_in="$bq_in_merged_night_loitering"; filtered_out="$filtered_night_loitering" ;;
  esac
  echo "==> fishing_events_incremental_filter ($score_field)"
  docker compose run \
    --rm \
    --entrypoint pipe-events pipeline \
    -v \
    --project "$EXECUTION_PROJECT" \
    --table-description "Filtered fishing events based on $score_field" \
    fishing_events_incremental_filter \
    --score-field "$score_field" \
    --bq-in-udfs-dataset "$bq_in_udfs_dataset" \
    --bq-in-segments-activity "$segs_activity" \
    --bq-in-segment-vessel "$segment_vessel" \
    --bq-in-product-vessel-info-summary "$product_vessel_info_summary" \
    --bq-in-merged-events "$merged_in" \
    --bq-out-filtered-events "$filtered_out" \
    --labels "$LABELS"
done

echo "2. Authorizations and regions"
docker compose run \
  --rm \
  --entrypoint pipe-events pipeline \
  -v \
  --project "$EXECUTION_PROJECT" \
  --table-description "Fishing events with authorizations" \
  fishing_events_auth_and_regions \
  --bq-in-udfs-dataset "$bq_in_udfs_dataset" \
  --bq-in-fishing-events "$filtered_nnet_score" \
  --bq-in-night-loitering-events "$filtered_night_loitering" \
  --bq-in-vessel-identity-core "$identity_core" \
  --bq-in-vessel-identity-authorization "$identity_authorization" \
  --bq-in-spatial-measures "$bq_in_spatial_measures" \
  --bq-in-regions "$bq_in_regions" \
  --bq-in-product-vessel-info-summary "$product_vessel_info_summary" \
  --bq-out-events "$fishing_events_v" \
  --bq-out-events-view "$fishing_events_view" \
  --reference-date "$reference_date" \
  --labels "$LABELS"

echo "3. Restrictive"
docker compose run \
  --rm \
  --entrypoint pipe-events pipeline \
  -v \
  --project "$EXECUTION_PROJECT" \
  --table-description "Restrictive fishing events used in products" \
  fishing_events_restrictive \
  --bq-in-events "$fishing_events_v" \
  --bq-out-events "$product_events_v" \
  --bq-out-events-view "$product_events_view" \
  --reference-date "$reference_date" \
  --labels "$LABELS"
