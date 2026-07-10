#!/bin/bash
set -euo pipefail

# Runs the port visit events publisher (`port_visit_events`) for a date range, writing one
# versioned table and a view. Standalone helper for manual end-to-end testing.

# Fixed for local testing: billing/execution project and output-table labels.
EXECUTION_PROJECT="world-fishing-827"
LABELS='{"project": "ais", "mode": "development", "stage": "fishing_intervals--v1"}'

# Required parameters (no defaults).
start_date=""
end_date=""
bq_in_port_visits=""
bq_in_named_anchorages=""
bq_in_identity_published_dataset=""   # product_vessel_info_summary
bq_out_dataset=""
bq_out_table_prefix=""

# Stable reference inputs; defaulted but overridable.
bq_in_spatial_measures="global-fishing-watch.pipe_static.spatial_measures_clustered_v20260403"
bq_in_regions="global-fishing-watch.pipe_regions_layers.event_regions"
pvis_field_prefix="self_reported_"

usage() {
  cat <<'EOF'
Usage: run_port_visit_events.sh \
  --start-date YYYY-MM-DD \
  --end-date YYYY-MM-DD \
  --bq-in-port-visits PROJECT.DATASET.TABLE \
  --bq-in-named-anchorages PROJECT.DATASET.TABLE \
  --bq-in-identity-published-dataset PROJECT.DATASET \
  --bq-out-dataset PROJECT.DATASET \
  --bq-out-table-prefix PREFIX \
  [--bq-in-spatial-measures PROJECT.DATASET.TABLE] \
  [--bq-in-regions PROJECT.DATASET.TABLE] \
  [--pvis-field-prefix PREFIX]

Writes <bq-out-dataset>.<bq-out-table-prefix>_port_visit_events (versioned table + view).
The identity-published dataset supplies product_vessel_info_summary.
--pvis-field-prefix defaults to "ais_".
EOF
}

while [[ $# -gt 0 ]]; do
  case $1 in
    --start-date) start_date="$2"; shift 2 ;;
    --end-date) end_date="$2"; shift 2 ;;
    --bq-in-port-visits) bq_in_port_visits="$2"; shift 2 ;;
    --bq-in-named-anchorages) bq_in_named_anchorages="$2"; shift 2 ;;
    --bq-in-identity-published-dataset) bq_in_identity_published_dataset="$2"; shift 2 ;;
    --bq-out-dataset) bq_out_dataset="$2"; shift 2 ;;
    --bq-out-table-prefix) bq_out_table_prefix="$2"; shift 2 ;;
    --bq-in-spatial-measures) bq_in_spatial_measures="$2"; shift 2 ;;
    --bq-in-regions) bq_in_regions="$2"; shift 2 ;;
    --pvis-field-prefix) pvis_field_prefix="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown parameter passed: $1" >&2; usage >&2; exit 1 ;;
  esac
done

# Validate required parameters.
missing=()
[[ -z "$start_date" ]] && missing+=(--start-date)
[[ -z "$end_date" ]] && missing+=(--end-date)
[[ -z "$bq_in_port_visits" ]] && missing+=(--bq-in-port-visits)
[[ -z "$bq_in_named_anchorages" ]] && missing+=(--bq-in-named-anchorages)
[[ -z "$bq_in_identity_published_dataset" ]] && missing+=(--bq-in-identity-published-dataset)
[[ -z "$bq_out_dataset" ]] && missing+=(--bq-out-dataset)
[[ -z "$bq_out_table_prefix" ]] && missing+=(--bq-out-table-prefix)
if [[ ${#missing[@]} -gt 0 ]]; then
  echo "Missing required parameter(s): ${missing[*]}" >&2
  usage >&2
  exit 1
fi

# Derived tables.
product_vessel_info_summary="${bq_in_identity_published_dataset}.product_vessel_info_summary"
bq_out_events="${bq_out_dataset}.${bq_out_table_prefix}_port_visit_events"

echo "----------------------------------------"
echo "Execution project: $EXECUTION_PROJECT"
echo "Start date:        $start_date"
echo "End date:          $end_date"
echo "Port visits:       $bq_in_port_visits"
echo "Named anchorages:  $bq_in_named_anchorages"
echo "Identity dataset:  $bq_in_identity_published_dataset"
echo "PVIS field prefix: $pvis_field_prefix"
echo "Spatial measures:  $bq_in_spatial_measures"
echo "Regions:           $bq_in_regions"
echo "Output events:     $bq_out_events"
echo "----------------------------------------"

echo "==> port_visit_events"
docker compose run \
  --rm \
  --entrypoint pipe-events pipeline \
  -v \
  --project "$EXECUTION_PROJECT" \
  --table-description "Port visit events" \
  port_visit_events \
  --start-date "$start_date" \
  --end-date "$end_date" \
  --bq-in-port-visits "$bq_in_port_visits" \
  --bq-in-product-vessel-info-summary "$product_vessel_info_summary" \
  --product-vessel-info-summary-field-prefix "$pvis_field_prefix" \
  --bq-in-spatial-measures "$bq_in_spatial_measures" \
  --bq-in-regions "$bq_in_regions" \
  --bq-in-named-anchorages "$bq_in_named_anchorages" \
  --bq-out-events "$bq_out_events" \
  --labels "$LABELS"
