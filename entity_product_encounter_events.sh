#!/bin/bash

./examples/run_encounter_events.sh \
  --start-date 2012-01-01 \
  --end-date 2026-01-01 \
  --bq-in-encounters world-fishing-827.vi_928_quick_fix_3.encounters \
  --bq-in-voyages world-fishing-827.vi_928_quick_fix_3.voyages_c4 \
  --bq-in-port-visits world-fishing-827.vi_928_quick_fix_3.product_port_visit_events_v20260101 \
  --bq-in-identity-published-dataset global-fishing-watch.pipe_ais_identity_v5_published \
  --bq-in-pvis-table world-fishing-827.vi_928_quick_fix_3.product_vessel_info_summary \
  --bq-in-spatial-measures global-fishing-watch.pipe_static.spatial_measures_20201105 \
  --bq-in-regions global-fishing-watch.pipe_regions_layers.event_regions \
  --pvis-field-prefix self_reported_ \
  --bq-out-dataset world-fishing-827.vi_928_quick_fix_3 \
  --bq-out-table-prefix product 