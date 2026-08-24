#!/bin/bash

./examples/run_port_visit_events.sh \
  --start-date 2012-01-01 \
  --end-date 2026-01-01 \
  --bq-in-port-visits world-fishing-827.vi_928_quick_fix_3.port_visits \
  --bq-in-named-anchorages global-fishing-watch.anchorages.named_anchorages \
  --bq-in-identity-published-dataset world-fishing-827.vi_928_quick_fix_3 \
  --bq-out-dataset world-fishing-827.vi_928_quick_fix_3 \
  --bq-out-table-prefix product \
  --bq-in-spatial-measures global-fishing-watch.pipe_static.spatial_measures_20201105 \
  --bq-in-regions global-fishing-watch.pipe_regions_layers.event_regions \
  --pvis-field-prefix self_reported_ 




