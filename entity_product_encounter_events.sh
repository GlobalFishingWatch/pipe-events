#!/bin/bash

./examples/run_loitering_events.sh \
  --start-date 2012-01-01 \
  --end-date 2026-01-01 \
  --bq-in-loitering gfw-int-ais-datalake.loitering_v1.loitering \
  --bq-in-segment-info world-fishing-827.prj_entity_hull.entity_epoch_v20260801 \
  --bq-in-research-segments global-fishing-watch.pipe_ais_v5_published.segs_activity \
  --bq-in-identity-published-dataset world-fishing-827.vi_928_quick_fix_3 \
  --pvis-field-prefix self_reported_ \
  --minimum-distance-from-shore-nm 20 \
  --bq-in-voyages world-fishing-827.vi_928_quick_fix_3.voyages_c4 \
  --bq-in-port-visits world-fishing-827.vi_928_quick_fix_3.product_port_visit_events_v20260101 \
  --bq-in-spatial-measures global-fishing-watch.pipe_static.spatial_measures_20201105 \
  --bq-out-dataset world-fishing-827.vi_928_quick_fix_3 \
  --bq-out-table-prefix product \
  --bq-in-regions global-fishing-watch.pipe_regions_layers.event_regions 