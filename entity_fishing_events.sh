#!/bin/bash

./examples/run_fishing_consolidated_stages.sh \
  --reference-date 2026-01-01 \
  --bq-in-merged-nnet-score gfw-int-ais-datalake.fishing_intervals_v1.daily_fishing_events \
  --bq-in-merged-night-loitering gfw-int-ais-datalake.fishing_intervals_v1.daily_night_loitering_events \
  --bq-in-identity-published-dataset global-fishing-watch.pipe_ais_identity_v5_published \
  --bq-in-identity-entity-dataset world-fishing-827.vi_928_quick_fix_3 \
  --bq-in-ais-published-dataset gfw-int-ais-datalake.summarized_positions_v1 \
  --bq-in-entity-internal-dataset world-fishing-827.prj_entity_hull \
  --bq-out-dataset world-fishing-827.vi_928_quick_fix_3 \
  --bq-out-table-prefix filtered 




