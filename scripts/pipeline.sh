#!/usr/bin/env bash

PIPELINE='pipe_events'
PIPELINE_VERSION=$(python -c "from importlib.metadata import version; print(version('${PIPELINE}'))")
