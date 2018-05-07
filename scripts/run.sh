#!/usr/bin/env bash

THIS_SCRIPT_DIR="$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )"

display_usage() {
	echo "Available Commands"
	echo "  legacy_events    run dataflow job to create legacy fishing events"
	}


if [[ $# -le 0 ]]
then
    display_usage
    exit 1
fi


case $1 in

  legacy_events)
    python -m pipe_events
    ;;


  *)
    display_usage
    exit 0
    ;;
esac
