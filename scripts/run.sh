#!/usr/bin/env bash

THIS_SCRIPT_DIR="$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )"

display_usage() {
	echo "Available Commands"
	echo "  legacy_events    run dataflow job to create legacy fishing events"
	echo "  gap_events       publish gap (on/off at sea) events"
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

  gap_events)
    ${THIS_SCRIPT_DIR}/gap_events.sh "${@:2}"
    ;;

  *)
    display_usage
    exit 0
    ;;
esac
