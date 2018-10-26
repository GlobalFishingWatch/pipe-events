#!/usr/bin/env bash

THIS_SCRIPT_DIR="$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )"

display_usage() {
	echo "Available Commands"
	echo "  gap_events                publish gap (on/off at sea) events"
	echo "  encounter_events          publish encounter (vessel rendevouz at sea) events"
	echo "  anchorage_events          publish port (port in and out) events"
	echo "  fishing_events            publish continuous fishing message events"
	}


if [[ $# -le 0 ]]
then
    display_usage
    exit 1
fi


case $1 in

  gap_events)
    ${THIS_SCRIPT_DIR}/gap_events.sh "${@:2}"
    ;;

  encounter_events)
    ${THIS_SCRIPT_DIR}/encounter_events.sh "${@:2}"
    ;;

  anchorage_events)
    ${THIS_SCRIPT_DIR}/anchorage_events.sh "${@:2}"
    ;;

  fishing_events)
    ${THIS_SCRIPT_DIR}/fishing_events.sh "${@:2}"
    ;;

  *)
    display_usage
    exit 1
    ;;
esac
