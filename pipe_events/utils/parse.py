import argparse
import datetime
import re
import logging
import json
import pkg_resources
import sys
import os

PIPELINE_NAME = "pipe-events"
PIPELINE_VERSION = pkg_resources.require(PIPELINE_NAME)[0].version
PIPELINE_DESCRIPTION = "Generate the incremental fishing events"

DEFAULT = dict(
    test=False,
    verbose=0,
    quiet=0,
    project="world-fishing-827",
    table_description="",
    # incremental_fishing_events
    start_date="2020-01-01",
    end_date="2020-01-02",
    messages_table="world-fishing-827.pipe_ais_test_202408290000_internal.research_messages",
    segs_activity_table="world-fishing-827.pipe_ais_test_202408290000_published.segs_activity",
    segment_vessel_table="world-fishing-827.pipe_ais_test_202408290000_internal.segment_vessel",
    product_vessel_info_summary_table=("world-fishing-827.pipe_ais_test_202408290000_published"
                                       ".product_vessel_info_summary"),
    nnet_score_night_loitering="nnet_score",
    max_fishing_event_gap_hours=2,
    destination_dataset="world-fishing-827.scratch_matias_ttl_7_days",
    destination_table_prefix="incremental_fishing_events",
    labels_incremental='{"environment":"develop"}',
    # auth and regions
    source_fishing_events=("world-fishing-827.scratch_matias_ttl_7_days."
                           "incremental_fishing_events_filtered"),
    source_night_loitering_events=("world-fishing-827.scratch_matias_ttl_7_days."
                                   "incremental_night_loitering_events_filtered"),
    vessel_identity_core="world-fishing-827.pipe_ais_v3_internal.identity_core",
    vessel_identity_authorization="world-fishing-827.pipe_ais_v3_internal.identity_authorization",
    spatial_measures_table="world-fishing-827.pipe_static.spatial_measures_clustered_20230307",
    regions_table="world-fishing-827.pipe_regions_layers.event_regions",
    all_vessels_byyear=("world-fishing-827.pipe_ais_test_202408290000_published."
                        "product_vessel_info_summary"),
    destination="world-fishing-827.scratch_matias_ttl_7_days.fishing_events_final",
    labels_auth='{"environment":"develop"}',
)


def valid_date(s: str) -> datetime.date:
    try:
        return datetime.datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        raise argparse.ArgumentTypeError(f"not a valid date: {s!r}")


def valid_table(s: str) -> datetime.datetime:
    matched = re.fullmatch(r"[\w\-_]+[:\.][\w\-_]+\.[\w\-_]+", s)
    if matched is None:
        raise argparse.ArgumentTypeError(
            f"not a valid table pattern (project.dataset.table): {s!r}"
        )
    return s


def setup_logging(verbosity):
    base_loglevel = getattr(logging, (os.getenv("LOGLEVEL", "WARNING")).upper())
    verbosity = min(verbosity, 2)
    loglevel = base_loglevel - (verbosity * 10)
    logging.basicConfig(stream=sys.stdout, level=loglevel, format="%(message)s")


def parse(arguments):
    parser = argparse.ArgumentParser(
        description=f"{PIPELINE_NAME}:{PIPELINE_VERSION} - {PIPELINE_DESCRIPTION}"
    )

    # Common arguments
    parser.add_argument(
        "--test",
        action="store_true",
        help="Test mode - print query and exit. Do not run queries",
        default=DEFAULT["test"],
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        dest="verbosity",
        default=DEFAULT["verbose"],
        help="verbose output (repeat for increased verbosity)",
    )
    parser.add_argument(
        "-q",
        "--quiet",
        action="store_const",
        const=-1,
        default=DEFAULT["quiet"],
        dest="verbosity",
        help="quiet output (show errors only)",
    )
    parser.add_argument(
        "--project",
        type=str,
        help="GCP project id (default: %(default)s)",
        default=DEFAULT["project"],
    )
    parser.add_argument(
        "--table_description",
        type=str,
        help="Additional text to include in the output table description",
        default=DEFAULT["table_description"],
    )

    # operations
    subparsers = parser.add_subparsers(dest="operation", required=True)
    incremental = subparsers.add_parser(
        "incremental_events",
        help="Generates the incremental fishing or night loitering events.",
    )
    auth_and_regions = subparsers.add_parser(
        "auth_and_regions_fishing_events",
        help="Combine the fishing and night_loitering with authorization and regions.",
    )

    incremental.add_argument(
        "-start",
        "--start_date",
        help="The start date of the source messages.",
        type=valid_date,
        default=DEFAULT["start_date"],
    )
    incremental.add_argument(
        "-end",
        "--end_date",
        help="The end date of the source messages.",
        type=valid_date,
        default=DEFAULT["end_date"],
    )
    incremental.add_argument(
        "-messages",
        "--messages_table",
        help="The source messages table having fishing and night loitering info.",
        type=valid_table,
        default=DEFAULT["messages_table"],
    )
    incremental.add_argument(
        "-segsact",
        "--segs_activity_table",
        help="The segments activity table.",
        type=valid_table,
        default=DEFAULT["segs_activity_table"],
    )
    incremental.add_argument(
        "-segvessel",
        "--segment_vessel_table",
        help="The segment vessel table.",
        type=valid_table,
        default=DEFAULT["segment_vessel_table"],
    )
    incremental.add_argument(
        "-pvesselinfo",
        "--product_vessel_info_summary_table",
        help="The prodiuct vessel info summary table.",
        type=valid_table,
        default=DEFAULT["product_vessel_info_summary_table"],
    )
    incremental.add_argument(
        "-sfield",
        "--nnet_score_night_loitering",
        help="The field name that has the score to eval.",
        choices=["nnet_score", "night_loitering"],
        default=DEFAULT["nnet_score_night_loitering"],
    )
    incremental.add_argument(
        "-maxhs",
        "--max_fishing_event_gap_hours",
        help="The max gap hours of yesterday to get potentially open events.",
        type=int,
        default=DEFAULT["max_fishing_event_gap_hours"],
    )
    incremental.add_argument(
        "-dest",
        "--destination_dataset",
        help="The destination dataset having fishing events.",
        type=str,
        default=DEFAULT["destination_dataset"],
    )
    incremental.add_argument(
        "-dest_tbl_prefix",
        "--destination_table_prefix",
        help="The destination table prefix having fishing events.",
        type=str,
        default=DEFAULT["destination_table_prefix"],
    )
    incremental.add_argument(
        "-labels",
        "--labels",
        help="The labels assigned to each table.",
        type=json.loads,
        default=DEFAULT["labels_incremental"],
    )

    auth_and_regions.add_argument(
        "-source_fishing",
        "--source_fishing_events",
        help="The incremental fishing events table.",
        type=valid_table,
        default=DEFAULT["source_fishing_events"],
    )
    auth_and_regions.add_argument(
        "-source_nl",
        "--source_night_loitering_events",
        help="The night loitering events table.",
        type=valid_table,
        default=DEFAULT["source_night_loitering_events"],
    )
    auth_and_regions.add_argument(
        "-idcore",
        "--vessel_identity_core",
        help="The vessel identity core table.",
        type=valid_table,
        default=DEFAULT["vessel_identity_core"],
    )
    auth_and_regions.add_argument(
        "-idauth",
        "--vessel_identity_authorization",
        help="The vessel identity authorization table.",
        type=valid_table,
        default=DEFAULT["vessel_identity_authorization"],
    )
    auth_and_regions.add_argument(
        "-measures",
        "--spatial_measures_table",
        help="The spatial measures table.",
        type=valid_table,
        default=DEFAULT["spatial_measures_table"],
    )
    auth_and_regions.add_argument(
        "-regions",
        "--regions_table",
        help="The event regions table.",
        type=valid_table,
        default=DEFAULT["regions_table"],
    )
    auth_and_regions.add_argument(
        "-allvessels",
        "--all_vessels_byyear",
        help="The all vessels by year table.",
        type=valid_table,
        default=DEFAULT["all_vessels_byyear"],
    )
    auth_and_regions.add_argument(
        "-dest",
        "--destination",
        help="The destination table having fishing events.",
        type=valid_table,
        default=DEFAULT["destination"],
    )
    auth_and_regions.add_argument(
        "-labels",
        "--labels",
        help="The labels assigned to each table.",
        type=json.loads,
        default=DEFAULT["labels_auth"],
    )

    args = parser.parse_args(arguments[1:])
    if hasattr(args, "start_date") and hasattr(args, "end_date"):
        args.start_date = args.start_date.strftime("%Y-%m-%d")
        args.end_date = args.end_date.strftime("%Y-%m-%d")
    setup_logging(args.verbosity)
    log = logging.getLogger()

    args.base_table_description = (
        f"Pipeline: {PIPELINE_NAME}:v{PIPELINE_VERSION}\n"
        f"Description: {PIPELINE_DESCRIPTION}\n"
    )

    log.info(args.base_table_description)
    log.info("==========================")

    return args
