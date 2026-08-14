import argparse
import logging
import json
import sys
import os
from pipe_events.utils.validators import valid_date, valid_table
from importlib.metadata import version

PIPELINE_NAME = "pipe-events"
PIPELINE_VERSION = version(PIPELINE_NAME)
PIPELINE_DESCRIPTION = "Generate the incremental fishing events"
PROJ = "world-fishing-827"
DATASET_IN = "pipe_ais_test_202408290000"
DATASET_OUT = "scratch_matias_ttl7d"


DEFAULT = dict(
    # common
    test=False,
    verbose=0,
    quiet=0,
    project=PROJ,
    table_description="",
    labels='{"environment":"develop"}',
    reference_date="2020-01-02",
    # incremental_fishing_events
    start_date="2020-01-01",
    end_date="2020-01-02",
    messages_table=f"{PROJ}.{DATASET_IN}_internal.research_messages",
    nnet_score_night_loitering="nnet_score",
    max_fishing_event_gap_hours=2,
    max_fishing_event_gap_m=10000,
    max_fishing_event_merge_gap_seconds=3600,
    max_fishing_event_merge_gap_m=2000,
    destination_dataset=f"{PROJ}.{DATASET_OUT}",
    destination_table_prefix="incremental_fishing_events",
    use_merged_table=None,
    # incremental_filter_fishing_events
    segs_activity_table=f"{PROJ}.{DATASET_IN}_published.segs_activity",
    segment_vessel_table=f"{PROJ}.{DATASET_IN}_internal.segment_vessel",
    product_vessel_info_summary_table=(f"{PROJ}.{DATASET_IN}_published"
                                       ".product_vessel_info_summary"),
    merged_table=(f"{PROJ}.{DATASET_OUT}."
                  "incremental_fishing_events_merged"),
    min_event_duration_seconds=1200,
    min_event_positions=5,
    max_avg_speed_knots=10,
    min_event_distance_m=500,
    min_squid_jigger_event_distance_m=50,
    # auth and regions
    source_fishing_events=(f"{PROJ}.{DATASET_OUT}."
                           "incremental_fishing_events_filtered"),
    source_night_loitering_events=(f"{PROJ}.{DATASET_OUT}."
                                   "incremental_night_loitering_events_filtered"),
    vessel_identity_core=f"{PROJ}.pipe_ais_v3_internal.identity_core",
    vessel_identity_authorization=f"{PROJ}.pipe_ais_v3_internal.identity_authorization",
    spatial_measures_table=f"{PROJ}.pipe_static.spatial_measures_clustered_20230307",
    regions_table=f"{PROJ}.pipe_regions_layers.event_regions",
    all_vessels_byyear=(f"{PROJ}.{DATASET_IN}_published."
                        "product_vessel_info_summary"),
    nautical_time_raster_table=(f"{PROJ}.paper_global_longline_sets."
                                "nautical_time_raster_v20211117"),
    overlap_threshold_hours=2.0,
    destination=f"{PROJ}.{DATASET_OUT}.fishing_events_v",
    dest_view=f"{PROJ}.{DATASET_OUT}.fishing_events",
    # fishing_restrictive
    source_restrictive_events=f"{PROJ}.{DATASET_OUT}.fishing_events_v",
    dest_restrictive_events=f"{PROJ}.{DATASET_OUT}.fishing_events_restrictive_v",
    dest_rest_view=f"{PROJ}.{DATASET_OUT}.fishing_restrictive_events",
)


def setup_logging(verbosity):
    base_loglevel = getattr(logging, (os.getenv("LOGLEVEL", "WARNING")).upper())
    verbosity = min(verbosity, 2)
    loglevel = base_loglevel - (verbosity * 10)
    logging.basicConfig(stream=sys.stdout, level=loglevel, format="%(message)s")


def parse(arguments):
    parser = argparse.ArgumentParser(
        description=f"{PIPELINE_NAME}:{PIPELINE_VERSION} - {PIPELINE_DESCRIPTION}"
    )

    ################################################################################
    # Common arguments
    ################################################################################
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

    ################################################################################
    # incremental_events
    ################################################################################
    incremental = subparsers.add_parser(
        "incremental_events",
        help="Generates the incremental fishing or night loitering events.",
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
        "--max_fishing_event_gap_m",
        help="Maximum distance in metres between consecutive positions before an event is split.",
        type=int,
        default=DEFAULT["max_fishing_event_gap_m"],
    )
    incremental.add_argument(
        "--max_fishing_event_merge_gap_seconds",
        help="Maximum time gap in seconds between two fishing bursts that can be merged.",
        type=int,
        default=DEFAULT["max_fishing_event_merge_gap_seconds"],
    )
    incremental.add_argument(
        "--max_fishing_event_merge_gap_m",
        help="Maximum distance in metres between two fishing bursts that can be merged.",
        type=int,
        default=DEFAULT["max_fishing_event_merge_gap_m"],
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
        default=DEFAULT["labels"],
    )
    incremental.add_argument(
        "-mtbl",
        "--use_merged_table",
        help="The product vessel info summary table.",
        type=valid_table,
        required=False,
        default=DEFAULT["use_merged_table"],
    )

    ################################################################################
    # incremental_filter
    ################################################################################
    incremental_filter = subparsers.add_parser(
        "incremental_filter_events",
        help="Takes the incremental fishing or night loitering events and apply filters.",
    )
    incremental_filter.add_argument(
        "-segsact",
        "--segs_activity_table",
        help="The segments activity table.",
        type=valid_table,
        default=DEFAULT["segs_activity_table"],
    )
    incremental_filter.add_argument(
        "-segvessel",
        "--segment_vessel_table",
        help="The segment vessel table.",
        type=valid_table,
        default=DEFAULT["segment_vessel_table"],
    )
    incremental_filter.add_argument(
        "-pvesselinfo",
        "--product_vessel_info_summary_table",
        help="The prodiuct vessel info summary table.",
        type=valid_table,
        default=DEFAULT["product_vessel_info_summary_table"],
    )
    incremental_filter.add_argument(
        "--product_vessel_info_summary_field_prefix",
        help="""
            Prefix to use to access vessel info fields in
            `product_vessel_info_summary_table`. This is to account for
            differences between PVIS tables in different environments. For
            example, on ais this is `ais_`, but VMS PVIS has no prefix
            """,
    )
    incremental_filter.add_argument(
        "-sfield",
        "--nnet_score_night_loitering",
        help="The field name that has the score to eval.",
        choices=["nnet_score", "night_loitering"],
        default=DEFAULT["nnet_score_night_loitering"],
    )
    incremental_filter.add_argument(
        "-dest",
        "--destination_dataset",
        help="The destination dataset having fishing events.",
        type=str,
        default=DEFAULT["destination_dataset"],
    )
    incremental_filter.add_argument(
        "-dest_tbl_prefix",
        "--destination_table_prefix",
        help="The destination table prefix having fishing events.",
        type=str,
        default=DEFAULT["destination_table_prefix"],
    )
    incremental_filter.add_argument(
        "-labels",
        "--labels",
        help="The labels assigned to each table.",
        type=json.loads,
        default=DEFAULT["labels"],
    )
    incremental_filter.add_argument(
        "-mtbl",
        "--merged_table",
        help="An existence merged table.",
        type=valid_table,
        required=False,
        default=DEFAULT["merged_table"],
    )
    incremental_filter.add_argument(
        "--min_event_duration_seconds",
        help="Minimum fishing event duration in seconds.",
        type=int,
        default=DEFAULT["min_event_duration_seconds"],
    )
    incremental_filter.add_argument(
        "--min_event_positions",
        help="Minimum number of AIS positions in a fishing event.",
        type=int,
        default=DEFAULT["min_event_positions"],
    )
    incremental_filter.add_argument(
        "--max_avg_speed_knots",
        help="Maximum average speed in knots for a fishing event.",
        type=float,
        default=DEFAULT["max_avg_speed_knots"],
    )
    incremental_filter.add_argument(
        "--min_event_distance_m",
        help="Minimum event distance in metres for non-squid-jigger fishing events.",
        type=int,
        default=DEFAULT["min_event_distance_m"],
    )
    incremental_filter.add_argument(
        "--min_squid_jigger_event_distance_m",
        help="Minimum event distance in metres for squid jigger fishing events.",
        type=int,
        default=DEFAULT["min_squid_jigger_event_distance_m"],
    )

    ################################################################################
    # auth_and_regions
    ################################################################################
    auth_and_regions = subparsers.add_parser(
        "auth_and_regions_fishing_events",
        help="Combine the fishing and night_loitering with authorization and regions.",
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
        "--product_vessel_info_summary_table",
        help="The all vessels by year table.",
        type=valid_table,
        default=DEFAULT["all_vessels_byyear"],
    )
    auth_and_regions.add_argument(
        "--product_vessel_info_summary_field_prefix",
        help="""
            Prefix to use to access vessel info fields in
            `product_vessel_info_summary_table`. This is to account for
            differences between PVIS tables in different environments. For
            example, on ais this is `ais_`, but VMS PVIS has no prefix
            """,
    )
    auth_and_regions.add_argument(
        "-dest",
        "--destination",
        help="The destination table having fishing events.",
        type=valid_table,
        default=DEFAULT["destination"],
    )
    auth_and_regions.add_argument(
        "-dest_view",
        "--destination_view",
        help="The destination view pointing to latest table having fishing events.",
        type=valid_table,
        default=DEFAULT["dest_view"],
    )
    auth_and_regions.add_argument(
        "--nautical_time_raster_table",
        help="Table with nautical dawn/dusk times by latitude and day of year.",
        type=valid_table,
        default=DEFAULT["nautical_time_raster_table"],
    )
    auth_and_regions.add_argument(
        "--overlap_threshold_hours",
        help="Hour threshold to classify over_dawn/over_dusk events as short or long overlap.",
        type=float,
        default=DEFAULT["overlap_threshold_hours"],
    )
    auth_and_regions.add_argument(
        "-rdate",
        "--reference_date",
        help="The reference date that has the less restrictive fishing events.",
        type=valid_date,
        default=DEFAULT["reference_date"],
    )
    auth_and_regions.add_argument(
        "-labels",
        "--labels",
        help="The labels assigned to each table.",
        type=json.loads,
        default=DEFAULT["labels"],
    )

    ################################################################################
    # fishing_restrictive
    ################################################################################
    fishing_restrictive = subparsers.add_parser(
        "fishing_restrictive",
        help="Generates a table with the fishing restrictive events in case does not exists.",
    )
    fishing_restrictive.add_argument(
        "-source_events",
        "--source_restrictive_events",
        help="The source of restrictive events table.",
        type=valid_table,
        default=DEFAULT["source_restrictive_events"],
    )
    fishing_restrictive.add_argument(
        "-destrest",
        "--dest_restrictive_events",
        help="The destination table to place the restrictive events table.",
        type=valid_table,
        default=DEFAULT["dest_restrictive_events"],
    )
    fishing_restrictive.add_argument(
        "-destrestview",
        "--dest_rest_view",
        help="The destination view pointing to the restrictive events table.",
        type=valid_table,
        default=DEFAULT["dest_rest_view"],
    )
    fishing_restrictive.add_argument(
        "-rdate",
        "--reference_date",
        help="The reference date that has the restrictive fishing events.",
        type=valid_date,
        default=DEFAULT["reference_date"],
    )
    fishing_restrictive.add_argument(
        "-labels",
        "--labels",
        help="The labels assigned to each table.",
        type=json.loads,
        default=DEFAULT["labels"],
    )

    args = parser.parse_args(arguments[1:])
    if hasattr(args, "start_date") and hasattr(args, "end_date"):
        args.start_date = args.start_date.strftime("%Y-%m-%d")
        args.end_date = args.end_date.strftime("%Y-%m-%d")
    if hasattr(args, "reference_date"):
        args.reference_date = args.reference_date.strftime("%Y-%m-%d").replace('-', '')

    setup_logging(args.verbosity)
    log = logging.getLogger()

    args.base_table_description = (
        f"Pipeline: {PIPELINE_NAME}:v{PIPELINE_VERSION}\n"
        f"Description: {PIPELINE_DESCRIPTION}\n"
    )

    log.info(args.base_table_description)
    log.info("==========================")

    return args
