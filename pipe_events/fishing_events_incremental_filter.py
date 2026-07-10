import json
import logging

from pipe_events.utils.bigquery import dest_table_description
from pipe_events.utils.validators import valid_table

COMMAND = "fishing_events_incremental_filter"
HELP = "Takes the incremental fishing or night loitering events and apply filters."


def add_arguments(parser):
    parser.add_argument(
        "-segsact",
        "--segs_activity_table",
        help="The segments activity table.",
        type=valid_table,
        required=True,
    )
    parser.add_argument(
        "-segvessel",
        "--segment_vessel_table",
        help="The segment vessel table.",
        type=valid_table,
        required=True,
    )
    parser.add_argument(
        "-pvesselinfo",
        "--product_vessel_info_summary_table",
        help="The product vessel info summary table.",
        type=valid_table,
        required=True,
    )
    parser.add_argument(
        "--product_vessel_info_summary_field_prefix",
        help="""
            Prefix to use to access vessel info fields in
            `product_vessel_info_summary_table`. This is to account for
            differences between PVIS tables in different environments. For
            example, on ais this is `ais_`, but VMS PVIS has no prefix
            """,
    )
    parser.add_argument(
        "-sfield",
        "--nnet_score_night_loitering",
        help="The field name that has the score to eval.",
        choices=["nnet_score", "night_loitering"],
        required=True,
    )
    parser.add_argument(
        "--udfs_project",
        help="GCP project id where the shared UDFs live.",
        type=str,
        required=True,
    )
    parser.add_argument(
        "-dest",
        "--destination_dataset",
        help="The destination dataset having fishing events.",
        type=str,
        required=True,
    )
    parser.add_argument(
        "-dest_tbl_prefix",
        "--destination_table_prefix",
        help="The destination table prefix having fishing events.",
        type=str,
        required=True,
    )
    parser.add_argument(
        "-labels",
        "--labels",
        help="The labels assigned to each table.",
        type=json.loads,
        required=True,
    )
    parser.add_argument(
        "-mtbl",
        "--merged_table",
        help="An existing merged table.",
        type=valid_table,
        required=True,
    )


def run(bq, params):
    log = logging.getLogger()
    params_copy = params.copy()
    prefix_table = f'{params["destination_dataset"]}.{params["destination_table_prefix"]}'
    schema_file = "./assets/bigquery/fishing-events-3-filter-schema.json"

    log.info("*** 1. Ensures filter table exists.")
    params_copy["filtered_table"] = f"{prefix_table}_filtered"
    bq.create_table(
        params_copy["filtered_table"],
        schema_file=schema_file,
        table_description=dest_table_description(**params),
        partition_field="event_end_date",
        clustering_fields=["event_end_date", "seg_id"],
        labels=params["labels"],
    )

    log.info("*** 2. Runs the filter over the merged table with truncated data.")
    filter_query = bq.format_query("fishing-events-3-filter.sql.j2", **params_copy)
    bq.run_query(
        filter_query,
        dest_table=params_copy["filtered_table"],
        write_disposition="WRITE_TRUNCATE",
        partition_field="event_end_date",
        clustering_fields=["event_end_date", "seg_id"],
        labels=params["labels"],
    )
    bq.update_table_schema(
        params_copy["filtered_table"],
        schema_file
    )  # schema should be kept after trucate

    return True
