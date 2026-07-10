import json
import logging

from pipe_events.utils.bigquery import dest_table_description
from pipe_events.utils.validators import valid_date, valid_table

COMMAND = "fishing_events_auth_and_regions"
HELP = "Combine the fishing and night_loitering with authorization and regions."


def add_arguments(parser):
    parser.add_argument(
        "-source_fishing",
        "--source_fishing_events",
        help="The incremental fishing events table.",
        type=valid_table,
        required=True,
    )
    parser.add_argument(
        "-source_nl",
        "--source_night_loitering_events",
        help="The night loitering events table.",
        type=valid_table,
        required=True,
    )
    parser.add_argument(
        "-idcore",
        "--vessel_identity_core",
        help="The vessel identity core table.",
        type=valid_table,
        required=True,
    )
    parser.add_argument(
        "-idauth",
        "--vessel_identity_authorization",
        help="The vessel identity authorization table.",
        type=valid_table,
        required=True,
    )
    parser.add_argument(
        "-measures",
        "--spatial_measures_table",
        help="The spatial measures table.",
        type=valid_table,
        required=True,
    )
    parser.add_argument(
        "-regions",
        "--regions_table",
        help="The event regions table.",
        type=valid_table,
        required=True,
    )
    parser.add_argument(
        "--product_vessel_info_summary_table",
        help="The all vessels by year table.",
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
        "--udfs_project",
        help="GCP project id where the shared UDFs live.",
        type=str,
        required=True,
    )
    parser.add_argument(
        "-dest",
        "--destination",
        help="The destination table having fishing events.",
        type=valid_table,
        required=True,
    )
    parser.add_argument(
        "-dest_view",
        "--destination_view",
        help="The destination view pointing to latest table having fishing events.",
        type=valid_table,
        required=True,
    )
    parser.add_argument(
        "-rdate",
        "--reference_date",
        help="The reference date that has the less restrictive fishing events.",
        type=valid_date,
        required=True,
    )
    parser.add_argument(
        "-labels",
        "--labels",
        help="The labels assigned to each table.",
        type=json.loads,
        required=True,
    )


def run(bq, params):
    log = logging.getLogger()
    params['reference_date'] = params['reference_date'].strftime("%Y%m%d")
    dest = params["destination"] + params['reference_date']
    schema_path = "./assets/bigquery/fishing-events-4-authorization-schema.json"

    bq.create_table(
        dest,
        schema_file=schema_path,
        table_description=dest_table_description(**params),
        partition_field="event_start",
        clustering_fields=["seg_id", "event_start"],
        labels=params["labels"],
    )

    log.info("*** 1. Creates the authorized with regions. Or less restrictive table.")
    auth_query = bq.format_query("fishing-events-4-authorization.sql.j2", **params)
    bq.run_query(
        auth_query,
        dest_table=dest,
        write_disposition="WRITE_TRUNCATE",
        partition_field="event_start",
        clustering_fields=["seg_id", "event_start"],
        labels=params["labels"],
    )
    bq.update_table_schema(
        dest,
        schema_path
    )  # schema should be kept after trucate
    log.info("*** 2. Creates/Updates the view over the authorized with regions table.")
    bq.create_view(
        params["destination_view"],
        f"select * from `{dest}`",
        dest_table_description(**params),
        params["labels"],
    )
    bq.update_table_schema(
        params["destination_view"],
        schema_path
    )
    return True
