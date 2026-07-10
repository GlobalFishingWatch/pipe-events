import json
import logging

from pipe_events.utils.bigquery import dest_table_description
from pipe_events.utils.validators import valid_date, valid_table

COMMAND = "fishing_events_restrictive"
HELP = "Generates a table with the fishing restrictive events in case does not exists."


def add_arguments(parser):
    parser.add_argument(
        "--bq-in-events",
        dest="source_restrictive_events",
        help="The source of restrictive events table.",
        type=valid_table,
        required=True,
    )
    parser.add_argument(
        "--bq-out-events",
        dest="dest_restrictive_events",
        help="The destination table to place the restrictive events table.",
        type=valid_table,
        required=True,
    )
    parser.add_argument(
        "--bq-out-events-view",
        dest="dest_rest_view",
        help="The destination view pointing to the restrictive events table.",
        type=valid_table,
        required=True,
    )
    parser.add_argument(
        "--reference-date",
        dest="reference_date",
        help="The reference date that has the restrictive fishing events.",
        type=valid_date,
        required=True,
    )
    parser.add_argument(
        "--labels",
        help="The labels assigned to each table.",
        type=json.loads,
        required=True,
    )


def run(bq, params):
    log = logging.getLogger()
    ref_date = params['reference_date'].strftime("%Y%m%d")
    params["source_restrictive_events"] += ref_date
    dest = params["dest_restrictive_events"] + ref_date
    # schama is the same just with fishing strict list
    schema_path = "./assets/bigquery/fishing-events-4-authorization-schema.json"
    bq.create_table(
        dest,
        schema_file=schema_path,
        table_description=dest_table_description(**params),
        partition_field="event_start",
        clustering_fields=["seg_id", "event_start"],
        labels=params["labels"],
    )

    log.info("*** 1. Creates the restrictive table.")
    auth_query = bq.format_query("fishing-events-5-restrictive.sql.j2", **params)
    bq.run_query(
        auth_query,
        dest_table=dest,
        write_disposition="WRITE_TRUNCATE",
        partition_field="event_start",
        clustering_fields=["seg_id", "event_start"],
        labels=params["labels"],
    )
    bq.update_table_schema(dest, schema_path)  # schema should be kept after trucate
    log.info(f"The table {dest} is ready.")

    log.info("*** 2. Creates/Updates the view over the restricted table.")
    bq.create_view(
        params["dest_rest_view"],
        f"select * from `{dest}`",
        dest_table_description(**params),
        params["labels"],
    )
    bq.update_table_schema(
        params["dest_rest_view"],
        schema_path
    )
    return True
