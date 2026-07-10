import json
import logging

from pipe_events.utils.bigquery import dest_table_description
from pipe_events.utils.validators import valid_date, valid_table

COMMAND = "fishing_events_incremental"
HELP = "Generates the incremental fishing or night loitering events."

DEFAULT_MAX_FISHING_EVENT_GAP_HOURS = 2


def add_arguments(parser):
    parser.add_argument(
        "--start-date",
        dest="start_date",
        help="The start date of the source messages.",
        type=valid_date,
        required=True,
    )
    parser.add_argument(
        "--end-date",
        dest="end_date",
        help="The end date of the source messages.",
        type=valid_date,
        required=True,
    )
    parser.add_argument(
        "--bq-in-messages",
        dest="messages_table",
        help="The source messages table having fishing and night loitering info.",
        type=valid_table,
        required=True,
    )
    parser.add_argument(
        "--score-field",
        dest="nnet_score_night_loitering",
        help="The field name that has the score to eval.",
        choices=["nnet_score", "night_loitering"],
        required=True,
    )
    parser.add_argument(
        "--max-fishing-event-gap-hours",
        dest="max_fishing_event_gap_hours",
        help="The max gap hours of yesterday to get potentially open events.",
        type=int,
        default=DEFAULT_MAX_FISHING_EVENT_GAP_HOURS,
    )
    parser.add_argument(
        "--bq-out-merged-events",
        dest="merged_events",
        help="Fully-qualified destination table for the merged fishing events.",
        type=valid_table,
        required=True,
    )
    parser.add_argument(
        "--labels",
        help="The labels assigned to each table.",
        type=json.loads,
        required=True,
    )


def run_incremental_fishing_events_query(temp_table, fishing_events_incremental_query):
    return f"""CREATE TEMP TABLE `{temp_table}`
    PARTITION BY DATE_TRUNC(event_end_date, MONTH)
    CLUSTER BY event_end_date, seg_id, timestamp
    AS ({fishing_events_incremental_query})"""


def run(bq, params):
    log = logging.getLogger()

    params["start_date"] = params["start_date"].strftime("%Y-%m-%d")
    params["end_date"] = params["end_date"].strftime("%Y-%m-%d")

    # Starts a BQ session
    session_id = bq.begin_session(params["labels"])

    log.info("*** 1. Run fishing-events-1-incremental.sql.j2 inside a BQ session.")

    merged_table = params["merged_events"]
    temp_table = "_SESSION.{}".format(
        "_".join(
            x.replace("-", "")
            for x in [merged_table.split(".")[-1], params["start_date"], params["end_date"]]
        )
    )
    incremental_query = bq.format_query("fishing-events-1-incremental.sql.j2", **params)
    query = run_incremental_fishing_events_query(temp_table, incremental_query)
    bq.run_query(query, session_id=session_id)

    log.info("*** 2. Ensure the merge table already exists or create it.")
    params_copy = params.copy()
    params_copy["temp_table"] = temp_table
    params_copy["existing_merged_fishing_events"] = merged_table
    log.info("Create the merged fishing events table if it does not exist.")
    bq.create_table(
        params_copy["existing_merged_fishing_events"],
        schema_file="./assets/bigquery/fishing-events-2-merge-schema.json",
        table_description=dest_table_description(**params),
        partition_field="event_end_date",
        clustering_fields=["event_end_date", "seg_id", "timestamp"],
        labels=params_copy["labels"],
    )

    log.info("Truncate incremental fishing events merged table and update event_end and "
             "event_end_date.")
    truncation_query = bq.format_query(
        "fishing-events-2a-truncate-before-merge.sql.j2",
        existing_merged_fishing_events=params_copy["existing_merged_fishing_events"],
        start_date=params["start_date"],
    )

    bq.run_query(truncation_query, session_id=session_id)

    log.info("*** 3. Merges the temp table with the merged table.")
    params_copy["merged_table"] = params_copy["existing_merged_fishing_events"]
    params_copy["temp_incremental_fishing_events"] = params_copy["temp_table"]
    params_copy["fishing_events_merge_query"] = bq.format_query(
        "fishing-events-2b-merge.sql.j2", **params_copy
    )
    merge_query = bq.format_query("fishing-events-2c-merge-into.sql.j2", **params_copy)
    bq.run_query(merge_query, session_id=session_id)
    bq.end_session(session_id)  # required to use destination in QueryJobConfig then

    return True
