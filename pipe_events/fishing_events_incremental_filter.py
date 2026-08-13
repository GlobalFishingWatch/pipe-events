import json
import logging
from datetime import datetime, timedelta

from pipe_events.utils.bigquery import dest_table_description
from pipe_events.utils.validators import valid_date, valid_dataset, valid_table

COMMAND = "fishing_events_incremental_filter"
HELP = ("Aggregates messages into events with regions + distances (step 3a, "
        "incremental) then filters with vessel identity (step 3b, full).")


def add_arguments(parser):
    parser.add_argument(
        "--bq-in-segments-activity",
        dest="segs_activity_table",
        help="The segments activity table.",
        type=valid_table,
        required=True,
    )
    parser.add_argument(
        "--bq-in-segment-vessel",
        dest="segment_vessel_table",
        help="The segment vessel table.",
        type=valid_table,
        required=True,
    )
    parser.add_argument(
        "--bq-in-product-vessel-info-summary",
        dest="product_vessel_info_summary_table",
        help="The product vessel info summary table.",
        type=valid_table,
        required=True,
    )
    parser.add_argument(
        "--product-vessel-info-summary-field-prefix",
        dest="product_vessel_info_summary_field_prefix",
        help="""
            Prefix to use to access vessel info fields in
            `product_vessel_info_summary_table`. This is to account for
            differences between PVIS tables in different environments. For
            example, on ais this is `ais_`, but VMS PVIS has no prefix
            """,
        required=True,
    )
    parser.add_argument(
        "--bq-in-regions",
        dest="regions_table",
        help=("The event regions table used by step 3a for the S2 spatial "
              "join. Contains geometry + `s2_cells` array per region."),
        type=valid_table,
        required=True,
    )
    parser.add_argument(
        "--bq-in-spatial-measures",
        dest="spatial_measures_table",
        help=("The spatial measures table used by step 3a for the distance-"
              "from-shore/port join. Keyed by `gridcode` derived from lat/lon."),
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
        "--bq-in-udfs-dataset",
        dest="udfs_dataset",
        help="Fully-qualified dataset (project.dataset) where the shared UDFs live.",
        type=valid_dataset,
        required=True,
    )
    parser.add_argument(
        "--bq-in-merged-events",
        dest="merged_table",
        help="An existing merged table (step 2 output).",
        type=valid_table,
        required=True,
    )
    parser.add_argument(
        "--bq-out-aggregated-events",
        dest="aggregated_events",
        help=("Fully-qualified destination table for the intermediate "
              "aggregated events (step 3a output). Event-level "
              "table with regions_mean_position + four distance columns; "
              "partitioned by event_end_date and clustered by "
              "event_end_date + seg_id. Consumed by step 3b."),
        type=valid_table,
        required=True,
    )
    parser.add_argument(
        "--bq-out-filtered-events",
        dest="filtered_events",
        help=("Fully-qualified destination table for the final filtered "
              "fishing events (step 3b output). Adds vessel identity + "
              "event_id on top of the aggregated schema. Consumed by step 4."),
        type=valid_table,
        required=True,
    )
    parser.add_argument(
        "--start-date",
        dest="start_date",
        help=("Optional. When provided, step 3a runs INCREMENTALLY -- deletes "
              "the aggregated table from `start_date - 1 day` onwards and "
              "re-appends fresh data. Without this flag, step 3a runs FULL "
              "(WRITE_TRUNCATE) over every partition of the merged table."),
        type=valid_date,
        required=False,
        default=None,
    )
    parser.add_argument(
        "--labels",
        help="The labels assigned to each table.",
        type=json.loads,
        required=True,
    )


def run(bq, params):
    log = logging.getLogger()
    params_copy = params.copy()
    aggregated_table = params["aggregated_events"]
    filtered_table = params["filtered_events"]
    params_copy["aggregated_table"] = aggregated_table
    params_copy["filtered_table"] = filtered_table

    # ---------------------------------------------------------------
    # Step 3a: Aggregate messages + regions S2 join + spatial measures
    # INCREMENTAL when --start-date is provided, FULL otherwise.
    #
    # Reads the message-level `_merged` table (step 2 output) and writes
    # the event-level `_aggregated` table. This is where the expensive
    # S2 spatial join and the `spatial_measures` gridcode joins now live
    # -- moved out of step 4, which used to run them on ALL history every
    # day. Partitioning `_aggregated` by `event_end_date` lets the
    # incremental mode process only the day's delta.
    # ---------------------------------------------------------------
    aggregate_schema_file = "./assets/bigquery/fishing-events-3a-aggregate-schema.json"

    log.info("*** 3a. Ensures aggregated table exists.")
    bq.create_table(
        aggregated_table,
        schema_file=aggregate_schema_file,
        table_description=dest_table_description(**params),
        partition_field="event_end_date",
        clustering_fields=["event_end_date", "seg_id"],
        labels=params["labels"],
    )

    start_date = params.get("start_date")
    if start_date is not None:
        # ``valid_date`` (pipe_events.utils.validators) returns a
        # ``datetime.date`` -- see its ``-> datetime.date`` annotation. The
        # SQL template renders it via ``'{{ start_date }}'``, so normalize
        # to YYYY-MM-DD for both the template render and the delete-boundary
        # math. The ``hasattr(strftime)`` fallback tolerates a plain-string
        # value (a hand-composed programmatic caller) without crashing.
        if hasattr(start_date, "strftime"):
            start_date_str = start_date.strftime("%Y-%m-%d")
        else:
            start_date_str = str(start_date)
        params_copy["start_date"] = start_date_str

        # Incremental mode: delete affected partitions, then append.
        # Boundary is start_date - 1 day (matches the 3a template's
        # `WHERE event_end_date >= DATE_SUB('{{ start_date }}', INTERVAL 1 DAY)`).
        boundary_date = (
            datetime.strptime(start_date_str, "%Y-%m-%d") - timedelta(days=1)
        ).strftime("%Y-%m-%d")
        log.info(f"*** 3a. Incremental mode: deleting from aggregated "
                 f"where event_end_date >= {boundary_date}")
        bq.delete_from_date(
            aggregated_table,
            "event_end_date",
            boundary_date,
            labels=params["labels"],
        )
        write_disposition = "WRITE_APPEND"
    else:
        # Full mode: truncate and rewrite. Used for backfills and for
        # regenerating _aggregated when the regions or spatial_measures
        # inputs change (both are otherwise treated as static by 3a).
        log.info("*** 3a. Full mode: WRITE_TRUNCATE on aggregated table.")
        write_disposition = "WRITE_TRUNCATE"

    log.info("*** 3a. Running aggregate query over merged table.")
    aggregate_query = bq.format_query(
        "fishing-events-3a-aggregate.sql.j2", **params_copy
    )
    bq.run_query(
        aggregate_query,
        dest_table=aggregated_table,
        write_disposition=write_disposition,
        partition_field="event_end_date",
        clustering_fields=["event_end_date", "seg_id"],
        labels=params["labels"],
    )
    bq.update_table_schema(aggregated_table, aggregate_schema_file)

    # ---------------------------------------------------------------
    # Step 3b: Filter + vessel identity (always FULL / WRITE_TRUNCATE).
    #
    # Reads the event-level `_aggregated` (small) and joins the mutable
    # dimension tables (segs_activity, segment_vessel, PVIS) whose values
    # change daily. WRITE_TRUNCATE because ALL events need identity data
    # refreshed on every run, not just the delta. Cheap despite the full
    # scan because the input is event-level (not message-level like the
    # pre-refactor step 3).
    # ---------------------------------------------------------------
    filter_schema_file = "./assets/bigquery/fishing-events-3-filter-schema.json"

    log.info("*** 3b. Ensures filtered table exists.")
    bq.create_table(
        filtered_table,
        schema_file=filter_schema_file,
        table_description=dest_table_description(**params),
        partition_field="event_end_date",
        clustering_fields=["event_end_date", "seg_id"],
        labels=params["labels"],
    )

    log.info("*** 3b. Running filter query over aggregated table.")
    filter_query = bq.format_query(
        "fishing-events-3-filter.sql.j2", **params_copy
    )
    bq.run_query(
        filter_query,
        dest_table=filtered_table,
        write_disposition="WRITE_TRUNCATE",
        partition_field="event_end_date",
        clustering_fields=["event_end_date", "seg_id"],
        labels=params["labels"],
    )
    bq.update_table_schema(filtered_table, filter_schema_file)

    return True
