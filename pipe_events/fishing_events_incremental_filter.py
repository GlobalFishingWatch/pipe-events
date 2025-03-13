import logging
from pipe_events.utils.bigquery import dest_table_description


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
