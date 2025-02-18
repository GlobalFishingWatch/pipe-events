import logging
from pipe_events.utils.bigquery import dest_table_description


def run(bq, params):
    log = logging.getLogger()
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
    return True
