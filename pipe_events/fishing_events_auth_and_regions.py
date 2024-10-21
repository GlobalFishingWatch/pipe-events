import logging


def dest_table_description(**extra_items):
    return (
        f"{extra_items['base_table_description']}\n"
        f"{extra_items['table_description']}"
    )


def run(bq, params):
    log = logging.getLogger()

    bq.create_table(
        params["destination"],
        schema_file="./assets/bigquery/fishing-events-4-authorization-schema.json",
        table_description=dest_table_description(**params),
        partition_field="event_end_date",
        clustering_fields=["event_end_date", "seg_id", "event_start"],
        labels=params["labels"],
    )

    log.info("*** 1. Creates the authorized with regions.")
    auth_query = bq.format_query("fishing-events-4-authorization.sql.j2", **params)
    bq.run_query(
        auth_query,
        dest_table=params["destination"],
        partition_field="event_end_date",
        clustering_fields=["event_end_date", "seg_id", "event_start"],
        labels=params["labels"],
    )
    return True
