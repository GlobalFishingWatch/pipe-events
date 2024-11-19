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

def run_create_messages_view(self):
    query = self.add_messages_view()
    bq.format_query("restrictive_view.sql.j2", **params)
    table_desc = self.dest_table_description()
    dest_table = f'{self.args.dest_table}'
    success = True

    try:
        self.bigquery.create_messages_view(dest_table, self.args.source_messages_table, query, self.args.labels)
        self.bigquery.update_table_description(dest_table, self.args.table_description)
    except Exception as e:
        self.log.error("Exception running create_messages view", e)
        success = False
    return success
