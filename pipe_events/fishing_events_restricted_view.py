import logging


def dest_table_description(**extra_items):
    return (
        f"{extra_items['base_table_description']}\n"
        f"{extra_items['table_description']}"
    )


def run(bq, params):
    log = logging.getLogger()

    query = bq.format_query('fishing-events-5-less-restrictive.sql.j2', **params)
    bq.create_view(
        params["dest_lr_events"],
        query,
        dest_table_description(**params),
        labels=params["labels"],
    )
    log.info(f"The view {params['dest_lr_events']} is ready.")
    return True
