"""Shared flow for the event-type generators (encounter, loitering, port visit).

Each generator publishes into a date-versioned table `<dest>_v<YYYYMMDD>` and points
a stable view at it, then drops the previous day's versioned table (a blue/green swap).
The only per-event differences are the SQL template, its parameters and the table
description, so the whole flow lives here.
"""

import datetime as dt
import logging

from pipe_events.constants import EVENTS_SCHEMA
from pipe_events.utils.bigquery import dest_table_description

PARTITION_FIELD = "event_start"


def build_description(params, sql_url, maintainers):
    """Compose a table description from the run params, the query URL and maintainers."""
    maintainer_lines = "\n".join(maintainers)
    return (
        f"{dest_table_description(**params)}\n"
        f"This table was generated using the query:\n{sql_url}\n"
        f"\nMaintainers:\n{maintainer_lines}\n"
    )


def versioned_table_names(dest_table, end_date):
    """Return (current, previous) versioned table names for the given end date.

    `end_date` is a datetime.date. The previous table is the one from the day before,
    which the swap removes once the current one is published.
    """
    current = f"{dest_table}_v{end_date:%Y%m%d}"
    previous = f"{dest_table}_v{end_date - dt.timedelta(days=1):%Y%m%d}"
    return current, previous


def publish_versioned_events(
    bq, *, dest_table, end_date, sql_template, template_params, description, labels
):
    log = logging.getLogger()
    current, previous = versioned_table_names(dest_table, end_date)

    log.info(f"Ensuring events table {current} exists")
    bq.create_table(
        current,
        schema_file=EVENTS_SCHEMA,
        table_description=description,
        partition_field=PARTITION_FIELD,
        labels=labels,
    )

    log.info(f"Truncating existing records in {current}")
    bq.run_query(bq.format_query("truncate-table.sql.j2", table=current))

    log.info(f"Inserting new records into {current} using {sql_template}")
    insert_query = bq.format_query(sql_template, **template_params)
    bq.run_query(
        insert_query,
        dest_table=current,
        write_disposition="WRITE_APPEND",
        partition_field=PARTITION_FIELD,
        labels=labels,
    )

    log.info(f"Pointing view {dest_table} at {current}")
    bq.create_view(dest_table, f"SELECT * FROM `{current}`", description, labels)
    bq.update_table_schema(dest_table, EVENTS_SCHEMA)

    log.info(f"Removing previous table {previous}")
    bq.remove_table(previous)

    return True
