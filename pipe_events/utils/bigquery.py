"""
Utilities for use with bigquery
"""

import datetime as dt
import logging
from time import sleep
from google.cloud import bigquery
from jinja2 import Environment
from jinja2 import FileSystemLoader
from jinja2 import StrictUndefined
import json
from google.cloud.exceptions import Conflict


def dest_table_description(**items) -> str:
    """Returns the table description."""
    return (
        f"{items.get('base_table_description', '')}\n"
        f"{items.get('table_description', '')}"
    )


def as_date_str(d):
    result = d
    if type(d) in [dt.datetime, dt.date]:
        result = d.strftime("%Y-%m-%d")
    return result


def format_query(template_file: str, **params) -> str:
    """
    Format a jinja2 templated query with the given params.

    You may have extra params which are not used by the template, but all params
    required by the template must be provided.
    """
    jinja2_env = Environment(
        loader=FileSystemLoader(["./assets/bigquery/"]), undefined=StrictUndefined
    )
    sql_template = jinja2_env.get_template(template_file)
    formatted_template = sql_template.render(params)
    return formatted_template


def load_schema(schema_file):
    with open(schema_file) as file:
        return json.load(file)


class BigqueryHelper:
    def __init__(self, project=None, logger=None, dry_run=False):
        self.project = project
        self.log = logger or logging.getLogger(__name__)
        self.dry_run = dry_run
        self.client = bigquery.Client(project=project)

    def format_query(self, template_file, **params):
        return format_query(template_file, **params)

    def begin_session(self, labels={}):
        config = bigquery.QueryJobConfig(
            create_session=True, dry_run=self.dry_run, labels=labels
        )
        self.log.info("Starting bigquery session")
        job = self.client.query("SELECT 1", job_config=config)
        session_id = job.session_info.session_id
        self.log.info(f"Bigquery session started {session_id}")
        return session_id

    def end_session(self, session_id):
        config = bigquery.QueryJobConfig(
            dry_run=self.dry_run,
        )
        self.log.info(f"Terminating bigquery session {session_id}")
        job = self.client.query(
            f"CALL BQ.ABORT_SESSION('{session_id}')", job_config=config
        )
        return job.result()

    def table_ref(self, table):
        return bigquery.TableReference.from_string(
            table, default_project=self.client.project
        )

    def create_view(
        self,
        view_id,
        view_query,
        table_description,
        labels={},
    ):
        """
        Create a BigQuery view.

        :param view_id: fully qualified table name 'project_id.dataset.table'.
        :param view_query: query of the view.
        :param table_description: text to include in the table's description field.
        :parma labels: labels to audit the table.
        :return: A new google.cloud.bigquery.table.Table
        """
        table_ref = self.table_ref(view_id)

        # Create BQ table object
        view = bigquery.Table(table_ref)

        # Assing query view
        view.view_query = view_query

        # Set table description
        view.description = table_description

        # Set labels
        view.labels = labels

        try:
            view = self.client.create_table(view)
            self.log.info(f"Created {view.table_type}: {str(view.reference)}")
        except Conflict:
            view = self.client.update_table(view, ["view_query", "description", "labels"])
            self.log.info(f"Updated {view.table_type}: {str(view.reference)}")

    def create_table(
        self,
        full_table_name,
        schema_file,
        table_description,
        partition_field=None,
        clustering_fields=None,
        exists_ok=True,
        labels={},
    ):
        """
        Create a BigQuery table.

        :param full_table_name: fully qualified table name 'project_id.dataset.table'
        :param schema_file: path to schema json file
        :param table_description: text to include in the table's description field
        :param partition_field: name of field to use for time partitioning (None for no partition)
        :param exists_ok: Defaults to True. If True, ignore “already exists”
        errors when creating the table.
        :parma labels: labels to audit the table.
        :return: A new google.cloud.bigquery.table.Table
        """

        # load schema
        schema = load_schema(schema_file)
        table_ref = self.table_ref(full_table_name)

        # Create BQ table object
        bq_table = bigquery.Table(table_ref, schema=schema)

        # Set table description
        bq_table.description = table_description

        # Set labels
        bq_table.labels = labels

        # Set clustering fields
        bq_table.clustering_fields = clustering_fields

        # Set partitioning
        if partition_field:
            bq_table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.MONTH, field=partition_field
            )

        # Create table
        return self.client.create_table(bq_table, exists_ok=exists_ok)

    def clear_table_partition(
        self,
        full_table_name,
        partition_field,
        partition_date_start,
        partition_date_end,
        labels={},
    ):
        """
        Clear data from a specific table partition

        partition_date_start and partition_date_end may be a datetime, date or
        a string formatted 'YYYY-MM-DD'
        """

        partition_date_start = as_date_str(partition_date_start)
        partition_date_end = as_date_str(partition_date_end)

        # Clear data for query date before running query
        d_q = (f"DELETE FROM `{full_table_name}` "
               f"WHERE DATE({partition_field}) >= '{partition_date_start}' and "
               f"DATE({partition_field}) < '{partition_date_end}'")

        config = bigquery.QueryJobConfig(priority="BATCH", labels=labels)
        self.log.info(
            f"About to remove partition {partition_date_start}-{partition_date_end} "
            f"from {full_table_name}"
        )
        job = self.client.query(d_q, job_config=config)

        if job.error_result:
            err = job.error_result["reason"]
            msg = job.error_result["message"]
            raise RuntimeError(f"{err}: {msg}")
        else:
            job.result()

    def update_table_schema(self, table, schema_file):
        table = self.client.get_table(self.table_ref(table))  # API request
        table.schema = load_schema(schema_file)
        table = self.client.update_table(table, ["schema"])  # API request

    def update_table_description(self, table, description):
        table = self.client.get_table(self.table_ref(table))  # API request
        table.description = description
        table = self.client.update_table(table, ["description"])  # API request

    def update_labels(self, table, labels):
        table = self.client.get_table(self.table_ref(table))  # API request
        table.labels = labels
        table = self.client.update_table(table, ["labels"])  # API request

    def update_require_partition_filter(self, table):
        table = self.client.get_table(self.table_ref(table))  # API request
        table.require_partition_filter = True
        table = self.client.update_table(
            table, ["require_partition_filter"]
        )  # API request

    def timeline_stats(self, timeline):
        stats = {
            "elapsed_ms": 0,
            "slot_millis": 0,
            "pending_units": "",
            "completed_units": "",
            "active_units": "",
        }
        if timeline:
            entry = timeline[-1]
            stats["elapsed_ms"] = int(entry.elapsed_ms)
            stats["slot_millis"] = int(entry.slot_millis)
            stats["pending_units"] = entry.pending_units
            stats["completed_units"] = entry.completed_units
            stats["active_units"] = entry.active_units
        return stats

    def log_job_stats(self, job):
        execution_seconds = (job.ended - job.started).total_seconds()
        slot_seconds = (job.slot_millis or 0) / 1000
        self.log.info(f"  execution_seconds:     {execution_seconds}")
        self.log.info(f"  slot_seconds:          {slot_seconds}")
        self.log.info(f"  num_child_jobs:        {job.num_child_jobs}")
        self.log.info(f"  total_bytes_processed: {job.total_bytes_processed}")
        self.log.info(f"  total_bytes_billed:    {job.total_bytes_billed}")
        self.log.info("  referenced_tables:")
        for table in job.referenced_tables:
            self.log.info(f"    {table.project}.{table.dataset_id}.{table.table_id}")
        if job.destination:
            self.log.info("  output_table:")
            self.log.info(f"    {job.destination}")

        self.log.info(f"  reservation_usage:     {job.reservation_usage}")
        self.log.info(f"  script_statistics:     {job.script_statistics}")

    def dump_query(self, query):
        self.log.warning("\n*** BEGIN SQL ***\n")
        self.log.warning(query)
        self.log.warning("\n*** END SQL ***\n")

    def run_query(
        self,
        query,
        dest_table=None,
        write_disposition="WRITE_APPEND",
        partition_field=None,
        clustering_fields=None,
        session_id=None,
        job_priority="INTERACTIVE",
        labels={},
    ):
        connection_properties = []
        if session_id is not None:
            connection_properties.append(
                bigquery.ConnectionProperty(key="session_id", value=session_id)
            )

        if dest_table:
            config = bigquery.QueryJobConfig(
                destination=self.table_ref(dest_table),
                priority=job_priority,
                write_disposition=write_disposition,
                dry_run=self.dry_run,
                time_partitioning=bigquery.table.TimePartitioning(
                    type_=bigquery.table.TimePartitioningType.MONTH,
                    field=partition_field,
                ),
                clustering_fields=clustering_fields,
                connection_properties=connection_properties,
                labels=labels,
            )
        else:
            config = bigquery.QueryJobConfig(
                priority=job_priority,
                dry_run=self.dry_run,
                connection_properties=connection_properties,
                labels=labels,
            )
        job = self.client.query(query, job_config=config)
        if self.dry_run:
            self.log.info("\n*** DRY RUN ***\n")
        self.log.info(f"Bigquery job created: {job.created:%Y-%m-%d %H:%M:%S}")
        self.log.info(f"job_id: {job.job_id}")
        self.log.debug("Running...")
        start_time = dt.datetime.now()
        i = 0
        while job.running():
            stats = self.timeline_stats(job.timeline)
            stats["run_time"] = (
                round((dt.datetime.now() - start_time).total_seconds()) + 1
            )
            stats["exec_s"] = round(stats["elapsed_ms"] / 1000)
            stats["slot_s"] = round(stats["slot_millis"] / 1000)
            if i % 10 == 0:
                line = (
                    "  elapsed: {run_time}s "
                    " exec: {exec_s}s  slot: {slot_s}s"
                    " pend: {pending_units} compl: {completed_units} active: {active_units}"
                    .format(
                        **stats
                    )
                )
                line = f"{line[:80]: <80}"
                if self.log.level == logging.DEBUG:
                    self.log.debug(line)

            i += 1
            sleep(0.1)
        if self.log.level == logging.DEBUG:
            self.log.debug(" " * 80)  # overwrite the previous line
            self.dump_query(job.query)
        self.log.info("Bigquery job done.")

        if job.dry_run:
            self.dump_query(job.query)
        elif job.error_result:
            err = job.error_result["reason"]
            msg = job.error_result["message"]
            raise RuntimeError(f"{err}: {msg}")
        else:
            self.log_job_stats(job)
        return job.result()
