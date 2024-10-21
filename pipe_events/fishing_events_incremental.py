import logging


def run_incremental_fishing_events_query(temp_table, fishing_events_incremental_query):
    return f"""CREATE TEMP TABLE `{temp_table}`
    PARTITION BY DATE_TRUNC(event_end_date, MONTH)
    CLUSTER BY event_end_date, seg_id, timestamp
    AS ({fishing_events_incremental_query})"""


def dest_table_description(**extra_items):
    return (
        f"{extra_items['base_table_description']}\n"
        f"{extra_items['table_description']}"
    )


def run(bq, params):
    log = logging.getLogger()
    # start a session
    session_id = bq.begin_session(params["labels"])

    log.info("*** 1. Run fishing-events-1-incremental.sql.j2 inside a BQ session.")

    temp_table = "_SESSION.{}".format(
        "_".join(
            list(
                map(
                    lambda x: x.replace("-", ""),
                    [params["destination_table_prefix"], params["start_date"], params["end_date"]],
                )
            )
        )
    )
    incremental_query = bq.format_query("fishing-events-1-incremental.sql.j2", **params)
    query = run_incremental_fishing_events_query(temp_table, incremental_query)
    bq.run_query(query, session_id=session_id)

    log.info("*** 2. Ensure the merge table already exists or create it.")
    params_copy = params.copy()
    params_copy["temp_table"] = temp_table
    prefix_table = f'{params["destination_dataset"]}.{params["destination_table_prefix"]}'
    params_copy["existing_merged_fishing_events"] = f"{prefix_table}_merged"
    bq.create_table(
        params_copy["existing_merged_fishing_events"],
        schema_file="./assets/bigquery/fishing-events-2-merge-schema.json",
        table_description=dest_table_description(**params),
        partition_field="event_end_date",
        clustering_fields=["event_end_date", "seg_id", "timestamp"],
        labels=params_copy["labels"],
    )

    log.info("*** 3. Merges the temp table with the merged table.")
    params_copy["merged_table"] = params_copy["existing_merged_fishing_events"]
    params_copy["temp_incremental_fishing_events"] = params_copy["temp_table"]
    params_copy["fishing_events_merge_query"] = bq.format_query(
        "fishing-events-2-merge.sql.j2", **params_copy
    )
    merge_query = bq.format_query("fishing-events-2b-merge-into.sql.j2", **params_copy)
    bq.run_query(merge_query, session_id=session_id)

    log.info("*** 4. Ensures filter table exists.")
    params_copy["filtered_table"] = f"{prefix_table}_filtered"
    bq.create_table(
        params_copy["filtered_table"],
        schema_file="./assets/bigquery/fishing-events-3-filter-schema.json",
        table_description=dest_table_description(**params),
        partition_field="event_end_date",
        clustering_fields=["event_end_date", "seg_id"],
        labels=params["labels"],
    )

    bq.end_session(session_id)  # required to use destination in QueryJobConfig then

    log.info("*** 5. Runs the filter over the merged table.")
    filter_query = bq.format_query("fishing-events-3-filter.sql.j2", **params_copy)
    bq.run_query(
        filter_query,
        dest_table=params_copy["filtered_table"],
        partition_field="event_end_date",
        clustering_fields=["event_end_date", "seg_id"],
        labels=params["labels"],
    )  # with session cannot set destination

    return True
