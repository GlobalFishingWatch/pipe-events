import datetime
from jinja2 import Environment
from jinja2.loaders import FileSystemLoader
import google.cloud.bigquery as bigquery
import logging

client = bigquery.Client()

def get_fishing_events_incremental_query(
        messages_table, 
        start_date, 
        end_date, 
        nnet_score_night_loitering
):
    env = Environment(loader=FileSystemLoader("."))

    return env.get_template("fishing-events-1-incremental.sql.j2").render(
        messages_table=messages_table,
        start_date=start_date,
        end_date=end_date,
        nnet_score_night_loitering=nnet_score_night_loitering
    )

def get_fishing_events_merge_query(temp_table, merged_table, start_date, end_date):
    env = Environment(loader=FileSystemLoader("."))

    return env.get_template("fishing-events-2-merge.sql.j2").render(
        existing_merged_fishing_events=merged_table,
        temp_incremental_fishing_events=temp_table,
        max_fishing_event_gap_hours=2,
        start_date=start_date,
        end_date=end_date,
    )

def run_incremental_fishing_events_query(dataset, fishing_events_incremental_query):
    temp_table=f"{dataset}.temp_dev_incremental_fishing_events_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}"
    create_temp_table_query=f"""CREATE TABLE `{temp_table}`
    PARTITION BY DATE_TRUNC(event_end_date, MONTH)
    CLUSTER BY event_end_date, seg_id, timestamp
    OPTIONS(
        expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
    )
    AS {fishing_events_incremental_query}"""

    logging.info(f"Creating temp table with query: {create_temp_table_query}")

    job = client.query(create_temp_table_query)
    job.result()

    return temp_table

def create_merged_table_schema(temp_table, merged_table):
    # create the schema based on the temp table if it doesn't exist yet
    create_merge_table_statement_sql=f"""CREATE TABLE IF NOT EXISTS {merged_table}
    PARTITION BY DATE_TRUNC(event_end_date, MONTH)
    CLUSTER BY event_end_date, seg_id, timestamp
    AS SELECT *
    FROM {temp_table} 
    LIMIT 0"""
    
    logging.info(f"Creating merged table schema: {create_merge_table_statement_sql}")

    job=client.query(create_merge_table_statement_sql)
    job.result()

def run_fishing_events_merge_query(merged_table, fishing_events_merge_query, start_date):
    merge_statement_sql=f"""
MERGE INTO `{merged_table}` AS TARGET
USING (
{fishing_events_merge_query}
) AS staged
ON target.seg_id = staged.seg_id
AND target.event_start = staged.event_start
AND target.timestamp = staged.timestamp
AND target.event_end_date >= '{start_date}'
WHEN MATCHED THEN UPDATE
SET 
  target.ssvid = staged.ssvid,
  target.seg_id = staged.seg_id,
  target.timestamp = staged.timestamp,
  target.year = staged.year,
  target.lat = staged.lat,
  target.lon = staged.lon,
  target.speed_knots = staged.speed_knots,
  target.meters_to_prev = staged.meters_to_prev,
  target.implied_speed_knots = staged.implied_speed_knots,
  target.hours = staged.hours,
  target.score = staged.score,
  target.event_start = staged.event_start,
  target.event_end = staged.event_end,
  target.event_end_date = staged.event_end_date
WHEN NOT MATCHED THEN INSERT VALUES (
  ssvid,
  seg_id,
  timestamp,
  year,
  lat,
  lon,
  speed_knots,
  meters_to_prev,
  implied_speed_knots,
  hours,
  score,
  event_start,
  event_end,
  event_end_date
)"""
    
    logging.info(f"Running merge statement: {merge_statement_sql}")

    job=client.query(merge_statement_sql)
    job.result()
    
def get_fishing_events_filter_query(
        merged_table,
        filtered_table,
        segs_activity_table,
        segment_vessel_table,
        product_vessel_info_summary_table,
        fishing_list_filters
):
    env = Environment(loader=FileSystemLoader("."))

    return env.get_template("fishing-events-3-filter.sql.j2").render(
        merged_table=merged_table,
        filtered_table=filtered_table,
        segs_activity_table=segs_activity_table,
        segment_vessel_table=segment_vessel_table,
        product_vessel_info_summary_table=product_vessel_info_summary_table,
        fishing_list_filters=fishing_list_filters
    )

def run_fishing_events_filter_query(fishing_events_filter_query):    
    logging.info(f"Running filter statement: {fishing_events_filter_query}")

    job = client.query(fishing_events_filter_query)
    job.result()

def orchestrate_fishing_events(destination_dataset, start_date, end_date, nnet_score_night_loitering):

    merged_table=f"{destination_dataset}.dev_incremental_fishing_events_merged"
    filtered_table=f"{destination_dataset}.dev_incremental_fishing_events_filtered"

    # 1. generate temp table based on research_messges
    # this can be a backfill or a delta load
    fishing_events_incremental_query=get_fishing_events_incremental_query(
        messages_table='world-fishing-827.pipe_ais_v3_internal.research_messages',
        start_date=start_date,
        end_date=end_date,
        nnet_score_night_loitering=nnet_score_night_loitering
    )

    incremental_temp_table=run_incremental_fishing_events_query(
        dataset=destination_dataset,
        fishing_events_incremental_query=fishing_events_incremental_query
    )
    # incremental_temp_table='world-fishing-827.scratch_christian_homberg_ttl120d.temp_dev_incremental_fishing_events_20240917113142'

    # 2. create the schema based on the temp table if it doesn't exist yet
    create_merged_table_schema(incremental_temp_table, merged_table)

    # 3. merge the temp table with the merged table
    fishing_events_merge_query=get_fishing_events_merge_query(
        temp_table=incremental_temp_table,
        merged_table=merged_table,
        start_date=start_date,
        end_date=end_date
    )

    run_fishing_events_merge_query(
        merged_table=merged_table,
        fishing_events_merge_query=fishing_events_merge_query,
        start_date=start_date
    )

    fishing_events_filter_query=get_fishing_events_filter_query(
        merged_table=merged_table,
        filtered_table=filtered_table,
        segs_activity_table='world-fishing-827.pipe_ais_v3_published.segs_activity',
        segment_vessel_table='world-fishing-827.pipe_ais_v3_internal.segment_vessel',
        product_vessel_info_summary_table='world-fishing-827.pipe_ais_v3_published.product_vessel_info_summary',
        fishing_list_filters="prod_shiptype='fishing'"
    )

    run_fishing_events_filter_query(
        fishing_events_filter_query=fishing_events_filter_query
    )


orchestrate_fishing_events(
    destination_dataset='world-fishing-827.scratch_christian_homberg_ttl120d',
    start_date='2024-09-11',
    end_date='2024-09-12',
    nnet_score_night_loitering=["nnet_score", "night_loitering"][0]
)