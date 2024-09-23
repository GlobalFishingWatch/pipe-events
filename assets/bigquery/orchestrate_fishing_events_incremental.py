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

def run_incremental_fishing_events_query(temp_table, fishing_events_incremental_query):
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
        nnet_score_night_loitering
):
    env = Environment(loader=FileSystemLoader("."))

    return env.get_template("fishing-events-3-filter.sql.j2").render(
        merged_table=merged_table,
        filtered_table=filtered_table,
        segs_activity_table=segs_activity_table,
        segment_vessel_table=segment_vessel_table,
        product_vessel_info_summary_table=product_vessel_info_summary_table,
        nnet_score_night_loitering=nnet_score_night_loitering
    )

def run_fishing_events_filter_query(fishing_events_filter_query):    
    logging.info(f"Running filter statement: {fishing_events_filter_query}")

    job = client.query(fishing_events_filter_query)
    job.result()

def orchestrate_fishing_events(
        source_dataset_internal,
        source_dataset_published,
        destination_dataset, 
        start_date, 
        end_date, 
        nnet_score_night_loitering,
        run_alias
):
    if nnet_score_night_loitering=="nnet_score":
        fishing_night_loitering="fishing"
    elif nnet_score_night_loitering=="night_loitering":
        fishing_night_loitering="night_loitering"
    else:
        raise ValueError("nnet_score_night_loitering must be either 'nnet_score' or 'night_loitering'")

    temp_incremental_table=f"{destination_dataset}.{run_alias}_temp_incremental_{fishing_night_loitering}_events_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}"
    merged_table=f"{destination_dataset}.{run_alias}_incremental_{fishing_night_loitering}_events_merged"
    filtered_table=f"{destination_dataset}.{run_alias}_incremental_{fishing_night_loitering}_events_filtered"

    # 1. generate temp table based on research_messges
    # this can be a backfill or a delta load
    fishing_events_incremental_query=get_fishing_events_incremental_query(
        messages_table=f'world-fishing-827.{source_dataset_internal}.research_messages',
        start_date=start_date,
        end_date=end_date,
        nnet_score_night_loitering=nnet_score_night_loitering
    )

    incremental_temp_table=run_incremental_fishing_events_query(
        temp_table=temp_incremental_table,
        fishing_events_incremental_query=fishing_events_incremental_query
    )

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
        segs_activity_table=f'world-fishing-827.{source_dataset_published}.segs_activity',
        segment_vessel_table=f'world-fishing-827.{source_dataset_internal}.segment_vessel',
        product_vessel_info_summary_table=f'world-fishing-827.{source_dataset_published}.product_vessel_info_summary',
        nnet_score_night_loitering=nnet_score_night_loitering
    )

    run_fishing_events_filter_query(
        fishing_events_filter_query=fishing_events_filter_query
    )


def get_fishing_events_authorization_query(
        fishing_events_less_restrictive_table,
        vessel_identity_core,
        vessel_identity_authorization,
        spatial_measures_table,
        regions_table,
        source_fishing_events,
        source_night_loitering_events,
        product_vessel_info_summary_table
):
    env = Environment(loader=FileSystemLoader("."))

    return env.get_template("fishing-events-4-authorization.sql.j2").render(
        fishing_events_less_restrictive_table=fishing_events_less_restrictive_table,
        vessel_identity_core=vessel_identity_core,
        vessel_identity_authorization=vessel_identity_authorization,
        spatial_measures_table=spatial_measures_table,
        regions_table=regions_table,
        source_fishing_events=source_fishing_events,
        source_night_loitering_events=source_night_loitering_events,
        all_vessels_byyear=product_vessel_info_summary_table
    )

def combine_fishing_night_loitering_events(
        fishing_events_table,
        night_loitering_events_table,
        source_dataset_published,
        destination_dataset,
        run_alias
):
    # 4. create authorization table
    fishing_events_authorization_query=get_fishing_events_authorization_query(
        fishing_events_less_restrictive_table=f"{destination_dataset}.{run_alias}_incremental_events_less_restrictive",
        vessel_identity_core=f'world-fishing-827.pipe_ais_v3_internal.identity_core',
        vessel_identity_authorization=f'world-fishing-827.pipe_ais_v3_internal.identity_authorization',
        spatial_measures_table=f'world-fishing-827.pipe_static.spatial_measures_20201105',
        regions_table=f'world-fishing-827.pipe_regions_layers.event_regions',
        source_fishing_events=fishing_events_table,
        source_night_loitering_events=night_loitering_events_table,
        product_vessel_info_summary_table=f'world-fishing-827.{source_dataset_published}.product_vessel_info_summary'
    )

    logging.info(f"Running authorization statement: {fishing_events_authorization_query}")

    job = client.query(fishing_events_authorization_query)
    job.result()


def fishing_night_loitering_events_backfill_incremental_pipeline(
        source_dataset_internal,
        source_dataset_published,
        destination_dataset,
        start_date_backfill,
        end_date_backfill,
        end_date_incremental,
        nnet_score_night_loitering,
        run_alias
):
    # generate start_date end_date pairs which start at start_date=end_date_backfill and end at end_date=end_date_incremental
    daily_loads = [
        (end_date_backfill + datetime.timedelta(days=i), end_date_backfill + datetime.timedelta(days=i+1)) 
                   for i in range((end_date_incremental - end_date_backfill).days)
                   ]

    orchestrate_fishing_events(
        source_dataset_internal=source_dataset_internal,
        source_dataset_published=source_dataset_published,
        destination_dataset=destination_dataset,
        start_date=start_date_backfill.strftime("%Y-%m-%d"),
        end_date=end_date_backfill.strftime("%Y-%m-%d"),
        nnet_score_night_loitering=nnet_score_night_loitering,
        run_alias=run_alias
    )

    for start_date, end_date in daily_loads:
        orchestrate_fishing_events(
            source_dataset_internal=source_dataset_internal,
            source_dataset_published=source_dataset_published,
            destination_dataset=destination_dataset,
            start_date=start_date.strftime("%Y-%m-%d"),
            end_date=end_date.strftime("%Y-%m-%d"),
            nnet_score_night_loitering=nnet_score_night_loitering,
            run_alias=run_alias
        )


def run_entire_fishing_events_backfill_incremental_pipeline(
        source_dataset_internal,
        source_dataset_published,
        destination_dataset,
        start_date_backfill,
        end_date_backfill,
        end_date_incremental,
        run_alias
):
    fishing_night_loitering_events_backfill_incremental_pipeline(
        source_dataset_internal=source_dataset_internal,
        source_dataset_published=source_dataset_published,
        destination_dataset=destination_dataset,
        start_date_backfill=start_date_backfill,
        end_date_backfill=end_date_backfill,
        end_date_incremental=end_date_incremental,
        nnet_score_night_loitering="nnet_score",
        run_alias=run_alias
    )

    fishing_night_loitering_events_backfill_incremental_pipeline(
        source_dataset_internal=source_dataset_internal,
        source_dataset_published=source_dataset_published,
        destination_dataset=destination_dataset,
        start_date_backfill=start_date_backfill,
        end_date_backfill=end_date_backfill,
        end_date_incremental=end_date_incremental,
        nnet_score_night_loitering="night_loitering",
        run_alias=run_alias
    )

    combine_fishing_night_loitering_events(
        fishing_events_table=f"{destination_dataset}.{run_alias}_incremental_fishing_events_filtered",
        night_loitering_events_table=f"{destination_dataset}.{run_alias}_incremental_night_loitering_events_filtered",
        source_dataset_published=source_dataset_published,
        destination_dataset=destination_dataset,
        run_alias=run_alias
    )
    
    

run_entire_fishing_events_backfill_incremental_pipeline(
    source_dataset_internal=['pipe_ais_v3_internal', 'pipe_ais_test_202408290000_internal'][0],
    source_dataset_published=['pipe_ais_v3_published', 'pipe_ais_test_202408290000_published'][0],
    destination_dataset='world-fishing-827.scratch_christian_homberg_ttl120d',
    start_date_backfill=datetime.datetime.strptime("2012-01-01", "%Y-%m-%d"),
    end_date_backfill=datetime.datetime.strptime("2024-09-17", "%Y-%m-%d"),
    end_date_incremental=datetime.datetime.strptime("2024-09-20", "%Y-%m-%d"),
    run_alias='pipe3_latest_20240920'
)

# run_entire_fishing_events_backfill_incremental_pipeline(
#     source_dataset_internal=['pipe_ais_v3_internal', 'pipe_ais_test_202408290000_internal'][1],
#     source_dataset_published=['pipe_ais_v3_published', 'pipe_ais_test_202408290000_published'][1],
#     destination_dataset='world-fishing-827.scratch_christian_homberg_ttl120d',
#     start_date_backfill=datetime.datetime.strptime("2020-01-01", "%Y-%m-%d"),
#     end_date_backfill=datetime.datetime.strptime("2020-12-01", "%Y-%m-%d"),
#     end_date_incremental=datetime.datetime.strptime("2020-12-31", "%Y-%m-%d"),
#     run_alias='pipe_ais_test_202012_incremental'
# )
