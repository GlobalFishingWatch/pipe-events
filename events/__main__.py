import argparse
import logging

import apache_beam as beam

source_query = """
select
    mmsi, lat, lon, score, timestamp
from
    [pipeline_tileset_p_p516_daily.516_resample_v2_2017_09_08_messages]
where
    year(timestamp) = 2017
"""

def to_timestamped_message(x):
    import datetime as dt
    import apache_beam as beam

    timestamp = dt.datetime.strptime(x['timestamp'], "%Y-%m-%d %H:%M:%S.%f %Z")
    unix_timestamp = (timestamp - dt.datetime(1970, 1, 1)).total_seconds()

    message = {
        "timestamp": timestamp,
        "lat": x['lat'],
        "lon": x['lon'],
        "mmsi": x['mmsi'],
        "score": x['score'],
    }

    return beam.window.TimestampedValue(message, unix_timestamp)

def has_score(x):
    return x['score'] is not None

def to_mmsi_tuples(x):
    return (x['mmsi'], x)

def collect_messages_into_event(x):
    import itertools as it
    import datetime as dt
    import statistics as stat

    def is_fishing(x):
        return x['score'] > 0.5

    def score(x):
        return x['score']

    def latitude(x):
        return x['lat']

    def longitude(x):
        return x['lon']

    def safe_stdev(xs, mean):
        if len(xs) > 1:
            return stat.stdev(xs, mean)
        else:
            return 0

    (mmsi, messages) = x
    logging.debug("Collecting %d messages for a single mmsi %s into an event: %s", len(messages), mmsi, messages)

    all_messages = sorted(messages, key=lambda x: x['timestamp'])
    logging.debug("Messages after sorting %s", all_messages)

    first_message = all_messages[0]
    logging.debug("First message %s", first_message)

    fishing_messages = filter(is_fishing, all_messages)
    logging.debug("Only fishing messages %s", fishing_messages)

    if not fishing_messages:
        logging.debug("This bucket does not contain fishing messages")
        return []

    all_scores = map(score, all_messages)
    all_mean = stat.mean(all_scores)
    all_stddev = safe_stdev(all_scores, all_mean)

    fishing_scores = map(score, fishing_messages)
    fishing_mean = stat.mean(fishing_scores)
    fishing_stddev = safe_stdev(fishing_scores, fishing_mean)

    time_bucket_start = first_message['timestamp'].replace(minute=0, second=0, microsecond=0)
    time_bucket_end = time_bucket_start + dt.timedelta(hours=1)

    latitudes = map(latitude, fishing_messages)
    longitudes = map(longitude, fishing_messages)

    return [{
        "mmsi": mmsi, # TODO: Use seriesgroup instead
        "start": time_bucket_start,
        "end": time_bucket_end,
        "min_lat": min(latitudes),
        "min_lon": min(longitudes),
        "max_lat": max(latitudes),
        "max_lon": max(longitudes),
        "count": len(fishing_messages),
        "score_avg_fishing": fishing_mean,
        "score_stddev_fishing": fishing_stddev,
        "score_avg_all": all_mean,
        "score_stddev_all": all_stddev,
    }]

def run():
    options = beam.options.pipeline_options.PipelineOptions()

    with beam.Pipeline(options=options) as pipeline:
        source = beam.io.gcp.bigquery.BigQuerySource(query=source_query)

        (
            pipeline
            | beam.io.Read(source)
            | beam.Filter(has_score)
            | beam.Map(to_timestamped_message)
            | beam.WindowInto(beam.window.FixedWindows(3600))
            | beam.Map(to_mmsi_tuples)
            | beam.GroupByKey()
            | beam.FlatMap(collect_messages_into_event)
            | beam.io.WriteToText('gs://andres-scratch/pipe-events/results/output')
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.DEBUG)
    run()
