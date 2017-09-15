import argparse
import logging

import apache_beam as beam

source_query = """
select
    messages.lat as lat,
    messages.lon as lon,
    messages.score as score,
    messages.timestamp as timestamp,
    vessels.seriesgroup as seriesgroup
from
    [pipeline_tileset_p_p516_daily.516_resample_v2_2017_09_08_messages] messages
    left join [pipeline_tileset_p_p516_daily.516_resample_v2_2017_09_08_vessels] vessels
        on messages.seg_id == vessels.seg_id
where
    year(timestamp) = 2017
    and month(timestamp) = 1
"""

def has_score(x):
    return x['score'] is not None

def parse_timestamp(x):
    import datetime as dt
    timestamp = dt.datetime.strptime(x['timestamp'], "%Y-%m-%d %H:%M:%S.%f %Z")
    return {
        "lat": x['lat'],
        "lon": x['lon'],
        "score": x['score'],
        "timestamp": timestamp,
        "timestamp_raw": x['timestamp'],
        "seriesgroup": x['seriesgroup'],
    }

def to_groupable_tuples(x):
    key = str.format("{}-{}", x['seriesgroup'], x['timestamp'].strftime("%Y-%m-%d-%H"))
    return (key, x)

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

    (key, messages) = x
    logging.debug("Collecting messages with key %s: %s", key, messages)

    all_messages = sorted(messages, key=lambda x: x['timestamp'])
    logging.debug("Messages after sorting %s", all_messages)

    first_message = all_messages[0]
    logging.debug("First message %s", first_message)

    time_bucket_start = first_message['timestamp'].replace(minute=0, second=0, microsecond=0)
    time_bucket_end = time_bucket_start + dt.timedelta(hours=1)

    fishing_messages = filter(is_fishing, all_messages)
    logging.debug("Only fishing messages %s", fishing_messages)

    if not fishing_messages:
        logging.debug("This bucket does not contain fishing messages")
        return []

    logging.debug("This bucket contains fishing messages, generating event")
    all_scores = map(score, all_messages)
    all_mean = stat.mean(all_scores)
    all_stddev = safe_stdev(all_scores, all_mean)

    fishing_scores = map(score, fishing_messages)
    fishing_mean = stat.mean(fishing_scores)
    fishing_stddev = safe_stdev(fishing_scores, fishing_mean)

    latitudes = map(latitude, fishing_messages)
    longitudes = map(longitude, fishing_messages)

    return [{
        "seriesgroup": first_message['seriesgroup'],
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

def encode_datetimes_to_iso(x):
    def encode_datetime_field(value):
        return value.strftime('%Y-%m-%d %H:%M:%S.%f UTC')

    for field in ['start', 'end']:
        x[field] = encode_datetime_field(x[field])

    return x

def build_table_schema(spec):
    schema = beam.io.gcp.internal.clients.bigquery.TableSchema()

    for name, type in spec.iteritems():
        field = beam.io.gcp.internal.clients.bigquery.TableFieldSchema()
        field.name = name
        field.type = type
        field.mode = 'nullable'
        schema.fields.append(field)

    return schema


def run():
    options = beam.options.pipeline_options.PipelineOptions()

    with beam.Pipeline(options=options) as pipeline:
        (
            pipeline
            | beam.io.Read(beam.io.gcp.bigquery.BigQuerySource(query=source_query))
            | beam.Filter(has_score)
            | beam.Map(parse_timestamp)
            | beam.Map(to_groupable_tuples)
            | beam.GroupByKey()
            | beam.FlatMap(collect_messages_into_event)
            | beam.Map(encode_datetimes_to_iso)
            | beam.io.Write(beam.io.gcp.bigquery.BigQuerySink(
                dataset='scratch_andres',
                table='api_events_datasource_seriesgroups',
                write_disposition=beam.io.gcp.bigquery.BigQueryDisposition.WRITE_TRUNCATE,
                schema=build_table_schema({
                    "seriesgroup": "integer",
                    "start": "timestamp",
                    "end": "timestamp",
                    "min_lat": "float",
                    "min_lon": "float",
                    "max_lat": "float",
                    "max_lon": "float",
                    "count": "integer",
                    "score_avg_fishing": "float",
                    "score_stddev_fishing": "float",
                    "score_avg_all": "float",
                    "score_stddev_all": "float",
                }
            )))
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.DEBUG)
    run()
