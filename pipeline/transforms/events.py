import itertools as it
import datetime as dt
import statistics as stat
import logging
from apache_beam import PTransform
from apache_beam import FlatMap


class Events(PTransform):
    def __init__(self, fishing_threshold=0.5):
        self.fishing_threshold = fishing_threshold

    def is_fishing(self, x):
        return self.score(x) > self.fishing_threshold

    def score(self, x):
        return x['score']

    def timestamp(self, x):
        return x['timestamp']

    def latitude(self, x):
        return x['lat']

    def longitude(self, x):
        return x['lon']

    def safe_stdev(self, xs, mean):
        if len(xs) > 1:
            return stat.stdev(xs, mean)
        else:
            return 0

    def to_event(self, x):
        (key, messages) = x
        logging.info("Collecting messages with key %s: %s", key, messages)

        all_messages = sorted(messages, key=self.timestamp)
        logging.info("Messages after sorting %s", all_messages)

        first_message = all_messages[0]
        time_bucket_start = self.timestamp(first_message).replace(minute=0, second=0, microsecond=0)
        time_bucket_end = time_bucket_start + dt.timedelta(hours=1)

        fishing_messages = filter(self.is_fishing, all_messages)
        logging.debug("Only %s fishing messages", len(fishing_messages))

        if not fishing_messages:
            logging.debug("This bucket does not contain fishing messages")
            return []

        logging.debug("This bucket contains fishing messages, generating event")
        all_scores = map(self.score, all_messages)
        all_mean = stat.mean(all_scores)
        all_stddev = self.safe_stdev(all_scores, all_mean)

        fishing_scores = map(self.score, fishing_messages)
        fishing_mean = stat.mean(fishing_scores)
        fishing_stddev = self.safe_stdev(fishing_scores, fishing_mean)

        latitudes = map(self.latitude, fishing_messages)
        longitudes = map(self.longitude, fishing_messages)

        return [{
            "id": first_message['id'],
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

    def expand(self, xs):
        return (
            xs
            | FlatMap(self.to_event)
        )
