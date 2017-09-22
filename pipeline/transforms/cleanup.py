import datetime as dt
from apache_beam import PTransform
from apache_beam import Filter
from apache_beam import Map

class Cleanup(PTransform):
    def has_score(self, x):
        return x['score'] is not None

    def parse_timestamp(self, x):
        raw_timestamp = x['timestamp']
        timestamp = dt.datetime.strptime(raw_timestamp, "%Y-%m-%d %H:%M:%S.%f %Z")

        x['timestamp'] = timestamp
        x['raw_timestamp'] = raw_timestamp
        return x

    def expand(self, xs):
        return (
            xs
            | Filter(self.has_score)
            | Map(self.parse_timestamp)
        )
