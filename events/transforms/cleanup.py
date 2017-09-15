from apache_beam import PTransform
from apache_beam import Filter
from apache_beam import Map

class Cleanup(PTransform):
    def expand(self, xs):
        def has_score(x):
            return x['score'] is not None

        def parse_timestamp(x):
            import datetime as dt
            raw_timestamp = x['timestamp']
            timestamp = dt.datetime.strptime(raw_timestamp, "%Y-%m-%d %H:%M:%S.%f %Z")

            x['timestamp'] = timestamp
            x['raw_timestamp'] = raw_timestamp
            return x

        return xs | Filter(has_score) | Map(parse_timestamp)
