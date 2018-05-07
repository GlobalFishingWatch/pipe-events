import logging
from apache_beam import PTransform
from apache_beam import Map
from apache_beam import GroupByKey

class GroupByIdAndTimeBucket(PTransform):
    HOURLY_BUCKET = "%Y-%m-%d-%H"
    DAILY_BUCKET = "%Y-%m-%d"

    def __init__(self, pattern):
        self.pattern = pattern

    def to_groupable_tuples(self, x):
        id = x['id']
        timestamp = x['timestamp'].strftime(self.pattern)
        key = str.format("{}-{}", id, timestamp)
        return (key, x)

    def expand(self, xs):
        return (
            xs
            | Map(self.to_groupable_tuples)
            | GroupByKey()
        )

