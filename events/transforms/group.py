import logging
from apache_beam import PTransform
from apache_beam import Map
from apache_beam import GroupByKey

class GroupByIdAndTimeBucket(PTransform):
    HOURLY_BUCKET = "%Y-%m-%d-%H"
    DAILY_BUCKET = "%Y-%m-%d"

    def __init__(self, pattern):
        self.pattern = pattern

    def expand(self, xs):
        def to_groupable_tuples(x):
            id = x['id']
            timestamp = x['timestamp'].strftime(self.pattern)
            key = str.format("{}-{}", id, timestamp)
            return (key, x)

        logging.info("Grouping messages with bucket expression %s", self.pattern)
        return xs | Map(to_groupable_tuples) | GroupByKey()

