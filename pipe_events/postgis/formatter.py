import sys
import json
import csv
import re

geography_regex = re.compile(r"^.*\((.*)\)$")

csv_file = sys.argv[1]

with open(csv_file, "wb+") as f:
    writer = csv.writer(f)

    for line in sys.stdin:
        record = json.loads(line)

        # We need to normalize points and multipoints into multipoints
        match = geography_regex.match(record['event_geography'])
        points = match.group(1)
        normalized_geography = "MULTIPOINT({})".format(points)

        writer.writerow([
            record['event_id'],
            record['event_type'],
            record['vessel_id'],
            record['event_start'],
            record['event_end'],
            record['event_info'],
            normalized_geography
        ])
