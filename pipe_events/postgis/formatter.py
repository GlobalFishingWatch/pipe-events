# -*- coding: utf-8 -*-
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

        # Normalize points and multipoints into multipoints
        match = geography_regex.match(record['event_geography'])
        points = match.group(1)
        normalized_geography = "MULTIPOINT({})".format(points)

        # Normalize mean_lat and mean_lon into an actual point
        normalized_mean_position = "POINT({} {})".format(
            record['lon_mean'], record['lat_mean'])
        try:
            writer.writerow([
                record['event_id'],
                record['event_type'],
                record['vessel_id'],
                record['event_start'],
                record.get('event_end'),
                record['event_info'],
                (b''.join([s.encode('utf-8') for s in record['event_vessels']])).decode('utf-8'),
                normalized_geography,
                normalized_mean_position
            ])
        except:
            print("Unable to convert record to csv at {}".format(record))
            raise
