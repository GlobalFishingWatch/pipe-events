import sys
import json
import csv

csv_file = sys.argv[1]

with open(csv_file, "wb+") as f:
    writer = csv.writer(f)

    for line in sys.stdin:
        record = json.loads(line)
        writer.writerow([
            record['event_id'],
            record['event_id'],
            record['event_type'],
            record['vessel_id'],
            record['event_start'],
            record['event_end'],
            record['event_info'],
            record['event_geography']
        ])
